#==============================================================================
# Copyright (C) 2020-present Alces Flight Ltd.
#
# This file is part of FlightSchedulerDaemon.
#
# This program and the accompanying materials are made available under
# the terms of the Eclipse Public License 2.0 which is available at
# <https://www.eclipse.org/legal/epl-2.0>, or alternative license
# terms made available by Alces Flight Ltd - please direct inquiries
# about licensing to licensing@alces-flight.com.
#
# FlightSchedulerDaemon is distributed in the hope that it will be useful, but
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, EITHER EXPRESS OR
# IMPLIED INCLUDING, WITHOUT LIMITATION, ANY WARRANTIES OR CONDITIONS
# OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY OR FITNESS FOR A
# PARTICULAR PURPOSE. See the Eclipse Public License 2.0 for more
# details.
#
# You should have received a copy of the Eclipse Public License 2.0
# along with FlightSchedulerDaemon. If not, see:
#
#  https://opensource.org/licenses/EPL-2.0
#
# For more information on FlightSchedulerDaemon, please visit:
# https://github.com/openflighthpc/flight-scheduler-daemon
#==============================================================================

require 'async'
require 'async/io'

module FlightScheduler
  #
  # Run the given job step and save a reference to it in the job registry.
  #
  # Current limitations:
  #
  # * The job's standard input and output are not redirected to the `run`
  #   client.
  #
  class JobStepRunner
    attr_reader :output

    def initialize(step)
      @step = step
      @job = @step.job
    end

    def wait
      @task.wait
    end

    # Checks if the child process has exited correctly
    def success?
      return nil unless @status
      @status.success?
    end

    # Run the given step in a subprocess and return an Async::Task.
    #
    # Invariants:
    #
    # * Blocks until the job and step have been validated and recorded in the
    #   job registry.
    #
    # * Returns an Async::Task that can be `wait`ed on.  When the returned
    #   task has completed, the subprocess has completed and is no longer in
    #   the job registry.
    def run
      unless @job.valid? && @step.valid?
        raise JobValidationError, <<~ERROR.chomp
        An unexpected error has occurred! The job step does not appear to be
        in a valid state.
        ERROR
      end

      FlightScheduler.app.job_registry.add_runner(@job.id, @step.id, self)
      @task = Async do |task|
        input_pipe, output_pipe = Async::IO.pipe
        # Fork to create the child process [Non Blocking]
        @child_pid = Kernel.fork do
          input_pipe.close
          # Become the requested user and session leader
          Process::Sys.setgid(@job.gid)
          Process::Sys.setuid(@job.username)
          Process.setsid

          opts = {
            [:out, :err] => output_pipe.io,
            unsetenv_others: true,
          }

          Dir.chdir(@job.working_dir)
          # Exec into the job command
          Kernel.exec(@job.env, @step.path, *@step.arguments, **opts)
        end
        output_pipe.close

        # Loop asynchronously until the child is finished
        until out = Process.wait2(@child_pid, Process::WNOHANG) do
          task.yield
        end
        @status = out.last
        @output = input_pipe.read

        # Reset the child_pid, this prevents cancel killing other processes
        # which might spawn with the same PID
        @child_pid = nil
      ensure
        FlightScheduler.app.job_registry.remove_runner(@job.id, @step.id)
        input_pipe&.close
      end
    end

    # Kills the associated subprocess
    def cancel
      return unless @child_pid
      Kernel.kill('SIGTERM', @child_pid)
    rescue Errno::ESRCH
      # NOOP - Don't worry if the process has already finished
    end
  end
end
