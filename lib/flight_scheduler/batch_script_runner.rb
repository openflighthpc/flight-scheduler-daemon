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

module FlightScheduler
  #
  # Run the given batch script and save a reference to the child process in
  # the job registry.
  #
  class BatchScriptRunner

    def initialize(script)
      @script = script
      @job = @script.job
    end

    def wait
      @task.wait
    end

    # Checks if the child process has exited correctly
    def success?
      return nil unless @status
      @status.success?
    end

    # Run the given batch script in a subprocess and return an Async::Task.
    #
    # Invariants:
    #
    # * Blocks until the job and script have been validated and recorded in
    #   the job registry.
    #
    # * Returns an Async::Task that can be `wait`ed on.  When the returned
    #   task has completed, the subprocess has completed and is no longer in
    #   the job registry.
    def run
      unless @script.valid?
        raise JobValidationError, <<~ERROR.chomp
          An unexpected error has occurred! The batch script does not appear
          to be in a valid state.
        ERROR
      end

      FlightScheduler.app.job_registry.add_runner(@job.id, 'BATCH', self)
      @task = Async do |task|
        # Fork to create the child process [Non Blocking]
        @child_pid = Kernel.fork do
          # Write the script_body to disk before we switch user.  We can't
          # assume that the new user can write to this directory.
          @script.write

          # Become the requested user and session leader
          Process::Sys.setgid(@job.gid)
          Process::Sys.setuid(@job.username)
          Process.setsid

          FileUtils.mkdir_p File.dirname(@script.stdout_path)
          FileUtils.mkdir_p File.dirname(@script.stderr_path)

          # Build the options hash
          opts = { unsetenv_others: true, close_others: true }
          if @script.stdout_path == @script.stderr_path
            opts.merge!({ [:out, :err] => @script.stdout_path })
          else
            opts.merge!(out: @script.stdout_path, err: @script.stderr_path)
          end

          Dir.chdir(@job.working_dir)
          # Exec into the job command
          Kernel.exec(@job.env, @script.path, *@script.arguments, **opts)
        rescue
          Async.logger.warn("Error forking script runner") { $! }
          raise
        end

        # Loop asynchronously until the child is finished
        until out = Process.wait2(@child_pid, Process::WNOHANG) do
          task.sleep 1
        end
        @status = out.last

        # Reset the child_pid, this prevents cancel killing other processes
        # which might spawn with the same PID
        @child_pid = nil
      ensure
        FlightScheduler.app.job_registry.remove_runner(@job.id, 'BATCH')
        @script.remove
      end
    end

    # Kills the associated subprocess
    def cancel
      return unless @child_pid
      Process.kill('SIGTERM', @child_pid)
    rescue Errno::ESRCH
      # NOOP - Don't worry if the process has already finished
    end
  end
end
