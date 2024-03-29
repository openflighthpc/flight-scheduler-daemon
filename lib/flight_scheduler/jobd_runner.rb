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
  # Start a jobd process for the given job and save a reference to the child
  # process in the job registry.
  #
  class JobdRunner

    def initialize(job)
      @job = job
    end

    # Run a jobd process for the job in a subprocess and return an Async::Task.
    #
    # Invariants:
    #
    # * Blocks until the job has been validated and recorded in the job
    # registry.
    #
    # * Returns an Async::Task that can be `wait`ed on.  When the returned
    #   task has completed, the subprocess has completed and is no longer in
    #   the job registry.
    def run
      unless @job.valid?
        raise JobValidationError, <<~ERROR.chomp
          An unexpected error has occurred! The job does not appear to be in a
          valid state.
        ERROR
      end

      FlightScheduler.app.job_registry.add_runner(@job.id, 'BATCH', self)
      Async do |task|
        # Fork to create the child process [Non Blocking]
        @child_pid = Kernel.fork do
          # Ignore SIGTERM in the parent. It has been sent to the children.
          trap('SIGTERM') {}

          # Become the session leader
          Process.setsid

          # We've inherited the running thread when we forked.  The runner
          # thread contains a running `::Async::Reactor` which doesn't play
          # nicely with `fork`.  We shut it down here and create a new thread
          # so that we can safely start a new reactor.
          #
          # XXX This could all be avoided by execing into a new process here.
          reactor = ::Async::Task.current.reactor
          thread = Thread.new do
            reactor.close
            until reactor.closed?
              sleep FlightScheduler.app.config.generic_short_sleep
            end

            # Start the Jobd daemon
            Jobd.new(@job).run.wait
          end
          thread.join

        rescue
          Async.logger.warn("[daemon] Error forking jobd") { $! }
          raise
        end

        # Loop asynchronously until the child is finished
        until Process.wait2(@child_pid, Process::WNOHANG) do
          task.sleep FlightScheduler.app.config.generic_long_sleep
        end

        # Reset the child_pid, this prevents cancel killing other processes
        # which might spawn with the same PID
        @child_pid = nil
      ensure
        FlightScheduler.app.job_registry.remove_runner(@job.id, 'BATCH')
      end
    end

    def send_signal(sig)
      return unless @child_pid
      Async.logger.debug "[daemon] Sending #{sig} to process group #{@child_pid}"
      Process.kill(-Signal.list[sig], @child_pid)
    rescue Errno::ESRCH
      # NOOP - Don't worry if the process has already finished
    end

    # Kills the associated subprocess
    def cancel
      send_signal('TERM')
    end
  end
end
