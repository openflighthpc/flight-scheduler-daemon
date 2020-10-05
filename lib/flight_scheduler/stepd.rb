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

require 'socket'
require 'pty'

module FlightScheduler
  class Stepd
    def initialize(job, step)
      @job = job
      @step = step
      @address = 3608
    end

    def run
      if @step.pty?
        run_step_pty
      else
        run_step
      end
    end

    private

    def run_step_pty
      opts = {
        chdir: @job.working_dir,
        unsetenv_others: true,
      }
      env = @job.env.merge({'TERM' => 'xterm-256color'})
      PTY.spawn(env, @step.path, *@step.arguments, **opts) do |read, write, pid|
        connect_std_streams(read, write, pid, post_child_exit_sleep: false)
      end
    end

    def run_step
      input_rd, input_wr = IO.pipe
      output_rd, output_wr = IO.pipe
      child_pid = Kernel.fork do
        input_wr.close
        output_rd.close
        opts = {
          [:out, :err] => output_wr,
          in: input_rd,
          chdir: @job.working_dir,
          unsetenv_others: true,
        }
        Kernel.exec(@job.env, @step.path, *@step.arguments, **opts)
      end
      input_rd.close
      output_wr.close
      connect_std_streams(output_rd, input_wr, child_pid, post_child_exit_sleep: true)
    end


    def connect_std_streams(output_rd, input_wr, child_pid, post_child_exit_sleep: false)
      server = TCPServer.new(@address)
      begin
        Async.logger.info("stepd: listening on #{@address}")
        connection = server.accept
        Async.logger.info("stepd: received connection")
        output_thread = create_output_thread(output_rd, connection)
        input_thread = create_input_thread(input_wr, connection)

        Async.logger.info("stepd: waiting on child_pid:#{child_pid}")
        Process.wait(child_pid)
        Async.logger.debug("stepd: done waiting on child_pid:#{child_pid}")

        if post_child_exit_sleep
          # FSR we need to sleep here to allow the output to be sent across the
          # network reliably.
          sleep 0.1
        end

        # The process has finished running. We kill both the input and output
        # threads.  We also join against them to ensure that we don't close
        # the connection until the threads have performed their cleanup.
        # Without this not all output is sent across the network reliably.
        output_thread.kill
        input_thread.kill
        input_thread.join
        output_thread.join
      rescue
        Async.logger.warn("stepd: Error running stepd #{$!.message}")
      ensure
        connection.close if connection
        Async.logger.info("stepd: connection closed")
        begin
          Async.logger.debug("stepd: killing child_pid:#{child_pid}")
          Process.kill('SIGTERM', child_pid)
        rescue Errno::ESRCH
          # NOOP - Don't worry if the process has already finished
        end
      end
    end

    def create_output_thread(output_rd, connection)
      Thread.new do
        begin
          IO.copy_stream(output_rd, connection)
        rescue IOError, Errno::EBADF, Errno::EIO
        ensure
          # If the process is exits, we end up here.  We close the output
          # pipe.
          output_rd.close_read unless output_rd.closed?
          Async.logger.debug("stepd: output thread exited")
        end
      end
    end

    def create_input_thread(input_wr, connection)
      Thread.new do
        begin
          IO.copy_stream(connection, input_wr)
        rescue IOError, Errno::EBADF, Errno::EIO
        ensure
          # If the client closes its stdin or the client closes the
          # connection, we end up here.  We close the input pipe which may
          # cause the process to exit.
          input_wr.close_write unless input_wr.closed?
          Async.logger.debug("stepd: input thread exited")
        end
      end
    end
  end
end
