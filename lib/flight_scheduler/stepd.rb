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
      @received_connection = false
    end

    def run
      run_step do |read, write, child_pid|
        io_thread = connect_std_streams(read, write, child_pid)
        notify_controller
        wait_for_child(child_pid, sleep_on_exit: !@step.pty?)
        wait_for_connection
        io_thread.kill
      end
    end

    private

    def run_step(&block)
      if @step.pty?
        run_step_pty(&block)
      else
        run_step_no_pty(&block)
      end
    end

    def run_step_pty
      opts = {
        chdir: @job.working_dir,
        unsetenv_others: true,
      }
      env = @job.env.merge({'TERM' => 'xterm-256color'})
      PTY.spawn(env, @step.path, *@step.arguments, **opts) do |read, write, pid|
        yield read, write, pid
      end
    end

    def run_step_no_pty
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

      yield output_rd, input_wr, child_pid
    end

    def wait_for_child(child_pid, sleep_on_exit:)
      Async.logger.info("stepd: waiting on child_pid:#{child_pid}")
      Process.wait(child_pid)
      Async.logger.debug("stepd: done waiting on child_pid:#{child_pid}")

      if sleep_on_exit
        # FSR we need to sleep here to allow the output to be sent across the
        # network reliably.
        sleep 0.1
      end
    end

    def wait_for_connection
      # If we haven't yet received a connection, it may be because we're
      # running a very quick command, which doesn't produce enough output to
      # block on a full output pipe.  Examples are `hostname` or `date`.
      #
      # We should wait a while longer in case a connection is soon to be
      # established.  If a connection is established we can assume that the
      # small amount of ouput is sent very quickly.
      #
      # If we don't receive a connection within max_sleep seconds, we're
      # unlikely to receive one at all.  The output will be lost.
      max_sleep = 5
      current_sleep = 0
      unless @received_connection
        loop do
          current_sleep += 0.1
          sleep 0.1
          if current_sleep >= max_sleep
            Async.logger.debug("No connection received. Giving up")
            break
          end
          if @received_connection
            # One additional sleep to make sure we have time to send all
            # output.
            sleep 0.1
            break
          end
        end
      end
    end

    def connect_std_streams(output_rd, input_wr, child_pid)
      server = TCPServer.new('0.0.0.0', 0)
      @port = server.addr[1]
      Thread.new do
        output_thread = nil
        input_thread = nil
        begin
          Async.logger.info("stepd: listening on #{server.addr[2]}:#{server.addr[1]}")
          connection = server.accept
          @received_connection = true
          Async.logger.info("stepd: received connection")
          output_thread = create_output_thread(output_rd, connection)
          input_thread = create_input_thread(input_wr, connection)
          output_thread.join
          input_thread.join
        rescue
          Async.logger.warn("stepd: Error running stepd #{$!.message} -- #{$!.class.name}")
        ensure
          begin
            # We kill both the input and output threads.  We also join against
            # them to ensure that we don't close the connection until the threads
            # have performed their cleanup.  Without this not all output is sent
            # across the network reliably.
            output_thread.kill if output_thread
            input_thread.kill if input_thread
            output_thread.join if output_thread
            input_thread.join if input_thread
            connection.close if connection
            Async.logger.info("stepd: connection closed")
            begin
              Async.logger.debug("stepd: killing child_pid:#{child_pid}")
              Process.kill('SIGTERM', child_pid)
            rescue Errno::ESRCH
              # NOOP - Don't worry if the process has already finished
            end
          rescue
            Async.logger.warn("stepd: Unexpected error when cleaning up #{$!.message}")
          end
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

    def notify_controller
      MessageSender.send({
        command: 'RUN_STEP_STARTED',
        job_id: @job.id,
        port: @port,
        step_id: @step.id,
      })
    end
  end
end
