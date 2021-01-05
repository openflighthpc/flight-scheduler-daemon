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
    class ServerCreateError < StandardError; end

    def initialize(job, step)
      @job = job
      @step = step
      @received_connection = false
    end

    def run
      Async do
        with_controller_connection do |connection|
          run_step do |read, write, child_pid|
            io_thread = connect_std_streams(read, write, child_pid)
            notify_started(connection)
            status = wait_for_child(child_pid, sleep_on_exit: !@step.pty?)
            wait_for_client_connection
            io_thread.kill
            notify_finished(status, connection)
          end
        end
      rescue
        Async.logger.warn { $! }
        raise
      end
    end

    private

    def env
      @job.env.merge(@step.env)
    end

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
        close_others: true
      }
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
          close_others: true
        }
        Kernel.exec(env, @step.path, *@step.arguments, **opts)
      end
      input_rd.close
      output_wr.close

      yield output_rd, input_wr, child_pid
    end

    def wait_for_child(child_pid, sleep_on_exit:)
      Async.logger.info("stepd: waiting on child_pid:#{child_pid}")
      _, status = Process.wait2(child_pid)
      Async.logger.debug("stepd: done waiting on child_pid:#{child_pid}")

      if sleep_on_exit
        # FSR we need to sleep here to allow the output to be sent across the
        # network reliably.
        # TODO: Investigate the reason for this sleep, does it need a
        # tcp_socket.closed? check?
        sleep FlightScheduler.app.config.generic_short_sleep
      end
      status
    end

    def wait_for_client_connection
      # If we haven't yet received a connection, it may be because we're
      # running a very quick command, which doesn't produce enough output to
      # block on a full output pipe.  Examples are `hostname` or `date`.
      #
      # We should wait a while longer in case a connection is soon to be
      # established.  If a connection is established we can assume that the
      # small amount of ouput is sent very quickly.
      #
      # NOTE: If we don't receive a connection within the maximum sleeping
      # period, it is likely it will never be received. However in practice
      # there are numerous situations where a timeout will occur prematurely.
      # Possible sources of delays are:
      # * When submitting the job step to another node(s),
      # * When the client connecting to another node, or
      # * During the clients long polling on job submission.
      #
      # Various small delays can build up, causing the connection to
      # timeout. Consider refactoring so connections are only closed when
      # they exit normally, are cancelled, the job/allocation timesout, or
      # NODE_DEALLOCATED is received.
      #
      # This may cause issues with too many open files, however this is
      # probably already possible. Consider implementing a job step limit.
      max_sleep = FlightScheduler.app.config.max_connection_sleep
      current_sleep = 0
      unless @received_connection
        loop do
          current_sleep += FlightScheduler.app.config.generic_short_sleep
          sleep FlightScheduler.app.config.generic_short_sleep
          if current_sleep >= max_sleep
            Async.logger.debug("No connection received. Giving up")
            break
          end
          if @received_connection
            # One additional sleep to make sure we have time to send all
            # output.
            sleep FlightScheduler.app.config.generic_short_sleep
            break
          end
        end
      end
    end

    def create_tcp_server
      # The range is shuffled to mitigate port collisions with existing stepd daemons
      enum = (FlightScheduler.app.config.stepd_port_start..FlightScheduler.app.config.stepd_port_end)
                .to_a.shuffle.each
      begin
        TCPServer.new('0.0.0.0', enum.next)
      rescue Errno::EADDRINUSE
        retry
      rescue StopIteration
        raise ServerCreateError, "Failed to create stepd server due to port exhaustion"
      end
    end

    def connect_std_streams(output_rd, input_wr, child_pid)
      server = create_tcp_server
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

    def notify_started(connection)
      connection.write({
        command: 'RUN_STEP_STARTED',
        job_id: @job.id,
        port: @port,
        step_id: @step.id,
      })
      connection.flush
    end

    def notify_finished(status, connection)
      command = status.success? ? 'RUN_STEP_COMPLETED' : 'RUN_STEP_FAILED'
      connection.write(command: command, job_id: @job.id, step_id: @step.id)
      connection.flush
    end

    def with_controller_connection(&block)
      controller_url = FlightScheduler.app.config.controller_url
      endpoint = Async::HTTP::Endpoint.parse(controller_url)
      auth_token = FlightScheduler::Auth.token

      Async.logger.info("Connecting to #{controller_url.inspect}") { endpoint }
      Async::WebSocket::Client.connect(endpoint) do |connection|
        Async.logger.info("Connected to #{controller_url.inspect}")
        @connection = connection
        connection.write({
          command: 'STEPD_CONNECTED',
          auth_token: auth_token,
          name: "#{@job.id}.#{@step.id}",
        })
        connection.flush
        block.call(connection)
      end
    end
  end
end
