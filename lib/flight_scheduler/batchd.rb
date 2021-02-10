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
require 'async/http/endpoint'
require 'async/websocket/client'

module FlightScheduler
  class Batchd
    def initialize(job)
      @job = job
      @deallocated = false
    end

    def run
      Async do
        # TODO: Rework timeout
        # job.start_time_out_task
        with_connection do
          while message = @connection.read
            case message[:command]
            when 'RUN_SCRIPT'
              run_script(message)
            when 'JOB_CANCELLED'
              job_cancelled
            else
              Async.logger.error("Unrecognised message: #{message[:command]}")
            end

            Async.logger.info("Processed #{message[:command]}: jobd - #{@job.id}")
          end
        end
      rescue
        Async.logger.warn { $! }
        raise
      end
    end

    private

    def run_script(message)
      # Create a script
      # TODO: Validate me!!
      arguments   = message[:arguments]
      script_body = message[:script]
      stderr      = message[:stderr_path]
      stdout      = message[:stdout_path]
      script = BatchScript.new(@job, script_body, arguments, stdout, stderr)

      @child_pid = Kernel.fork do
        # Write the script before changing user permissions
        script.write

        # Become the user and session leader
        Process::Sys.setgid(@job.gid)
        Process::Sys.setuid(@job.username)
        Process.setsid

        # Generate the output files as the user
        FileUtils.mkdir_p(File.dirname(script.stdout_path))
        FileUtils.mkdir_p(File.dirname(script.stderr_path))

        # Setup the environment and file redirects
        opts = { unsetenv_others: true, close_others: true, chdir: @job.working_dir }
        if script.stdout_path == script.stderr_path
          opts.merge!({ [:out, :err] => script.stdout_path })
        else
          opts.merge!(out: script.stdout_path, err: script.stderr_path)
        end

        # Start the job
        Kernel.exec(@job.env, script.path, *script.arguments, **opts)
      end

      # Asynchronously wait for the process to finish
      Async do |task|
        Async.logger.info("batchd: waiting on child_pid:#{@child_pid}")
        until out = Process.wait2(@child_pid, Process::WNOHANG)
          task.sleep FlightScheduler.app.config.generic_long_sleep
        end
        Async.logger.debug("batchd: done waiting on child_pid:#{@child_pid}")
        _, status = out

        # Notify the controller the process has finished
        command = status.success? ? 'NODE_COMPLETED_JOB' : 'NODE_FAILED_JOB'
        @connection.write(command: command)
        @connection.flush
      end
    end

    def job_cancelled
      Async.logger.info("Cancelling job:#{@job.id}")

      # Deallocate the job to prevent any further job steps
      @deallocated = true

      # TODO: Cancel all the existing runners!
      # FlightScheduler.app.job_registry.lookup_runners(job_id).each do |_, runner|
      #   runner.cancel
      # end

      # Terminate the batch script
      send_signal('TERM')
    end

    def with_connection
      raise UnexpectedError, 'a connection has already been established' if @connection

      controller_url = FlightScheduler.app.config.controller_url
      endpoint = Async::HTTP::Endpoint.parse(controller_url)
      auth_token = FlightScheduler::Auth.token

      Async.logger.info("Connecting to #{controller_url.inspect}") { endpoint }
      Async::WebSocket::Client.connect(endpoint) do |connection|
        Async.logger.info("Connected to #{controller_url.inspect}")
        @connection = connection
        connection.write({
          command: 'BATCHD_CONNECTED',
          auth_token: auth_token,
          job_id: @job.id
        })
        connection.flush
        yield if block_given?
      end
    ensure
      @connection.close if @connection && ! @connection.closed?
      @connection = nil
    end

    def send_signal(sig)
      return unless @child_pid
      Async.logger.debug "Sending #{sig} to Process Group #{@child_pid}"
      Process.kill(-Signal.list[sig], @child_pid)
    rescue Errno::ESRCH
      # NOOP - Don't worry if the process has already finished
    end
  end
end
