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
  class Jobd
    def initialize(job)
      @job = job
      @deallocated = false
      @steps = []
    end

    def run
      Async do
        trap('SIGTERM') { job_terminated(130) }
        trap('SIGINT') { job_terminated(143) }

        with_connection do
          start_time_out_task

          while message = @connection.read
            case message[:command]
            when 'RUN_SCRIPT'
              run_script(message)
            when 'RUN_STEP'
              run_step(message)
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

    def run_step(message)
      arguments = message[:arguments]
      env       = message[:environment]
      path      = message[:path]
      pty       = message[:pty]
      step_id   = message[:step_id]

      Async.logger.debug("Running step:#{step_id} for job:#{@job.id} path:#{path} arguments:#{arguments}")
      step = JobStep.new(@job, step_id, path, arguments, pty, env)
      @steps << JobStepRunner.new(step).run
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
          command: 'JOBD_CONNECTED',
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

    # Preform a graceful shutdown of Jobd
    def job_cancelled
      Async.logger.info("Cancelling job:#{@job.id}")

      # Deallocate the job to prevent any further job steps
      @deallocated = true

      # Terminate the batch script and steps
      @steps.each(&:cancel)
      send_signal('TERM')
    end

    # Preform a hard shutdown of Jobd
    def job_terminated(exitcode)
      Async.logger.info("Terminate job:#{@job.id}")

      # Deallocate the job to prevent any further job steps
      @deallocated = true

      # Terminate the batch script and steps
      @steps.each(&:cancel)
      send_signal('TERM')

      # Give the child and step process time to exit
      sleep FlightScheduler.app.config.generic_long_sleep
      if running?
        sleep 90
        send_signal('KILL')
        @steps.each { |s| s.send_signal('KILL') }
      end

      exit exitcode
    end

    def send_signal(sig)
      return unless @child_pid
      Async.logger.debug "Sending #{sig} to Process Group #{@child_pid}"
      Process.kill(-Signal.list[sig], @child_pid)
    rescue Errno::ESRCH
      # NOOP - Don't worry if the process has already finished
    end

    def running?
      if send_signal('EXIT')
        true
      elsif @steps.any? { |s| s.send_signal('EXIT') }
        true
      else
        false
      end
    end

    def start_time_out_task
      return if @job.time_out.nil?
      Async do |task|
        remaining_time = @job.time_out + @job.created_time - Process.clock_gettime(Process::CLOCK_MONOTONIC)
        Async.logger.info "Job '#{@job.id}' will start timing out in '#{remaining_time.to_i}' seconds"
        while !@job.time_out? || running?
          if @timed_out_time || @job.time_out?
            if @timed_out_time
              first = false
            else
              Async.logger.info "Job Timed Out: #{@job.id}"
              @timed_out_time = Process.clock_gettime(Process::CLOCK_MONOTONIC).to_i
              first = true
            end

            if first
              send_signal("TERM")
              @steps.each { |s| s.send_signal('TERM') }
              @connection.write(command: 'JOB_TIMED_OUT')

              # Allow fast exiting runners to finalise quickly
              task.yield
            elsif (Process.clock_gettime(Process::CLOCK_MONOTONIC).to_i - @timed_out_time) > 90
              send_signal("KILL")
              @steps.each { |s| s.send_signal('KILL') }

              # Ensure slow exiting runners have finished
              task.sleep FlightScheduler.app.config.generic_long_sleep
            end
          end
          task.sleep FlightScheduler.app.config.generic_long_sleep
        end

        if @timed_out_time
          Async.logger.debug "Finished time out handling for job: #{@job.id}"
        end
      end
    end
  end
end
