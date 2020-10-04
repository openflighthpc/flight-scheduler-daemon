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

module FlightScheduler
  #
  # Process incoming messages and send responses.
  #
  class MessageProcessor
    def initialize(connection)
      @connection = connection
    end

    def call(message)
      Async.logger.info("Processing message #{message.inspect}")
      command = message[:command]
      case command

      when 'JOB_ALLOCATED'
        job_id    = message[:job_id]
        env       = message[:environment]
        username  = message[:username]

        Async.logger.debug("Environment: #{env.map { |k, v| "#{k}=#{v}" }.join("\n")}")
        begin
          job = FlightScheduler::Job.new(job_id, env, username)
          FlightScheduler.app.job_registry.add_job(job.id, job)
        rescue
          Async.logger.info("Error configuring job #{job_id} #{$!.message}")
          @connection.write({command: 'JOB_ALLOCATION_FAILED', job_id: job_id})
          @connection.flush
        end

      when 'RUN_SCRIPT'
        arguments   = message[:arguments]
        job_id      = message[:job_id]
        script_body = message[:script]
        stderr      = message[:stderr_path]
        stdout      = message[:stdout_path]

        Async.logger.debug("Running script for job:#{job_id} script:#{script_body} arguments:#{arguments}")
        error_handler = lambda do
          Async.logger.info("Error running script job:#{job_id} #{$!.message}")
          if message[:array_job_id]
            @connection.write({
              command: 'NODE_FAILED_ARRAY_TASK',
              array_job_id: message[:array_job_id],
              array_task_id: message[:array_task_id],
            })
          else
            @connection.write({command: 'NODE_FAILED_JOB', job_id: job_id})
          end
          @connection.flush
        end
        begin
          job = FlightScheduler.app.job_registry.lookup_job(job_id)
          script = BatchScript.new(job, script_body, arguments, stdout, stderr)
          runner = FlightScheduler::BatchScriptRunner.new(script)
          runner.run
        rescue
          error_handler.call
        else
          Async do
            runner.wait
            Async.logger.info("Completed job #{job_id}")
            if message[:array_job_id]
              command = runner.success? ?
                'NODE_COMPLETED_ARRAY_TASK' :
                'NODE_FAILED_ARRAY_TASK'
              @connection.write({
                command: command,
                array_job_id: message[:array_job_id],
                array_task_id: message[:array_task_id],
              })
            else
              command = runner.success? ? 'NODE_COMPLETED_JOB' : 'NODE_FAILED_JOB'
              @connection.write({command: command, job_id: job_id})
            end
            @connection.flush
          rescue
            error_handler.call
          end
        end

      when 'JOB_CANCELLED'
        job_id = message[:job_id]
        Async.logger.info("Cancelling job:#{job_id}")
        job_runner = FlightScheduler.app.job_registry.lookup_runner(job_id, 'BATCH')
        job_runner.cancel if job_runner
        # The JOB_ALLOCATED task will report back that the process has failed.
        # We don't need to send any messages to the controller here.

      else
        Async.logger.info("Unknown message #{message}")
      end
      Async.logger.debug("Processed message #{message.inspect}")
    rescue
      Async.logger.warn("Error processing message #{$!.message}")
    end
  end
end
