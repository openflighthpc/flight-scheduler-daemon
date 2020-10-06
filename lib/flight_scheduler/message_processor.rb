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
    # TODO: Remove the particular instance of connection as it might drop out
    # during long running operations. Instead use MessageSender which polls for
    # the currently open connection
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

      when 'RUN_STEP'
        arguments = message[:arguments]
        job_id    = message[:job_id]
        path      = message[:path]
        step_id   = message[:step_id]

        Async.logger.debug("Running step:#{step_id} for job:#{job_id} path:#{path} arguments:#{arguments}")
        error_handler = lambda do
          Async.logger.info("Error running step:#{step_id} for job:#{job_id} #{$!.message}")
          @connection.write({command: 'RUN_STEP_FAILED', job_id: job_id, step_id: step_id})
          @connection.flush
        end
        begin
          job = FlightScheduler.app.job_registry.lookup_job!(job_id)
          step = JobStep.new(job, step_id, path, arguments)
          runner = JobStepRunner.new(step)
          runner.run
        rescue
          error_handler.call
        else
          Async do
            runner.wait
            Async.logger.info("Completed step for job #{job_id}")
            Async.logger.debug("Output: #{runner.output}")
            command = runner.success? ? 'RUN_STEP_COMPLETED' : 'RUN_STEP_FAILED'
            @connection.write({command: command, job_id: job_id, step_id: step_id})
            @connection.flush
          rescue
            error_handler.call
          end
        end

      when 'JOB_CANCELLED'
        job_id = message[:job_id]
        Async.logger.info("Cancelling job:#{job_id}")

        # Deallocate the job to prevent any further job steps
        FlightScheduler.app.job_registry.deallocate_job(job_id)

        Async do |task|
          # Allow other tasks to run a final time before cancelling
          # This is a last attempt to collect any finished processes
          task.yield

          # Cancel all current runners
          FlightScheduler.app.job_registry.lookup_runners(job_id).each do |_, runner|
            runner.cancel
          end

          # Wait for the runners to finish and remove the job
          task.yield until FlightScheduler.app.job_registry.lookup_runners(job_id).empty?
          FlightScheduler.app.job_registry.remove_job(job_id)
          MessageSender.send(command: 'NODE_DEALLOCATED', job_id: job_id)
        end

      when 'JOB_DEALLOCATED'
        job_id = message[:job_id]
        Async.logger.info("Deallocating job:#{job_id}")

        # Deallocate the job to prevent any further job steps
        FlightScheduler.app.job_registry.deallocate_job(job_id)

        # Report back when all the runners have stop
        Async do |task|
          task.yield until FlightScheduler.app.job_registry.lookup_runners(job_id).empty?
          FlightScheduler.app.job_registry.remove_job(job_id)
          MessageSender.send(command: 'NODE_DEALLOCATED', job_id: job_id)
        end

      else
        Async.logger.info("Unknown message #{message}")
      end
      Async.logger.debug("Processed message #{message.inspect}")
    rescue => e
      Async.logger.warn("Error processing message #{$!.message}")
      Async.logger.debug e.full_message
    end
  end
end
