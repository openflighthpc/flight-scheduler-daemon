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
        script    = message[:script]
        arguments = message[:arguments]
        env       = message[:environment]
        username  = message[:username]

        Async.logger.info("Running job:#{job_id} script:#{script} arguments:#{arguments}")
        Async.logger.debug("Environment: #{env.map { |k, v| "#{k}=#{v}" }.join("\n")}")
        begin
          job = FlightScheduler::JobRunner.new(job_id, env, script, arguments, username)
          job.run
        rescue
          Async.logger.info("Error running job #{job_id} #{$!.message}")
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
        else
          Async do
            job.wait
            Async.logger.info("Completed job #{job_id}")
            if message[:array_job_id]
              command = job.success? ?
                'NODE_COMPLETED_ARRAY_TASK' :
                'NODE_FAILED_ARRAY_TASK'
              @connection.write({
                command: command,
                array_job_id: message[:array_job_id],
                array_task_id: message[:array_task_id],
              })
            else
              command = job.success? ? 'NODE_COMPLETED_JOB' : 'NODE_FAILED_JOB'
              @connection.write({command: command, job_id: job_id})
            end
            @connection.flush
          end
        end

      when 'JOB_CANCELLED'
        job_id = message[:job_id]
        Async.logger.info("Cancelling job:#{job_id}")
        FlightScheduler.app.job_registry[job_id].cancel
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
