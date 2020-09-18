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
        job_id, script, arguments = message[:job_id], message[:script], message[:arguments]
        Async.logger.info("Running job:#{job_id} script:#{script} arguments:#{arguments}")
        begin
          task = FlightScheduler::JobRunner.run_job(job_id, script, *arguments)
        rescue
          Async.logger.info("Error running job #{job_id} #{$!.message}")
          @connection.write({command: 'NODE_FAILED_JOB', job_id: job_id})
          @connection.flush
        else
          Async do
            status = task.wait
            Async.logger.info("Completed job #{job_id}")
            command = status.exitstatus == 0 ? 'NODE_COMPLETED_JOB' : 'NODE_FAILED_JOB'
            @connection.write({command: command, job_id: job_id})
            @connection.flush
          end
        end

      else
        Async.logger.info("Unknown message #{message}")
      end
      Async.logger.debug("Processed message #{message.inspect}")
    rescue
      Async.logger.warn("Error processing message #{$!.message}")
    end
  end
end
