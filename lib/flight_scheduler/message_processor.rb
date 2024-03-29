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

require 'active_support/core_ext/hash/except'

module FlightScheduler
  #
  # Process incoming messages and send responses.
  #
  class MessageProcessor
    def call(message)
      Async.logger.info("[daemon] Processing message #{sanitize_message(message).inspect}")
      command = message[:command]
      case command

      when 'JOB_ALLOCATED'
        job_id    = message[:job_id]
        env       = message[:environment]
        username  = message[:username]
        time_out  = message[:time_limit]

        Async.logger.debug("Environment: #{env.map { |k, v| "#{k}=#{v}" }.join("\n")}")
        begin
          job = FlightScheduler::Job.new(job_id, env, username, time_out)
          if job.valid?
            job.write
            FlightScheduler.app.job_registry.add_job(job.id, job)
            FlightScheduler.app.job_registry.save

            # Start the jobd process
            FlightScheduler::JobdRunner.new(job).run
          else
            raise JobValidationError, <<~ERROR.chomp
              An unexpected error has occurred! The job does not appear to be
              in a valid state.
            ERROR
          end
        rescue
          Async.logger.warn("[daemon] Error configuring job #{job_id}") { $! }
          MessageSender.send(command: 'JOB_ALLOCATION_FAILED', job_id: job_id)
        end

      when 'JOB_DEALLOCATED'
        job_id = message[:job_id]
        Async.logger.info("[daemon] Deallocating job:#{job_id}")

        # Deallocate the job to prevent any further job steps
        FlightScheduler.app.job_registry.deallocate_job(job_id)

        # Report back when all the runners have stop
        Async do |task|
          # Cancel any current runners.
          #
          # There shouldn't be any for successfully completed jobs.  For
          # failed jobs, there could be job step runners which have gotten
          # stuck for reasons
          FlightScheduler.app.job_registry.lookup_runners(job_id).each do |_, runner|
            runner.cancel
          end

          until FlightScheduler.app.job_registry.lookup_runners(job_id).empty?
            task.sleep FlightScheduler.app.config.generic_short_sleep
          end
          FlightScheduler.app.job_registry.remove_job(job_id)
          FlightScheduler.app.job_registry.save
          MessageSender.send(command: 'NODE_DEALLOCATED', job_id: job_id)
        end

      else
        Async.logger.info("[daemon] Unknown message #{sanitize_message(message)}")
      end
      Async.logger.debug("[daemon] Processed message #{message.inspect}")
    rescue
      Async.logger.warn("[daemon] Error processing message #{$!.message}")
      Async.logger.debug $!.full_message
    end

    def sanitize_message(message)
      message.except(:environment, :script)
    end
  end
end
