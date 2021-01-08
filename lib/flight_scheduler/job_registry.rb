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

require 'concurrent'

module FlightScheduler
  #
  # Maintains a mapping from job id to subprocess.
  #
  # The mapping can be used to gain a reference to the job's process in order
  # to check its state or kill it.
  #
  # Adding and removing a entry is thread safe.
  #
  class JobRegistry
    class DuplicateJob < RuntimeError; end
    class DuplicateRunner < RuntimeError; end
    class UnknownJob < RuntimeError; end
    class DeallocatedJob < RuntimeError; end
    class TimedOutJob < RuntimeError; end

    def initialize
      @jobs = Concurrent::Hash.new
    end

    def each_job
      @jobs.each do |_, job|
        yield job[:job] if block_given?
      end
    end

    def add_job(job_id, job, start_timeout: true)
      if @jobs[job_id]
        raise DuplicateJob, job_id
      end
      @jobs[job_id] = { job: job, runners: Concurrent::Hash.new, deallocated: false }
      job.start_time_out_task if start_timeout
    end

    def add_runner(job_id, runner_id, runner)
      data = @jobs[job_id]
      raise UnknownJob, job_id if data.nil?
      raise TimedOutJob, job_id if data[:job].time_out?
      raise DeallocatedJob, job_id if data[:deallocated]
      runners = data[:runners]
      if runners[runner_id]
        raise DuplicateRunner, runner_id
      end
      runners[runner_id] = runner
    end

    def remove_job(job_id)
      lookup_job(job_id)&.remove
      @jobs.delete(job_id)
    end

    def remove_runner(job_id, runner_id)
      data = @jobs[job_id]
      return if data.nil?
      data[:runners].delete(runner_id)
    end

    def deallocate_job(job_id)
      data = @jobs[job_id]
      return if data.nil?
      data[:deallocated] = true
    end

    def lookup_job(job_id)
      data = @jobs[job_id]
      data.nil? ? nil : data[:job]
    end

    def lookup_job!(job_id)
      lookup_job(job_id) or raise UnknownJob, job_id
    end

    def lookup_runners(job_id)
      data = @jobs[job_id]
      data.nil? ? [] : data[:runners].to_a
    end

    def lookup_runner(job_id, runner_id)
      data = @jobs[job_id]
      data.nil? ? nil : data[:runners][runner_id]
    end

    def load
      data = persistence.load
      return if data.nil?
      data.each do |hash|
        job = Job.from_serialized_hash(hash)
        if job.valid?
          add_job(job.id, job, start_timeout: false)
        else
          Async.logger.warn("Invalid job loaded: #{job.inspect}")
        end
      end
    rescue
      Async.logger.warn("Error loading job registry") { $! }
      raise
    end

    def save
      serialized_jobs = @jobs.values.map { |h| h[:job].serializable_hash }
      persistence.save(serialized_jobs)
    end

    private

    def persistence
      @persistence ||= FlightScheduler::Persistence.new('job registry', 'job_state')
    end
  end
end
