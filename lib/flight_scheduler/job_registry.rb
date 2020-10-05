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

    def initialize
      @jobs = Concurrent::Hash.new
    end

    def add_job(job_id, job)
      if @jobs[job_id]
        raise DuplicateJob, job_id
      end
      @jobs[job_id] = { job: job, runners: Concurrent::Hash.new }
    end

    def add_runner(job_id, runner_id, runner)
      data = @jobs[job_id]
      raise UnknownJob, job_id if data.nil?
      runners = data[:runners]
      if runners[runner_id]
        raise DuplicateRunner, runner_id
      end
      runners[runner_id] = runner
    end

    def remove_job(job_id)
      @jobs.delete(job_id)
    end

    def remove_runner(job_id, runner_id)
      data = @jobs[job_id]
      return if data.nil?
      data[:runners].delete(runner_id)
    end

    def lookup_job(job_id)
      data = @jobs[job_id]
      data.nil? ? nil : data[:job]
    end

    def lookup_job!(job_id)
      lookup_job(job_id) or raise UnknownJob, job_id
    end

    def lookup_runner(job_id, runner_id)
      data = @jobs[job_id]
      data.nil? ? nil : data[:runners][runner_id]
    end
  end
end
