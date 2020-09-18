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

    def initialize
      @jobs = Concurrent::Hash.new
    end

    def add(job_id, process)
      if @jobs[job_id]
        raise DuplicateJob, job_id
      end
      @jobs[job_id] = process
    end

    def remove(job_id)
      @jobs.delete(job_id)
    end

    def [](job_id)
      @jobs[job_id]
    end
  end
end
