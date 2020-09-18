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
require 'async/process'

module FlightScheduler
  #
  # Run the given job script and save a reference to it in the job registry.
  #
  # Current limitations:
  #
  # * The environment is not cleaned up before execution.
  # * The environment is not set according to the options given to the
  #   scheduler.
  # * The job's standard and error output is not saved to disk.
  #
  module JobRunner
    extend self

    def run_job(job_id, *arguments, **options)
      child = Async::Process::Child.new(*arguments, **options)
      FlightScheduler.app.job_registry.add(job_id, child)
      child.wait
    ensure
      FlightScheduler.app.job_registry.remove(job_id)
    end
  end
end
