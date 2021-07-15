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

require "active_support/string_inquirer"
require "active_support/core_ext/object/blank"

require 'flight_scheduler/errors'

module FlightScheduler
  autoload(:Application, 'flight_scheduler/application')
  autoload(:Auth, 'flight_scheduler/auth')
  autoload(:BatchScript, 'flight_scheduler/batch_script')
  autoload(:Configuration, 'flight_scheduler/configuration')
  autoload(:Job, 'flight_scheduler/job')
  autoload(:JobRegistry, 'flight_scheduler/job_registry')
  autoload(:JobStep, 'flight_scheduler/job_step')
  autoload(:JobStepRunner, 'flight_scheduler/job_step_runner')
  autoload(:Jobd, 'flight_scheduler/jobd')
  autoload(:JobdRunner, 'flight_scheduler/jobd_runner')
  autoload(:MessageProcessor, 'flight_scheduler/message_processor')
  autoload(:MessageSender, 'flight_scheduler/message_sender')
  autoload(:Persistence, 'flight_scheduler/persistence')
  autoload(:Profiler, 'flight_scheduler/profiler')
  autoload(:Stepd, 'flight_scheduler/stepd')

  VERSION = "0.0.1"

  def app
    @app ||= Application.new(
      job_registry: JobRegistry.new,
    )
  end
  module_function :app

  def env
    @env ||= ActiveSupport::StringInquirer.new(
      ENV["RACK_ENV"].presence || "development"
    )
  end
  module_function :env

  def env=(environment)
    @env = ActiveSupport::StringInquirer.new(environment)
  end
  module_function :env=
end
