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

module FlightScheduler
  class Configuration
    autoload(:Loader, 'flight_scheduler/configuration/loader')

    class ConfigError < StandardError; end

    ATTRIBUTES = [
      {
        name: :auth_type,
        env_var: true,
        default: 'munge',
      },
      {
        name: :controller_url,
        env_var: true,
        default: "http://127.0.0.1:6307/v0/ws",
      },
      {
        name: :node_name,
        env_var: true,
        default: ->(*_) { `hostname --short`.chomp },
      },
      {
        name: :log_level,
        env_var: true,
        default: 'info',
      },
      {
        name: :spool_dir,
        env_var: true,
        default: ->(root) { root.join('var/spool') },
        transform: ->(dir) { Pathname.new(dir) },
      },
      {
        name: :node_type,
        env_var: true
      },
      {
        name: :lower_ephemeral_port,
        env_var: true,
        default: 50000,
        transform: ->(int) { int.to_i }
      },
      {
        name: :upper_ephemeral_port,
        env_var: true,
        default: 51000,
        transform: ->(int) { int.to_i }
      }
    ]
    attr_accessor(*ATTRIBUTES.map { |a| a[:name] })

    def self.load(root)
      Loader.new(root, root.join('etc/flight-scheduler-daemon.yaml')).load.tap do |config|
        if config.lower_ephemeral_port < 1
          raise ConfigError, "The 'lower_ephemeral_port' must be positive"
        elsif config.upper_ephemeral_port < 1
          raise ConfigError, "The 'upper_ephemeral_port' must be positive"
        elsif config.lower_ephemeral_port > config.upper_ephemeral_port
          # NOTE: Technically the lower an upper port could be the same
          # This is not recommended as it will only allow one Stepd process to run
          # However it is the user's responsibility to prevent port exhaustion
          raise ConfigError, "The lower_ephemeral_port can not be greater than upper_ephemeral_port"
        elsif config.upper_ephemeral_port > 65535
          raise ConfigError, "The upper_ephemeral_port must be less than or equal to 65535"
        end
      end
    end

    def log_level=(level)
      @log_level = level
      Async.logger.send("#{@log_level}!")
    end
  end
end
