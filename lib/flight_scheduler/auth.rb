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

require 'active_support/core_ext/string/inflections'
require 'timeout'
require 'open3'

module FlightScheduler
  module Auth
    class AuthenticationError < RuntimeError; end
    class UnknownAuthType < AuthenticationError; end

    # Return an auth token identifying the node this daemon is running on.
    def self.token
      auth_type = FlightScheduler.app.config.auth_type
      const_string = auth_type.classify
      auth_type = const_get(const_string)
    rescue NameError
      Async.logger.warn("Auth type not found: #{self}::#{const_string}")
      raise UnknownAuthType, "Unknown auth type #{name}"
    else
      auth_type.call
    end

    module Basic
      def self.call
        # This is the no-auth option.  Just return the node name.
        FlightScheduler.app.config.node_name
      end
    end

    module Munge
      def self.call
        payload = "NODE_NAME: #{FlightScheduler.app.config.node_name}"
        token, _status = Timeout.timeout(2) {
          Open3.capture2('munge', stdin_data: payload)
        }
        if token.nil?
          raise AuthenticationError, "Unable to obtain munge token"
        end
        token
      rescue Timeout::Error
        raise AuthenticationError, "Unable to obtain munge token"
      end
    end
  end
end
