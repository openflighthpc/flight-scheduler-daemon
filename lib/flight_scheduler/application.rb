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
require 'async/http/endpoint'
require 'async/websocket/client'

module FlightScheduler
  # Class to store configuration and provide a singleton resource to lookup
  # that configuration.  Similar in nature to `Rails.app`.
  class Application
    attr_reader :job_registry

    def initialize(job_registry:)
      @job_registry = job_registry
    end

    def config
      @config ||= Configuration.new
    end

    def configure(&block)
      instance_eval(&block)
    end

    def root
      config.root
    end

    def run
      Async do |task|
        controller_url = FlightScheduler.app.config.controller_url
        endpoint = Async::HTTP::Endpoint.parse(controller_url)
        node = FlightScheduler.app.config.node_name

        loop do
          Async.logger.info("Connecting to #{controller_url.inspect}")
          Async::WebSocket::Client.connect(endpoint) do |connection|
            Async.logger.info("Connected to #{controller_url.inspect}")
            processor = MessageProcessor.new(connection)
            connection.write({ command: "CONNECTED", node: node })
            connection.flush
            while message = connection.read
              processor.call(message)
            end
            connection.close
            Async.logger.info("Connection closed")
          end
        rescue EOFError, Errno::ECONNREFUSED
          Async.logger.info("Connection closed: #{$!.message}")
          # XXX Incremental backoff.
          sleep 1
          retry
        end
      end
    end
  end
end
