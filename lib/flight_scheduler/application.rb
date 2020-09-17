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

URL = "http://127.0.0.1:9292/v0/ws"
NODE = 'node01'

class MessageProcessor
  def initialize(connection)
    @connection = connection
  end

  def call(message)
    Async.logger.info("Processing message #{message.inspect}")
    command = message.first
    case command

    when 'JOB_ALLOCATED'
      _, job_id, script, arguments = message
      Async.logger.info("Running job:#{job_id} script:#{script} arguments:#{arguments}")
      sleep 2
      Async.logger.info("Job #{job_id} completed")
      @connection.write(['NODE_COMPLETED_JOB', job_id])
      @connection.flush

    else
      Async.logger.info("Unknown message #{message}")
    end
  rescue
    Async.logger.info("Error processing message #{$!.message}")
  end
end

module FlightScheduler
  # Class to store configuration and provide a singleton resource to lookup
  # that configuration.  Similar in nature to `Rails.app`.
  class Application
    def run
      Async do |task|
        endpoint = Async::HTTP::Endpoint.parse(URL)

        loop do
          Async.logger.info("Connecting to #{URL.inspect}")
          Async::WebSocket::Client.connect(endpoint) do |connection|
            Async.logger.info("Connected to #{URL.inspect}")
            processor = MessageProcessor.new(connection)
            connection.write ["CONNECTED", NODE]
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
