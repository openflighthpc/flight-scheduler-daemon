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

require 'nokogiri'

module FlightScheduler
  class Profiler
    MEM_TOTAL_REGEX = /^MemTotal:\s*(?<size>\d+)\s*kB$/

    def self.run_lshw_xml
      # TODO: Run the lshw command here
      ''
    end

    def self.read_meminfo
      File.read('/proc/meminfo')
    end

    def cpus
      parser.xpath('//node[starts-with(@id, "cpu")]').length
    end

    def gpus
      parser.xpath('//node[starts-with(@id, "display")]').length
    end

    # NOTE: The memory info comes form /proc/meminfo as lshw is unreliable
    #       The meminfo *says* its unit is kB (1000 B) but it's actually KiB (1024 B)
    #       https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/6/html/deployment_guide/s2-proc-meminfo
    def memory
      @memory ||= MEM_TOTAL_REGEX.match(self.class.read_meminfo)
                                 .named_captures['size']
                                 .to_i * 1024
    end

    private

    def parser
      @parser ||= Nokogiri::XML(self.class.run_lshw_xml)
    end
  end
end
