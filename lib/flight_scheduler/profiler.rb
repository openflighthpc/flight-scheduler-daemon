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
require 'open3'

module FlightScheduler
  class Profiler
    class ProfilerError < RuntimeError; end

    MEM_TOTAL_REGEX = /^MemTotal:\s*(?<size>\d+)\s*kB$/

    # NOTE: The lshw command needs root permissions to function correctly. This
    # shouldn't be a problem as the daemon already has these permissions to switch
    # users. However it may cause issues if this was to change
    def self.run_lshw_xml
      out, err, status = Open3.capture3('lshw', '-xml',
                                        close_others: true,
                                        unsetenv_others: true)
      if status.success?
        return out.chomp
      else
        Async.logger.debug <<~DEBUG
          An error occurred when running lshw -xml:
          STATUS: #{status.exitstatus}
          STDOUT:
          #{out}
          STDERR:
          #{err}
        DEBUG
        raise ProfilerError, <<~ERROR
          Failed to determine the hardware information
        ERROR
      end
    end

    def self.read_meminfo
      File.read('/proc/meminfo')
    rescue
      Async.logger.debug($!.full_message)
      raise ProfilerError, <<~ERROR.chomp
        Failed to determine the memory information
      ERROR
    end

    def log
      Async.logger.info <<~PROFILE
        Profiler Results:
        cpus:   #{cpus}
        gpus:   #{gpus}
        memory: #{memory}
      PROFILE
    end

    # Currently assumes hyperthreading is the same as additional cores
    # This ensures consistency with the output form nproc
    # Consider revisiting
    def cpus
      @cpus ||= parser.xpath('//node[starts-with(@id, "cpu")]').reduce(0) do |sum, cpu|
        config = cpu.xpath('configuration/setting').each_with_object({}) do |config, memo|
          key, value = config.values
          memo[key] = value.to_i
        end

        if config.key?('threads')
          sum += config['threads']
        elsif config.key?('enabledcores')
          sum += config['enabledcores']
        elsif config.key?('cores')
          sum += config['cores']
        else
          sum += 1
        end
      end
    end

    def gpus
      @gpus ||= parser.xpath('//node[starts-with(@id, "display")]').length
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
