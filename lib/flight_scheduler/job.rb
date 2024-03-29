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

require 'etc'

module FlightScheduler
  class Job

    attr_reader :id, :username, :time_out, :created_time

    def self.from_serialized_hash(hash)
      id, username, time_out, created_time = hash.stringify_keys
                                                 .slice(*%w(id username time_out created_time))
                                                 .values
      new(id, nil, username, time_out, created_time: created_time)
    end

    def initialize(id, env, username, time_out, created_time: nil)
      @id = id
      @env = env
      @username = username
      @time_out = time_out
      @created_time = created_time || Process.clock_gettime(Process::CLOCK_MONOTONIC).to_i
    end

    # Checks the various parameters are in the correct format before running
    # This is to prevent rogue data being passed Process.spawn or rm -f
    def valid?
      return false unless id.is_a?(String)
      return false unless /\A[\w-]+\Z/.match? id
      return false unless env.is_a? Hash
      return false unless passwd
      return false unless @time_out.nil? || (@time_out.is_a?(Integer) && @time_out >= 0)
      true
    end

    def env
      # We deliberately don't cache the value here.
      return adjusted_env if @env && @env.is_a?(Hash)
      serialized_env = Sync do
        File.read(env_path)
      rescue Errno::ENOENT
        Async.logger.warn("Unable to load environment for job #{id}") { $! }
        nil
      end
      if serialized_env.nil?
        nil
      else
        JSON.load(serialized_env)
      end
    end

    def home_dir
      passwd.dir
    end

    def working_dir
      home_dir
    end

    def gid
      passwd.gid
    end

    def serializable_hash
      { id: id, username: username, time_out: @time_out, created_time: @created_time }
    end

    def passwd
      @passwd ||= Etc.getpwnam(username)
    rescue ArgumentError
      # NOOP - The user can not be found, this is handled in valid?
    end

    def write
      Sync do
        FileUtils.mkdir_p(dirname)
        File.write(env_path, JSON.dump(adjusted_env))
        # We don't want the env hanging around in memory.
        @env = nil
      end
    end

    def dirname
      spool_dir = FlightScheduler.app.config.spool_dir
      spool_dir.join('state', id)
    end

    def remove
      Sync do
        FileUtils.rm_rf(dirname)
      end
    end

    def time_out?
      return false if [nil, 0].include?(@time_out)
      Process.clock_gettime(Process::CLOCK_MONOTONIC).to_i > @created_time + @time_out
    end

    private

    def env_path
      dirname.join(dirname, 'environment.json').to_path
    end

    def adjusted_env
      stringified = @env.map { |k, v| [k.to_s, v] }.to_h
      stringified.merge(
        'HOME' => home_dir,
        'LOGNAME' => username,
        'PATH' => '/bin:/sbin:/usr/bin:/usr/sbin',
        'USER' => username,
        'flight_ROOT' => ENV['flight_ROOT'],
      )
    end
  end
end
