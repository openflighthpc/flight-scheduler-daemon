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

module FlightScheduler
  class Job

    attr_reader :id, :script_body, :arguments, :username

    def initialize(id, env, script_body, arguments, username, stdout_path, stderr_path)
      @id = id
      @env = env
      @script_body = script_body
      @arguments = arguments
      @username = username
    end

    def path
      FlightScheduler.app.config.spool_dir.join('state', id, 'job-script').to_path
    end

    # Checks the various parameters are in the correct format before running
    # This is to prevent rogue data being passed Process.spawn or rm -f
    def valid?
      return false unless /\A[\w-]+\Z/.match? id
      return false unless env.is_a? Hash
      return false unless arguments.is_a? Array
      return false unless passwd
      return false if stdout_path.to_s.empty?
      return false if stderr_path.to_s.empty?
      true
    end

    def env
      stringified = super.map { |k, v| [k.to_s, v] }.to_h
      stringified.merge(
        'HOME' => home_dir,
        'LOGNAME' => username,
        'PATH' => '/bin:/sbin:/usr/bin:/usr/sbin',
        'USER' => username,
        'flight_ROOT' => ENV['flight_ROOT'],
      )
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

    def stdout_path
      File.expand_path(@stdout_path, home_dir)
    end

    def stderr_path 
      File.expand_path(@stderr_path, home_dir)
    end

    def write_script
      # Write the script_body to disk
      FileUtils.mkdir_p(File.dirname(path))
      File.write(path, script_body)
      FileUtils.chmod(0755, path)
    end

    def remove_script
      FileUtils.rm_rf(File.dirname(path))
    end

    def passwd
      @passwd ||= Etc.getpwnam(username)
    rescue ArgumentError
      # NOOP - The user can not be found, this is handled in valid?
    end
  end
end
