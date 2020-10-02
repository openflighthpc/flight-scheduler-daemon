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
  class SubmissionScript

    attr_reader :job, :arguments

    def initialize(job, script_body, arguments, stdout_path, stderr_path)
      @job = job
      @script_body = script_body
      @arguments = arguments
      @stdout_path = stdout_path
      @stderr_path = stderr_path
    end

    def path
      spool_dir = FlightScheduler.app.config.spool_dir
      spool_dir.join('state', job.id, 'job-script').to_path
    end

    # Checks the various parameters are in the correct format before running
    # This is to prevent rogue data being passed Process.spawn or rm -f
    def valid?
      return false unless job
      return false unless @script_body.is_a? String
      return false if @script_body.empty?
      return false unless @script_body[0..1] == '#!'
      return false unless arguments.is_a? Array
      return false if stdout_path.to_s.empty?
      return false if stderr_path.to_s.empty?
      true
    end

    def stdout_path
      File.expand_path(@stdout_path, job.home_dir)
    end

    def stderr_path 
      File.expand_path(@stderr_path, job.home_dir)
    end

    def write
      # Write the script_body to disk
      FileUtils.mkdir_p(File.dirname(path))
      File.write(path, script_body)
      FileUtils.chmod(0755, path)
    end

    def remove
      FileUtils.rm_rf(File.dirname(path))
    end
  end
end
