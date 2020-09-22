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
require 'async/process'

module FlightScheduler
  #
  # Run the given job script and save a reference to it in the job registry.
  #
  # Current limitations:
  #
  # * The environment is not cleaned up before execution.
  # * The environment is not set according to the options given to the
  #   scheduler.
  # * The job's standard and error output is not saved to disk.
  #
  JobRunner = Struct.new(:id, :env, :script_body, :arguments) do
    attr_accessor :child, :task

    def self.script_dir
      File.expand_path('../../var/spool/state', __dir__)
    end

    # DEPRECATED: Use id
    def job_id
      id
    end

    # Checks the various parameters are in the correct format before running
    # This is to prevent rogue data being passed Process.spawn or rm -f
    def valid?
      return false unless /\A[\w-]+\Z/.match? job_id
      return false unless env.is_a? Hash
      true
    end

    # Run the given arguments in a subprocess and return an Async::Task.
    #
    # Invariants:
    #
    # * Blocks until the subprocess has been registered with the job registry.
    #
    # * Returns an Async::Task that can be `wait`ed on.  When the returned
    #   task has completed, the subprocess has completed and is no longer in
    #   the job registry.
    def run
      # Write the script_body to disk
      script_path = File.join(self.class.script_dir, job_id, 'job-script')
      FileUtils.mkdir_p File.dirname(script_path)
      File.write script_path, script_body
      FileUtils.chmod 0755, script_path

      # Run the script
      self.child = Async::Process::Child.new(env, script_path, *arguments, unsetenv_others: true)
      FlightScheduler.app.job_registry.add(id, self)
      self.task = Async do
        child.wait
      ensure
        FlightScheduler.app.job_registry.remove(job_id)
      end
    ensure
      FileUtils.rm_rf File.dirname(script_path) unless script_path.nil?
    end

    # Kills the subprocess associated with the given job id if one exists.
    #
    # Invariants:
    #
    # * Returns an Async::Task that can be `wait`ed on.  When the returned
    #   task has completed, the subprocess will have been sent a `TERM`
    #   signal.
    def cancel
      process = FlightScheduler.app.job_registry[job_id]
      if process && process.running?
        Async do
          process.kill
        end
      end
    end
  end
end
