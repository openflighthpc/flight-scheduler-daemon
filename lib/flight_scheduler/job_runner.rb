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
require 'forwardable'

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

    extend Forwardable
    def_delegator :task, :wait

    def self.script_dir
      File.expand_path('../../var/spool/state', __dir__)
    end

    def build_script_path
      File.join(self.class.script_dir, id, 'job-script')
    end

    # Checks the various parameters are in the correct format before running
    # This is to prevent rogue data being passed Process.spawn or rm -f
    def valid?
      return false unless /\A[\w-]+\Z/.match? id
      return false unless env.is_a? Hash
      return false unless arguments.is_a? Array
      true
    end

    # Checks if the child process has exited correctly
    # Will return false if it didn't start in the first place
    def success?
      return false unless child
      child.success?
    end

    # Run the given arguments in a subprocess and return an Async::Task.
    #
    # Invariants:
    #
    # * Blocks until the job has been validated and recorded in the registry
    #
    # * Returns an Async::Task that can be `wait`ed on.  When the returned
    #   task has completed, the subprocess has completed and is no longer in
    #   the job registry.
    def run
      raise JobValidationError, <<~ERROR.chomp unless valid?
        An unexpected error has occurred! The job does not appear to be in a valid state.
      ERROR

      # Add the job to the registry
      path = build_script_path
      FlightScheduler.app.job_registry.add(id, self)

      self.task = Async do
        # Write the script_body to disk
        FileUtils.mkdir_p File.dirname(path)
        File.write path, script_body
        FileUtils.chmod 0755, path

        # Starts the child process
        self.child = Async::Process::Child.new(env, path, *arguments, unsetenv_others: true)
        child.wait
      ensure
        FlightScheduler.app.job_registry.remove(id)
        FileUtils.rm_rf File.dirname(path)
      end
    end

    # Kills the subprocess associated with the given job id if one exists.
    #
    # Invariants:
    #
    # * Returns an Async::Task that can be `wait`ed on.  When the returned
    #   task has completed, the subprocess will have been sent a `TERM`
    #   signal.
    def cancel
      process = FlightScheduler.app.job_registry[id]
      if process && process.running?
        Async do
          process.kill
        end
      end
    end
  end
end
