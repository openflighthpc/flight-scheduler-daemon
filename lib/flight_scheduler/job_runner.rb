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
require 'etc'

module FlightScheduler
  #
  # Run the given job script and save a reference to it in the job registry.
  #
  # Current limitations:
  #
  # * The job's standard and error output is not saved to disk.
  #
  JobRunner = Struct.new(:id, :envs, :script_body, :arguments, :username, :stdout, :stderr) do
    attr_accessor :child_pid, :task, :status

    extend Forwardable
    def_delegator :task, :wait

    def script_path
      FlightScheduler.app.config.spool_dir.join('state', id, 'job-script')
    end

    def passwd
      @passwd ||= Etc.getpwnam(username)
    rescue ArgumentError
      # NOOP - The user can not be found, this is handled in valid?
    end

    # Checks the various parameters are in the correct format before running
    # This is to prevent rogue data being passed Process.spawn or rm -f
    def valid?
      return false unless /\A[\w-]+\Z/.match? id
      return false unless envs.is_a? Hash
      return false unless arguments.is_a? Array
      return false unless passwd
      return false if stdout.to_s.empty?
      return false if stderr.to_s.empty?
      true
    end

    # Checks if the child process has exited correctly
    def success?
      return nil unless status
      status.success?
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

      # Ensures env's is a stringified hash
      string_envs = envs.map { |k, v| [k.to_s, v] }.to_h

      # Add the job to the registry
      path = script_path.to_path
      FlightScheduler.app.job_registry.add(id, self)


      self.task = Async do |task|
        # Fork to create the child process [Non Blocking]
        self.child_pid = Kernel.fork do
          # Become the requested user and session leader
          string_envs.merge!(
            'HOME' => passwd.dir,
            'LOGNAME' => username,
            'PATH' => '/bin:/sbin:/usr/bin:/usr/sbin',
            'USER' => username,
            'flight_ROOT' => ENV['flight_ROOT'],
          )
          Process::Sys.setgid(passwd.gid)
          Process::Sys.setuid(username)
          Process.setsid

          # Create the stdout/stderr directories
          stdout_path = File.expand_path(stdout, '~')
          stderr_path = File.expand_path(stderr, '~')
          FileUtils.mkdir_p File.dirname(stdout_path)
          FileUtils.mkdir_p File.dirname(stderr_path)

          # Write the script_body to disk
          FileUtils.mkdir_p File.dirname(path)
          File.write(path, script_body)
          FileUtils.chmod 0755, path

          # Build the options hash
          opts = { unsetenv_others: true }
          if stdout_path == stderr_path
            opts.merge!({ [:out, :err] => stdout_path })
          else
            opts.merge!(out: stdout_path, err: stderr_path)
          end

          Dir.chdir(passwd.dir)

          # Exec into the job command
          Kernel.exec(string_envs, path, *arguments, **opts)
        end

        # Loop asynchronously until the child is finished
        until out = Process.wait2(child_pid, Process::WNOHANG) do
          task.yield
        end
        self.status = out.last

        # Reset the child_pid, this prevents cancel killing other processes
        # which might spawn with the same PID
        self.child_pid = nil
      ensure
        FlightScheduler.app.job_registry.remove(id)
        FileUtils.rm_rf File.dirname(path)
      end
    end

    # Kills the associated subprocess
    def cancel
      return unless child_pid
      Kernel.kill('SIGTERM', self.child_pid)
    rescue Errno::ESRCH
      # NOOP - Don't worry if the process has already finished
    end
  end
end
