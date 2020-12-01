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
  class Batchd
    def initialize(job, script)
      @job = job
      @script = script
    end

    def run
      child_pid = run_script
      status = wait_for_child(child_pid)
      notify_finished(status)
    end

    private

    def run_script
      FileUtils.mkdir_p(File.dirname(@script.stdout_path))
      FileUtils.mkdir_p(File.dirname(@script.stderr_path))
      opts = { unsetenv_others: true }
      if @script.stdout_path == @script.stderr_path
        opts.merge!({ [:out, :err] => @script.stdout_path })
      else
        opts.merge!(out: @script.stdout_path, err: @script.stderr_path)
      end

      Dir.chdir(@job.working_dir)
      Kernel.fork do
        Kernel.exec(@job.env, @script.path, *@script.arguments, **opts)
      end
    end

    def wait_for_child(child_pid)
      Async.logger.info("batchd: waiting on child_pid:#{child_pid}")
      _, status = Process.wait2(child_pid)
      Async.logger.debug("batchd: done waiting on child_pid:#{child_pid}")
      status
    end

    def notify_finished(status)
      command = status.success? ? 'NODE_COMPLETED_JOB' : 'NODE_FAILED_JOB'
      MessageSender.send(command: command, job_id: @job.id)
    end
  end
end
