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

require 'spec_helper'
require 'securerandom'

RSpec.describe FlightScheduler::BatchScript do
  let(:job) {
    FlightScheduler::Job.new(SecureRandom.uuid, {}, Etc.getlogin)
  }
  let(:script_body) do
    <<~SCRIPT
      #!/bin/bash
      echo 'test'
    SCRIPT
  end
  let(:arguments) { [] }

  subject do
    described_class.new(
      job, script_body, arguments, '/tmp/foo', '/tmp/foo'
    )
  end

  it { should be_valid }

  context 'with a nil job' do
    let(:job) { nil }

    it { should_not be_valid }
  end

  context 'with a file path as the job_id' do
    let(:job) { '../../../../../../../root' }

    it { should_not be_valid }
  end

  context 'with a script as the job' do
    let(:job) { '/usr/sbin/shutdown' }

    it { should_not be_valid }
  end

  context 'with a string as arrguments' do
    let(:arguments) { 'adds-nice-handling-to-internal-errors' }

    it { should_not be_valid }
  end
end

