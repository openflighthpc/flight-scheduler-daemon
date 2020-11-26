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

RSpec.describe FlightScheduler::Job do
  let(:id) { SecureRandom.uuid }
  let(:env) { {} }
  let(:username) { Etc.getlogin }

  subject do
    described_class.new(id, env, username)
  end

  it { should be_valid }

  context 'with a nil id' do
    let(:id) { nil }

    it { should_not be_valid }
  end

  context 'with a hash the job_id' do
    let(:id) { {} }

    it { should_not be_valid }
  end

  context 'with a username as the env' do
    let(:env) { 'username' }

    it { should_not be_valid }
  end
end
