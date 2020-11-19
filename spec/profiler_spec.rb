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
require 'yaml'

RSpec.describe FlightScheduler::Profiler do
  shared_examples 'core profiler spec' do
    let(:xml)       { file_fixture("lshw/#{name}.xml").read }
    let(:metadata)  do
      # The metadata is used to configure the spec to the instance,
      # it is not part of the Profiler itself
      YAML.load(file_fixture("lshw/#{name}.metadata.yaml").read, symbolize_names: true)
    end
    subject         { described_class.new }

    before do
      allow(described_class).to receive(:run_lshw_xml).and_return(xml)
    end

    describe '#cpus' do
      it { expect(subject.cpus).to eq(metadata[:cpus]) }
    end

    describe '#gpus' do
      it { expect(subject.gpus).to eq(metadata[:gpus]) }
    end
  end

  context 'with a Standard_NC24s_v3 machine' do
    let(:name) { 'Standard_NC24s_v3' }

    include_examples 'core profiler spec'
  end
end
