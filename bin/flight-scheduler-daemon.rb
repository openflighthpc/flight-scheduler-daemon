#!/usr/bin/env ruby
#==============================================================================
# Copyright (C) 2020-present Alces Flight Ltd.
#
# This file is part of Flight Docs.
#
# This program and the accompanying materials are made available under
# the terms of the Eclipse Public License 2.0 which is available at
# <https://www.eclipse.org/legal/epl-2.0>, or alternative license
# terms made available by Alces Flight Ltd - please direct inquiries
# about licensing to licensing@alces-flight.com.
#
# Flight Docs is distributed in the hope that it will be useful, but
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, EITHER EXPRESS OR
# IMPLIED INCLUDING, WITHOUT LIMITATION, ANY WARRANTIES OR CONDITIONS
# OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY OR FITNESS FOR A
# PARTICULAR PURPOSE. See the Eclipse Public License 2.0 for more
# details.
#
# You should have received a copy of the Eclipse Public License 2.0
# along with Flight Docs. If not, see:
#
#  https://opensource.org/licenses/EPL-2.0
#
# For more information on Flight Docs, please visit:
# https://github.com/alces-flight/flight-docs
#==============================================================================
begin
  # lib_dir = File.expand_path(File.join(__FILE__, '../../lib'))
  # $LOAD_PATH << lib_dir
  ENV['BUNDLE_GEMFILE'] ||= File.join(__FILE__, '../../Gemfile')

  require 'rubygems'
  gem 'bundler', '2.1.4'
  require 'bundler'

  Bundler.setup(:default)

  require_relative '../config/boot'
  Dir.chdir(ENV.fetch('FLIGHT_CWD','.'))
  OpenFlight.set_standard_env rescue nil

  require 'daemons'
  Daemons.run_proc('flight-scheduler-daemon') do
    FlightScheduler.app.run
  end
rescue Interrupt
  if Kernel.const_defined?(:Paint)
    $stderr.puts "\n#{Paint['WARNING', :underline, :yellow]}: Cancelled by user"
  else
    $stderr.puts "\nWARNING: Cancelled by user"
  end
  exit(130)
end

