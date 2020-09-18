#!/usr/bin/env ruby
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
# https://github.com/alces-flight/flight-scheduler-daemon
#==============================================================================
begin
  ENV['BUNDLE_GEMFILE'] ||= File.join(__FILE__, '../../Gemfile')

  require 'rubygems'
  gem 'bundler', '2.1.4'
  require 'bundler'

  Bundler.setup(:default)

  require_relative '../config/boot'
  Dir.chdir(ENV.fetch('FLIGHT_CWD','.'))
  OpenFlight.set_standard_env rescue nil

  require 'daemons'
  options = {
    # Passing ARGV allows the below options to be overridden on the command
    # line.
    ARGV: ARGV,

    # The below settings are suitable for development a production
    # installation would likely want to override them.

    # The relative path to the directory to store the pid file.  The path is
    # relative to the current working directory.  An absolute path can be
    # given instead.
    dir: ENV.fetch('PID_DIR', 'tmp'),
    dir_mode: :normal,

    # The absolute path to the directory to store the logs.  Note, relative
    # paths do not work here.
    log_dir: File.expand_path(File.join(__FILE__, '../../log')),

    # Redirect stdout and stderr to the file given by `output_logfilename`.
    log_output: true,

    # The name of the log file.  It will be created in the directory given by
    # `log_dir`.
    output_logfilename: 'flight-scheduler-daemon.log',
  }
  Daemons.run_proc('flight-scheduler-daemon', **options) do
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

