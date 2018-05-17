#!/usr/bin/ruby

require 'nokogiri'
require 'net/ftp'
require 'time'

require_relative 'options'
require_relative 'database'
require_relative 'commands'
require_relative 'ftp_get'

# Add argument of setup when calling this program and it will setup the database
run_method = ARGV[0]

puts "Auto Election Upload for #{$elec}"
if run_method == 'setup'
	puts "Entering Setup mode"
else
	run_method = 'results'
end

#puts "Checking for RUN in download table"

## TODO insert a check if we should do the process

create_syncs_table				#only creates if dones't exist

if run_method == 'setup'
	$file_type = 'preload'
else
	$file_type = 'verbose'
end

newf = reuse_file
# newf = download_file

if !newf
	puts "Nothing to download"
else
	if run_method == 'setup'
		process_booths
		process_candidates
		process_districts
		process_parties
		create_results_table	#only creates if doesn't exist
		create_votetypes_table	#only creates if doesn't exist
		create_views			#drops view and recreates
	elsif run_method == 'results'
		process_house
		process_house_candidates
		process_candidate_elected
		process_candidate_order
		process_house_tcp
		two_party_preferred
		# process_senate
	end
end