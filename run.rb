#!/usr/bin/ruby

require 'nokogiri'
require 'net/ftp'
require 'time'

require_relative 'options'
require_relative 'database'
require_relative 'commands'
require_relative 'ftp_get'


puts "Auto Election Upload for #{$elec}"
#puts "Checking for RUN in download table"

## TODO insert a check if we should do the process

create_syncs_table				#only creates if dones't exist

if $run_method == 'setup'
	$file_type = 'preload'
end

# newf = reuse_file
newf = download_file

if !newf
	puts "Nothing to download"
else
	if $run_method == 'setup'
		process_booths
		process_candidates
		process_districts
		process_parties
		create_results_table	#only creates if doesn't exist
		create_votetypes_table	#only creates if doesn't exist
		create_views			#drops view and recreates
	elsif $run_method == 'results'
		process_house
		process_house_candidates
		# process_candidate_elected
		# process_candidate_order
		process_house_tcp
		two_party_preferred
		# process_senate
	end
end