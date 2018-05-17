# require 'net/http'
# 

def sync_number(database,elec)
	##A table is kept to record each sync
	##This function simply finds out what number we are up to so that the ZIP and XML file
	## can be saved with the appropriate number
	sql = "select count(*) as syncs from #{elec}_syncs"
	numberss = SqlDatabase.runQuery("#{database}",sql)
	numberr = 1
	numberss.each do | number |
		numberr = number['syncs'] + 1
	end
	log_report(1,"sync numbers is #{numberr}")
	return numberr
end

def download_file
	##Downlaod the XML zip file from location specificed in personal_options
	log_report(1,'Starting the download process. First checking website.')

	aec_ftp = AECFtp.new

	syncnumber = sync_number($database,$elec)

	newfile =  aec_ftp.download_file_if_newer   ### returns false if no newer file, otherwise returns the local path of the new file

	if newfile != false
		newfile.sub! ' (Queensland Labor)',''
		newfile.sub! '/raw',''

		`unzip -o #{newfile} -d raw`
		`mv #{newfile} raw`
	end

	return newfile
end

def sql_upload(sql)
	#If you want to upload data to multiple MySQL servers, replicatate the below line for each server
	#You will also need to add a Class for each server in the database.rb
	SqlDatabase.runQuery($database,sql)
end


def log_report(level,message)
	#Really simple function to enable log reporting
	#Adjust the log level in personal_options.rb and program will print any logs less than your number
	if level.to_i <= $report_level.to_i
		puts message
	end
end

def load_XML(xmlfile)
	@xmlfile = xmlfile
	log_report(5,"Connecting the XML file @ #{@xmlfile}")


	@doc = Nokogiri::XML(File.open(@xmlfile)) do | config |
		config.noblanks
	end
	@doc.remove_namespaces!

	if $run_method == 'results'
		updated = @doc.at_xpath("//MediaFeed")['Created']
		sql_upload("insert into " + $elec + "_syncs (updated) values (\'#{updated}\');")
	end
	return @doc
end

def reuse_file
	##This function is used to connect an existing XML file
	##during testing, rather than download new ones
	@xmlfile = $location + "/raw/xml/aec-mediafeed-results-detailed-#{$file_type}-#{$elec_id}.xml"
	log_report(5,"file at #{@xmlfile}")

	@doc = Nokogiri::XML(File.open(@xmlfile)) do | config |
		config.noblanks
	end
	@doc.remove_namespaces!

	updated = @doc.at_xpath("//MediaFeed")['Created']
	return @doc
end

def create_syncs_table
	log_report(9,"Checking if Sync Table Exists")
	tbl_check = "select * from #{$elec}_syncs;"
	begin
		sql_upload(tbl_check).first
	rescue
		log_report(9,"Sync Table doesn't exist. Creating one now")
		sql = "CREATE TABLE #{$elec}_syncs (`sync` int(11) NOT NULL AUTO_INCREMENT, `updated` datetime DEFAULT NULL, `timestamp` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY (`sync`)) AUTO_INCREMENT=1;"
		sql_upload(sql)
	end
end

def create_views
	puts "Let's create some views."
	sql = "drop view if exists vw_#{$elec}_results; CREATE VIEW `vw_#{$elec}_results` AS SELECT `s`.`#{$area}_id` AS #{$area}_id, `s`.#{$area}_name AS `#{$area}`, `b`.`pollingplace_id` AS `booth_id`, `b`.`pollingplace_name` AS `booth_name`, `c`.`ballot_position` AS `ballot_position`, `c`.`candidate_name` AS `candidate_name`, `p`.`short_name` AS `party`, `r`.`type` AS `vote_type`, `vt`.`description` AS `description`, `r`.`votes` AS `votes`, `r`.`result_id` AS `result_id`, `r`.`area_id` AS `area_id`, `r`.`entity_id` AS `entity_id` from #{$elec}_results r inner join #{$elec}_vote_types vt ON r.type = vt.vote_id inner join #{$elec}_booths b ON r.area_id = b.pollingplace_id and vt.level = 'pp' inner join #{$elec}_candidates c ON r.entity_id = c.candidate_id left join #{$elec}_parties p ON c.affiliation_code = p.affiliation_id inner join #{$elec}_#{$area}s s ON c.#{$area}_id = s.#{$area}_id union SELECT `s`.#{$area}_id AS #{$area}_id, `s`.#{$area}_name AS `#{$area}`, null AS `booth_id`, null AS `booth_name`, `c`.`ballot_position` AS `ballot_position`, `c`.`candidate_name` AS `candidate_name`, `p`.`short_name` AS `party`, `r`.`type` AS `vote_type`, `vt`.`description` AS `description`, `r`.`votes` AS `votes`, `r`.`result_id` AS `result_id`, `r`.`area_id` AS `area_id`, `r`.`entity_id` AS `entity_id` from #{$elec}_results r inner join #{$elec}_vote_types vt ON r.type = vt.vote_id and vt.level = '#{$area}' inner join #{$elec}_candidates c ON r.entity_id = c.candidate_id left join #{$elec}_parties p ON c.affiliation_code = p.affiliation_id inner join #{$elec}_#{$area}s s ON c.#{$area}_id = s.#{$area}_id;"
	sql_upload(sql)
	sql = "drop view if exists vw_#{$elec}_results_div_fp; create view vw_#{$elec}_results_div_fp as select #{$area}_state as stateab, #{$area}_id as divisionid, #{$area}_name as divisionnm, candidate_id as candidateid, last_name as surname, first_ballot_name as givennm, ballot_position as ballotposition, elected, historicelected, partyab, party_name as partynm, ordinaryvotes, absentvotes, provisionalvotes, prepollvotes, postalvotes, totalvotes, swing from( SELECT s.#{$area}_state, s.#{$area}_id, s.#{$area}_name, c.candidate_id, c.last_name, c.first_ballot_name, c.ballot_position, c.elected, c.historicelected, if(c.affiliation_code is null, 'IND',p.short_name) as partyab, if(p.party_name is null, 'Independent',p.party_name) as party_name, sum(if(r.type = 9,votes,0)) as ordinaryvotes, sum(if(r.type = 13,votes,0)) as absentvotes, sum(if(r.type = 17,votes,0)) as provisionalvotes, sum(if(r.type = 21,votes,0)) as prepollvotes, sum(if(r.type = 25,votes,0)) as postalvotes, sum(if(r.type = 29,votes,0)) as totalvotes, sum(if(r.type = 32,votes,0)) / 100 as swing from #{$elec}_results r inner join #{$elec}_#{$area}s s ON r.area_id = s.#{$area}_id inner join #{$elec}_candidates c ON r.entity_id = c.candidate_id left join #{$elec}_parties p ON c.affiliation_code = p.affiliation_id where r.type in (9,13,17,21,25,29,32) group by s.#{$area}_id, c.candidate_id union SELECT s.#{$area}_state, s.#{$area}_id, s.#{$area}_name, 99991, 'Informal', 'Informal', 999, 'N', 'N', null as partyab, 'Informal' as party_name, sum(if(r.type = 9,votes,0)) as ordinaryvotes, sum(if(r.type = 13,votes,0)) as absentvotes, sum(if(r.type = 17,votes,0)) as provisionalvotes, sum(if(r.type = 21,votes,0)) as prepollvotes, sum(if(r.type = 25,votes,0)) as postalvotes, sum(if(r.type = 29,votes,0)) as totalvotes, sum(if(r.type = 32,votes,0)) / 100 as swing from #{$elec}_results r inner join #{$elec}_#{$area}s s ON r.area_id = s.#{$area}_id where r.type in (9,13,17,21,25,29,32) and entity_id = 99991 group by s.#{$area}_id) as x order by #{$area}_id, ballot_position;"
	sql_upload(sql)
	sql = "drop view if exists vw_#{$elec}_results_div_tcp; create view vw_#{$elec}_results_div_tcp as select #{$area}_state as stateab, #{$area}_id as divisionid, #{$area}_name as divisionnm, candidate_id as candidateid, last_name as surname, first_ballot_name as givennm, ballot_position as ballotposition, elected, historicelected, partyab, party_name as partynm, ordinaryvotes, absentvotes, provisionalvotes, prepollvotes, postalvotes, totalvotes, swing from( SELECT s.#{$area}_state, s.#{$area}_id, s.#{$area}_name, c.candidate_id, c.last_name, c.first_ballot_name, c.ballot_position, c.elected, c.historicelected, if(c.affiliation_code is null, 'IND',p.short_name) as partyab, if(p.party_name is null, 'Independent',p.party_name) as party_name, sum(if(r.type = 39,votes,0)) as ordinaryvotes, sum(if(r.type = 43,votes,0)) as absentvotes, sum(if(r.type = 47,votes,0)) as provisionalvotes, sum(if(r.type = 51,votes,0)) as prepollvotes, sum(if(r.type = 55,votes,0)) as postalvotes, sum(if(r.type = 59,votes,0)) as totalvotes, sum(if(r.type = 62,votes,0)) / 100 as swing from #{$elec}_results r inner join #{$elec}_#{$area}s s ON r.area_id = s.#{$area}_id inner join #{$elec}_candidates c ON r.entity_id = c.candidate_id left join #{$elec}_parties p ON c.affiliation_code = p.affiliation_id where r.type in (39,43,47,51,55,59,62) group by s.#{$area}_id, c.candidate_id union SELECT s.#{$area}_state, s.#{$area}_id, s.#{$area}_name, 99991, 'Informal', 'Informal', 999, 'N', 'N', null as partyab, 'Informal' as party_name, sum(if(r.type = 39,votes,0)) as ordinaryvotes, sum(if(r.type = 43,votes,0)) as absentvotes, sum(if(r.type = 47,votes,0)) as provisionalvotes, sum(if(r.type = 51,votes,0)) as prepollvotes, sum(if(r.type = 55,votes,0)) as postalvotes, sum(if(r.type = 59,votes,0)) as totalvotes, sum(if(r.type = 62,votes,0)) / 100 as swing from #{$elec}_results r inner join #{$elec}_#{$area}s s ON r.area_id = s.#{$area}_id where r.type in (39,43,47,51,55,59,62) and entity_id = 99991 group by s.#{$area}_id) as x order by #{$area}_id, ballot_position;"
	sql_upload(sql)	
	sql = "drop view if exists vw_#{$elec}_results_pp_fp; create view vw_#{$elec}_results_pp_fp as select #{$area}_state as stateab, #{$area}_id as divisionid, #{$area}_name as divisionnm, pollingplace_id as pollingplaceid, pollingplace_name as pollingplace, candidate_id as candidateid, last_name as surname, first_ballot_name as givennm, ballot_position as ballotposition, elected, historicelected, partyab, party_name as partynm, ordinaryvotes, swing from( SELECT s.#{$area}_state, s.#{$area}_id, s.#{$area}_name, b.pollingplace_id, b.pollingplace_name, c.candidate_id, c.last_name, c.first_ballot_name, c.ballot_position, c.elected, c.historicelected, if(c.affiliation_code is null, 'IND',p.short_name) as partyab, if(p.party_name is null, 'Independent',p.party_name) as party_name, sum(if(r.type = 1,votes,0)) as ordinaryvotes, sum(if(r.type = 4,votes,0)) / 100 as swing from #{$elec}_results r inner join #{$elec}_booths b ON r.area_id = b.pollingplace_id inner join #{$elec}_candidates c ON r.entity_id = c.candidate_id inner join #{$elec}_#{$area}s s ON c.#{$area}_id = s.#{$area}_id left join #{$elec}_parties p ON c.affiliation_code = p.affiliation_id where r.type in (1,4) group by s.#{$area}_id, b.pollingplace_id, c.candidate_id union SELECT s.#{$area}_state, s.#{$area}_id, s.#{$area}_name, b.pollingplace_id, b.pollingplace_name, 99991, 'Informal', 'Informal', 999, 'N', 'N', null as partyab, 'Informal' as party_name, sum(if(r.type = 1,votes,0)) as ordinaryvotes, sum(if(r.type = 4,votes,0)) / 100 as swing from #{$elec}_results r inner join #{$elec}_booths b ON r.area_id = b.pollingplace_id inner join #{$elec}_#{$area}s s ON b.#{$area}_id = s.#{$area}_id where r.type in (1,4) and entity_id = 99991 group by s.#{$area}_id, b.pollingplace_id) as x order by #{$area}_id, pollingplace_id, ballot_position;"
	sql_upload(sql)
	sql = "drop view if exists vw_#{$elec}_results_pp_tcp; create view vw_#{$elec}_results_pp_tcp as select #{$area}_id, #{$area}, booth_id, booth_name, ballot_position, candidate_name, party, sum(if(vote_type = 5,votes,0)) as ordinary, sum(if(vote_type = 8,votes,0)) / 100 as swing from vw_#{$elec}_results where vote_type in(5,8) group by #{$area}_id, booth_id, ballot_position order by #{$area}_id, booth_name, ballot_position"
	sql_upload(sql)
	puts "Views complete!"
end

def process_booths

	load_XML("raw/xml/aec-mediafeed-pollingdistricts-#{$elec_id}.xml")

	puts "Processing the booths."

	##create a booth table
	sql = "drop table if exists #{$elec}_booths; CREATE TABLE `#{$elec}_booths` (  `#{$area}_id` int(11) NOT NULL,  `physical_address` int(11) DEFAULT NULL,  `address_details` varchar(255) DEFAULT NULL,  `postal_service` varchar(255) DEFAULT NULL,  `latitude` decimal(15,8) DEFAULT NULL,  `longitude` decimal(15,8) DEFAULT NULL,  `premises` varchar(255) DEFAULT NULL,  `address_one` varchar(255) DEFAULT NULL,  `address_two` varchar(255) DEFAULT NULL,  `address_suburb` varchar(255) DEFAULT NULL,  `address_state` varchar(255) DEFAULT NULL,  `address_postcode` int(5) DEFAULT NULL,  `pollingplace_id` int(11) NOT NULL,  `pollingplace_name` varchar(255) DEFAULT NULL,  `pollingplace_type` varchar(255) DEFAULT NULL,  PRIMARY KEY (pollingplace_id),  KEY idx_#{$area}_id (#{$area}_id));"
	sql << "TRUNCATE #{$elec}_booths; INSERT INTO #{$elec}_booths (#{$area}_id,physical_address,address_details,postal_service,latitude,longitude,premises,address_one,address_two,address_suburb,address_state,address_postcode,pollingplace_id,pollingplace_name,pollingplace_type) VALUES "

	pollingdistrict = @doc.xpath(".//PollingDistrict")

	pollingdistrict.each do | district |
		districtid = district.at_xpath(".//PollingDistrictIdentifier")

		polplaces = district.at_xpath("./PollingPlaces")

		polplaces.children.each do | polplace |

			physaddress = polplace.at_xpath("./PhysicalLocation")

			if physaddress.nil?
				next
			end

			address = physaddress.at_xpath("./Address")
			postserviceel = address.at_xpath("./PostalServiceElements")
			addlat = postserviceel.at_xpath("./AddressLatitude").text
			addlong = postserviceel.at_xpath("./AddressLongitude").text
			addall = address.at_xpath("./AddressLines")
			polplaceid = polplace.at_xpath("./PollingPlaceIdentifier")

			if addlong.empty?
				longlong = 'NULL'
			else
				longlong = addlong
			end

			if addlat.empty?
				latlat = 'NULL'
			else
				latlat = addlat
			end

			# Variables
			add_premises = ''
			add_add1 = ''
			add_add2 = ''
			add_suburb = ''
			add_state = ''
			add_postcode = 'NULL'

			addall.children.each do | addal |

				if addal['Type'] == 'Premises'
					add_premises = addal.text
					next
				end

				if addal['Type'] == 'AddressLine1'
					add_add1 = addal.text
					next
				end

				if addal['Type'] == 'AddressLine2'
					add_add2 = addal.text
					next
				end

				if addal['Type'] == 'Suburb'
					add_suburb = addal.text
					next
				end

				if addal['Type'] == 'State'
					add_state = addal.text
					next
				end

				if addal['Type'] == 'Postcode'
					add_postcode = addal.text
					next
				end
			end

			if polplaceid['Classification'].nil?
				pollace_type = ''
			else
				pollace_type = polplaceid['Classification']
			end

			#puts districtid['Id'] +',' + physaddress['Id'] + ',' + address['AddressDetailsKey'] + ',' + postserviceel['Type'] + ',' + addlat.text + ',' + addlong.text + ',' + add_premises + ',' + add_add1 + ',' + add_add2 + ',' + add_suburb + ',' + add_state + ',' + add_postcode + ',' + polplaceid['Id'] + ',' + polplaceid['Name'] + ',' + pollace_type
			sql << "(#{districtid['Id']},#{physaddress['Id']},\"#{address['AddressDetailsKey']}\",\"#{postserviceel['Type']}\",#{latlat},#{longlong},\"#{add_premises}\",\"#{add_add1}\",\"#{add_add2}\",\"#{add_suburb}\",\"#{add_state}\",#{add_postcode},#{polplaceid['Id']},\"#{polplaceid['Name']}\",\"#{pollace_type}\"),"
		end
	end
	sql  = sql[0..-2]	#chop off the last comma
	sql << ";"
	sql_upload(sql)
	puts "Booths complete."
end

def process_candidates

	load_XML("raw/xml/eml-230-candidates-#{$elec_id}.xml")

	puts "Processing the candidates."

	sql = "drop table if exists #{$elec}_candidates; CREATE TABLE `#{$elec}_candidates` (`#{$area}_id` varchar(11) DEFAULT NULL,  `candidate_id` int(11) NOT NULL,  `candidate_name` varchar(255) DEFAULT NULL,  `first_ballot_name` varchar(255) DEFAULT NULL,  `first_name` varchar(255) DEFAULT NULL,  `last_name` varchar(255) DEFAULT NULL,  `gender` varchar(255) DEFAULT NULL,  `address_line_1` varchar(255) DEFAULT NULL,  `address_line_2` varchar(255) DEFAULT NULL,  `address_suburb` varchar(255) DEFAULT NULL,  `address_state` varchar(255) DEFAULT NULL,  `address_code` int(255) DEFAULT NULL,  `postal_line_1` varchar(255) DEFAULT NULL,  `postal_line_2` varchar(255) DEFAULT NULL,  `postal_suburb` varchar(255) DEFAULT NULL,  `postal_state` varchar(255) DEFAULT NULL,  `postal_code` int(255) DEFAULT NULL,  `email` varchar(255) DEFAULT NULL,  `telephone` varchar(255) DEFAULT NULL,  `fax` varchar(255) DEFAULT NULL,  `mobile` varchar(255) DEFAULT NULL,  `affiliation_code` int(11) DEFAULT NULL,  `profession` varchar(255) DEFAULT NULL,  `ballot_position` int(2) DEFAULT NULL, elected varchar(1), historicelected varchar(1),  PRIMARY KEY (`candidate_id`),  KEY `idx_affil` (`affiliation_code`));"
	sql << "TRUNCATE #{$elec}_candidates; INSERT INTO #{$elec}_candidates (#{$area}_id,candidate_id,candidate_name,first_ballot_name,first_name,last_name,gender,address_line_1,address_line_2,address_suburb,address_state,address_code,postal_line_1,postal_line_2,postal_suburb,postal_state,postal_code,email,telephone,fax,mobile,affiliation_code,profession) VALUES "

	contest = @doc.xpath(".//Contest")

	contest.each do | cont |
		contestid = cont.at_xpath("./ContestIdentifier")
		#puts 'Contest ' + contestid['Id']

		cont.children.each do | ddd |
			if ddd['Independent'].nil?
				next
			end
			canid = ddd.at_xpath(".//CandidateIdentifier")
			canname = canid.at_xpath(".//CandidateName")
			canfullname = ddd.at_xpath(".//CandidateFullName")
			persname = canfullname.at_xpath(".//PersonName")

			#puts canname.text

			canfirst_ballot = persname.at_xpath(".//FirstName[@Type='BallotPaper']")
			canfirst_both = persname.at_xpath(".//FirstName")
			canlast = persname.at_xpath(".//LastName")

			cangender = ddd.at_xpath(".//Gender")

			res_address = ddd.at_xpath(".//QualifyingAddress[@AddressType='Residential']")

			if res_address.nil?
				ress_add_lin1 = ''
				ress_add_lin2 = ''
				ress_add_sub = ''
				ress_add_sta = ''
				ress_add_cod = 'NULL'
			else
				ress_add_lin1 = res_address.at_xpath(".//AddressLine[@Type='AddressLine1']")
				ress_add_lin2 = res_address.at_xpath(".//AddressLine[@Type='AddressLine2']")
				ress_add_sub = res_address.at_xpath(".//AddressLine[@Type='Suburb']").text
				ress_add_sta = res_address.at_xpath(".//AddressLine[@Type='State']").text
				ress_add_cod = res_address.at_xpath(".//AddressLine[@Type='Postcode']").text
					ress_add_cod = '"' + ress_add_cod + '"'

				if ress_add_lin1.nil?
					ress_add_lin1 = ''
				else
					ress_add_lin1 = ress_add_lin1.text.gsub("\"", "")
				end

				if ress_add_lin2.nil?
					ress_add_lin2 = ''
				else
					ress_add_lin2 = ress_add_lin2.text.gsub("\"", "")
				end
			end

			post_address = ddd.at_xpath(".//MailingAddress[@AddressType='Postal']")

			if post_address.nil?

				post_add_lin1 = ''
				post_add_lin2 = ''
				post_add_sub = ''
				post_add_sta = ''
				post_add_cod = 'NULL'
			else
				post_add_lin1 = post_address.at_xpath(".//AddressLine[@Type='AddressLine1']")
				post_add_lin2 = post_address.at_xpath(".//AddressLine[@Type='AddressLine2']")
				post_add_sub = post_address.at_xpath(".//AddressLine[@Type='Suburb']").text
				post_add_sta = post_address.at_xpath(".//AddressLine[@Type='State']").text
				post_add_cod = post_address.at_xpath(".//AddressLine[@Type='Postcode']").text

				if post_add_lin1.nil?
					post_add_lin1 = ''
				else
					post_add_lin1 = post_add_lin1.text.gsub("\"", "")
				end

				if post_add_lin2.nil?
					post_add_lin2 = ''
				else
					post_add_lin2 = post_add_lin2.text.gsub("\"", "")
				end
			end



			email = ddd.at_xpath(".//Email")
			if email.nil?
				email = ''
			else
				email = email.text
			end

			mobile = ddd.at_xpath(".//Telephone[@Mobile='yes']/Number")
			
			if mobile.nil?
				mobile = ''
			else
				mobile = mobile.text
			end

			telephone = ddd.at_xpath(".//Telephone[not(@Mobile)]/Number")

			if telephone.nil?
				telephone = ''
			else
				telephone = telephone.text
			end

			fax = ddd.at_xpath(".//Fax/Number")

			if fax.nil?
				fax = ''
			else
				fax = fax.text
			end

			if ddd['Independent'] == 'no'
				affil = ddd.at_xpath(".//Affiliation")
				
				if affil.nil?
	             	affil_id = 'NULL'
	            else
	            	affil_id = ddd.at_xpath(".//AffiliationIdentifier")['Id']
	            end
			else
				affil_id = 'NULL'
			end

			proffesion = ddd.at_xpath(".//Profession").text

			sql << "(\"#{contestid['Id']}\",#{canid['Id']},\"#{canname.text}\",\"#{canfirst_ballot.text}\",\"#{canfirst_both.text}\",\"#{canlast.text}\",\"#{cangender.text}\",\"#{ress_add_lin1}\",\"#{ress_add_lin2}\",\"#{ress_add_sub}\",\"#{ress_add_sta}\",#{ress_add_cod},\"#{post_add_lin1}\",\"#{post_add_lin2}\",\"#{post_add_sub}\",\"#{post_add_sta}\",#{post_add_cod},\"#{email}\",\"#{telephone}\",\"#{fax}\",\"#{mobile}\",#{affil_id},\"#{proffesion}\"),"
		end
		#break
	end
	sql = sql[0..-2]	#chop off the last comma
	sql << ";"
	sql_upload(sql)
	puts "Candidates complete."

end

def process_candidate_order
	load_XML("raw/xml/aec-mediafeed-results-detailed-verbose-#{$elec_id}.xml")

	puts "Processing the ballot order of candidates."

	sql = "drop table if exists tmp_#{$elec}_ballot_order; create temporary table tmp_#{$elec}_ballot_order (`#{$area}_id` varchar(11) DEFAULT NULL,  `candidate_id` int(11) NOT NULL, ballot_position int not null);"
	sql << "insert into tmp_#{$elec}_ballot_order (`#{$area}_id`, `candidate_id`, ballot_position) values"

	contests = @doc.at_xpath(".//House/Contests")

	contests.children.each do | contest |
		contestid = contest.at_xpath("./ContestIdentifier")['Id']

		candidate = contest.xpath("./FirstPreferences/Candidate")
		candidate.each do | cand |

			candid  = cand.at_xpath("./CandidateIdentifier")['Id']
			canorder = cand.at_xpath("./BallotPosition").text

			sql << "(#{contestid},#{candid},#{canorder}),"

		end
	end

	contests = @doc.at_xpath(".//Senate/Contests")

	unless contests.nil?

		contests.children.each do | contest |
			contestid = contest.at_xpath("./ContestIdentifier")['Id']

			candidate = contest.xpath("./FirstPreferences//Candidate")
			candidate.each do | cand |

				candid  = cand.at_xpath("./CandidateIdentifier")['Id']
				canorder = cand.at_xpath("./BallotPosition").text

				sql << "(\'#{contestid}\',#{candid},#{canorder}),"
			end

			ugcandidate = contest.xpath("./FirstPreferences//UngroupedCandidate")
			ugcandidate.each do | cand |

				candid  = cand.at_xpath("./CandidateIdentifier")['Id']
				canorder = cand.at_xpath("./BallotPosition").text

				sql << "(\'#{contestid}\',#{candid},#{canorder}),"
			end
		end
	end

	sql = sql[0..-2]
	sql << "; update #{$elec}_candidates c, tmp_#{$elec}_ballot_order b set c.ballot_position = b.ballot_position where c.#{$area}_id = b.#{$area}_id and c.candidate_id = b.candidate_id;"
	log_report(2,'Creating Ballot Position table')
	sql_upload(sql)
	log_report(1,'Uploaded Ballot Position Table')

end

def process_candidate_elected
	load_XML("raw/xml/aec-mediafeed-results-detailed-verbose-#{$elec_id}.xml")

	puts "Processing the election of candidates."

	sql = "drop table if exists tmp_#{$elec}_candidate_elected; create temporary table tmp_#{$elec}_candidate_elected (`#{$area}_id` varchar(11) DEFAULT NULL,  `candidate_id` int(11) NOT NULL, elected varchar(1), historicelected varchar(1));"
	sql << "insert into tmp_#{$elec}_candidate_elected (`#{$area}_id`, `candidate_id`, elected, historicelected) values"

	contests = @doc.at_xpath(".//House/Contests")

	contests.children.each do | contest |
		contestid = contest.at_xpath("./ContestIdentifier")['Id']

		candidate = contest.xpath("./FirstPreferences/Candidate")
		candidate.each do | cand |

			candid  = cand.at_xpath("./CandidateIdentifier")['Id']
			canelec = cand.at_xpath("./Elected").text
			canhiselec = cand.at_xpath("./Elected")['Historic']

			if canelec == 'true'
				canelec = 'Y'
			else
				canelec = 'N'
			end

			if canhiselec == 'true'
				canhiselec = 'Y'
			else
				canhiselec = 'N'
			end

			sql << "(#{contestid},#{candid},\'#{canelec}\',\'#{canhiselec}\'),"

		end
	end

	contests = @doc.at_xpath(".//Senate/Contests")

	unless contests.nil?

		contests.children.each do | contest |
			contestid = contest.at_xpath("./ContestIdentifier")['Id']

			candidate = contest.xpath("./FirstPreferences//Candidate")
			candidate.each do | cand |

				candid  = cand.at_xpath("./CandidateIdentifier")['Id']
				canelec = cand.at_xpath("./Elected").text
				canhiselec = cand.at_xpath("./Elected")['Historic']

				if canelec == 'true'
					canelec = 'Y'
				else
					canelec = 'N'
				end

				if canhiselec == 'true'
					canhiselec = 'Y'
				else
					canhiselec = 'N'
				end

				sql << "(\'#{contestid}\',#{candid},\'#{canelec}\',\'#{canhiselec}\'),"
			end

			ugcandidate = contest.xpath("./FirstPreferences//UngroupedCandidate")
			ugcandidate.each do | cand |

				candid  = cand.at_xpath("./CandidateIdentifier")['Id']
				canelec = cand.at_xpath("./Elected").text
				canhiselec = cand.at_xpath("./Elected")['Historic']

				if canelec == 'true'
					canelec = 'Y'
				else
					canelec = 'N'
				end

				if canhiselec == 'true'
					canhiselec = 'Y'
				else
					canhiselec = 'N'
				end			

				sql << "(\'#{contestid}\',#{candid},\'#{canelec}\',\'#{canhiselec}\'),"
			end
		end	
	end

	sql = sql[0..-2]
	sql << "; update #{$elec}_candidates c, tmp_#{$elec}_candidate_elected b set c.elected = b.elected, c.historicelected = b.historicelected  where c.#{$area}_id = b.#{$area}_id and c.candidate_id = b.candidate_id;"
	log_report(2,'Creating Elected table')
	sql_upload(sql)
	log_report(1,'Uploaded Elected Table')

end 


def process_districts
	load_XML("raw/xml/aec-mediafeed-pollingdistricts-#{$elec_id}.xml")

	puts "Processing the #{$area}s."

	sql = "drop table if exists #{$elec}_#{$area}s; CREATE TABLE #{$elec}_#{$area}s (`#{$area}_id` int(11) NOT NULL,  `#{$area}_shortcode` varchar(255) DEFAULT NULL,  `#{$area}_name` varchar(255) DEFAULT NULL,  `#{$area}_state` varchar(255) DEFAULT NULL,  `#{$area}_history` varchar(255) DEFAULT NULL,  `#{$area}_industry` varchar(255) DEFAULT NULL,  `#{$area}_location` varchar(255) DEFAULT NULL,  `#{$area}_demographic` varchar(255) DEFAULT NULL,  `#{$area}_area` int(11) DEFAULT NULL,  PRIMARY KEY (`#{$area}_id`),  KEY `idx_#{$area}_name` (`#{$area}_name`),  KEY `idx_#{$area}_state` (`#{$area}_state`));"
	sql << "TRUNCATE #{$elec}_#{$area}s; INSERT INTO #{$elec}_#{$area}s (#{$area}_id,#{$area}_shortcode,#{$area}_name,#{$area}_state,#{$area}_history,#{$area}_industry,#{$area}_location,#{$area}_demographic,#{$area}_area) VALUES "

	pollingdistrict = @doc.xpath(".//PollingDistrict")
	pollingdistrict.each do | district |
		districtid = district.at_xpath(".//PollingDistrictIdentifier")
		disname = districtid.at_xpath(".//Name")
		disstate = districtid.at_xpath(".//StateIdentifier")
		disnamehistory = district.at_xpath(".//NameDerivation")
		disproducts = district.at_xpath(".//ProductsIndustry")
		dislocation = district.at_xpath(".//Location")
		disDemo = district.at_xpath(".//Demographic")
		disArea = district.at_xpath(".//Area")

		#puts districtid['Id'] +',' + districtid['ShortCode'] + ',' + disname.text + ',' + disstate['Id'] + ',' + disnamehistory.text + ',' + disproducts.text + ',' + dislocation.text + ',' + disDemo.text + ',' + disArea.text
		sql << "(#{districtid['Id']},\"#{districtid['ShortCode']}\",\"#{disname.text}\",\"#{disstate['Id']}\",\"#{disnamehistory.text}\",\"#{disproducts.text}\",\"#{dislocation.text}\",\"#{disDemo.text}\",\"#{disArea.text}\"),"
	end
	sql = sql[0..-2]
	sql << ";"
	sql_upload(sql)
	puts "#{$area}s complete."
end

def process_parties
	load_XML("raw/xml/eml-230-candidates-#{$elec_id}.xml")

	puts "Processing parties."

	sql = "drop table if exists #{$elec}_parties; CREATE TABLE #{$elec}_parties (  `affiliation_id` int(11) NOT NULL,  `short_name` varchar(4) DEFAULT NULL,  `party_name` varchar(50) DEFAULT NULL,  PRIMARY KEY (`affiliation_id`));"
	sql << "TRUNCATE #{$elec}_parties; INSERT ignore INTO #{$elec}_parties (affiliation_id,short_name,party_name) VALUES"

	contest = @doc.xpath(".//Contest")
	contest.each do | cont |
		contestid = cont.at_xpath("./ContestIdentifier")

		cont.children.each do | ddd |
			if ddd['Independent'].nil?
				next
			end

			if ddd['Independent'] == 'yes'
				next
			end		

			if ddd['Independent'] == 'no'
				affil = ddd.at_xpath(".//Affiliation")
				
				if affil.nil?
	             	affil_id = 'NULL'
	             	affil_short = ''
	             	affil_name = ''
	            else
	            	affil_id = ddd.at_xpath(".//AffiliationIdentifier")['Id']
	            	affil_short = ddd.at_xpath(".//AffiliationIdentifier")['ShortCode']
	            	affil_name = ddd.at_xpath(".//RegisteredName").text
	            end
			else
				affil_id = 'NULL'
				affil_short = ''
				affil_name = ''
			end

			#puts affil_id + affil_short + affil_name
			sql << "(#{affil_id},\"#{affil_short}\",\"#{affil_name}\"),"
		end
	end
	sql = sql[0..-2]
	sql << ";"
	sql_upload(sql)
	puts "Parties complete."
end

def create_results_table
	begin
		sql_upload("select * from #{$elec}_results;").first
	rescue
		sql = "CREATE TABLE #{$elec}_results (  result_id int(7) NOT NULL AUTO_INCREMENT,  area_id int(7) DEFAULT NULL,  entity_id int(6) DEFAULT NULL,  type int(2) DEFAULT NULL,  votes int(5) DEFAULT NULL, updated timestamp not null default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP, PRIMARY KEY (result_id),  UNIQUE KEY idx_unique (area_id,entity_id,type),  KEY idx_candidate (entity_id),  KEY idx_type (type));"
		sql_upload(sql)
		puts "Results table complete"
	end

	begin
		sql_upload("select * from #{$elec}_senate_results;").first
	rescue
		sql = "CREATE TABLE #{$elec}_senate_results (  result_id int(7) NOT NULL AUTO_INCREMENT,  entity_id int(6) DEFAULT NULL,  type int(2) DEFAULT NULL,  value varchar(10) DEFAULT NULL, updated timestamp not null default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP, PRIMARY KEY (result_id),  UNIQUE KEY idx_unique (entity_id,type),  KEY idx_candidate (entity_id),  KEY idx_type (type));"
		sql_upload(sql)
		puts "Senate results table complete"
	end



end

def create_votetypes_table
	begin
		sql_upload("select * from #{$elec}_vote_types;").first
	rescue
		sql = "CREATE TABLE #{$elec}_vote_types (  vote_id int(11) NOT NULL,  type varchar(15) DEFAULT NULL,  description varchar(40) DEFAULT NULL, level varchar(20), PRIMARY KEY (vote_id),  UNIQUE KEY idx_type (type));"
		sql_upload(sql)
		sql_upload("INSERT INTO #{$elec}_vote_types (`vote_id`, `type`, `description`, `level`) VALUES ('1', 'pp_fp', 'polling place first preference', 'pp'), ('2', 'pp_his', 'polling place historic', 'pp'), ('3', 'pp_perc', 'polling place percent', 'pp'), ('4', 'pp_swing', 'polling place swing', 'pp'), ('5', 'pp_tcp', 'polling place tcp', 'pp'), ('6', 'pp_tcp_his', 'polling place tcp historic', 'pp'), ('7', 'pp_tcp_perc', 'polling place tcp percent', 'pp'), ('8', 'pp_tcp_swing', 'polling place tcp swing', 'pp'), ('9', 'c_ord_votes', 'candidate ordinary votes', 'seat'), ('10', 'c_ord_his', 'candidate ordinary historic', 'seat'), ('11', 'c_ord_perc', 'candidate ordinary percent', 'seat'), ('12', 'c_ord_swing', 'candidate ordinary swing', 'seat'), ('13', 'c_abs_votes', 'candidate absent votes', 'seat'), ('14', 'c_abs_his', 'candidate absent historic', 'seat'), ('15', 'c_abs_perc', 'candidate absent percent', 'seat'), ('16', 'c_abs_swing', 'candidate absent swing', 'seat'), ('17', 'c_pro_votes', 'candidate provisional votes', 'seat'), ('18', 'c_pro_his', 'candidate provisional historic', 'seat'), ('19', 'c_pro_perc', 'candidate provisional percent', 'seat'), ('20', 'c_pro_swing', 'candidate provisional swing', 'seat'), ('21', 'c_pre_votes', 'candidate prepoll votes', 'seat'), ('22', 'c_pre_his', 'candidate prepoll historic', 'seat'), ('23', 'c_pre_perc', 'candidate prepoll percent', 'seat'), ('24', 'c_pre_swing', 'candidate prepoll swing', 'seat'), ('25', 'c_pos_votes', 'candidate postal votes', 'seat'), ('26', 'c_pos_his', 'candidate postal historic', 'seat'), ('27', 'c_pos_perc', 'candidate postal percent', 'seat'), ('28', 'c_pos_swing', 'candidate postal swing', 'seat'), ('29', 'c_tot_votes', 'candidate total votes', 'seat'), ('30', 'c_tot_his', 'candidate total historic', 'seat'), ('31', 'c_tot_perc', 'candidate total percent', 'seat'), ('32', 'c_tot_swing', 'candidate total swing', 'seat'), ('33', 'c_tot_mhis', 'candidate total matched historic', 'seat'), ('34', 'tpp_votes', 'two party preferred votes', 'seat'), ('35', 'tpp_his', 'two party preferred historic', 'seat'), ('36', 'tpp_perc', 'two party preferred percent', 'seat'), ('37', 'tpp_swing', 'two party preferred swing', 'seat'), ('38', 'tpp_mhis', 'two party preferred matched historic', 'seat'), ('39', 'tcp_ord_votes', 'tcp ordinary votes', 'seat'), ('40', 'tcp_ord_his', 'tcp ordinary historic', 'seat'), ('41', 'tcp_ord_perc', 'tcp ordinary percentage', 'seat'), ('42', 'tcp_ord_swing', 'tcp ordinary swing', 'seat'), ('43', 'tcp_abs_votes', 'tcp absent votes', 'seat'), ('44', 'tcp_abs_his', 'tcp absent historic', 'seat'), ('45', 'tcp_abs_perc', 'tcp absent percentage', 'seat'), ('46', 'tcp_abs_swing', 'tcp absent swing', 'seat'), ('47', 'tcp_pro_votes', 'tcp provisional votes', 'seat'), ('48', 'tcp_pro_his', 'tcp provisional historic', 'seat'), ('49', 'tcp_pro_perc', 'tcp provisional percentage', 'seat'), ('50', 'tcp_pro_swing', 'tcp provisional swing', 'seat'), ('51', 'tcp_pre_votes', 'tcp prepoll votes', 'seat'), ('52', 'tcp_pre_his', 'tcp prepoll historic', 'seat'), ('53', 'tcp_pre_perc', 'tcp prepoll percentage', 'seat'), ('54', 'tcp_pre_swing', 'tcp prepoll swing', 'seat'), ('55', 'tcp_pos_votes', 'tcp postal votes', 'seat'), ('56', 'tcp_pos_his', 'tcp postal historic', 'seat'), ('57', 'tcp_pos_perc', 'tcp postal percentage', 'seat'), ('58', 'tcp_pos_swing', 'tcp postal swing', 'seat'), ('59', 'tcp_tot_votes', 'tcp total votes', 'seat'), ('60', 'tcp_tot_his', 'tcp total historic', 'seat'), ('61', 'tcp_tot_perc', 'tcp total percentage', 'seat'), ('62', 'tcp_tot_swing', 'tcp total swing', 'seat'), ('63', 'tcp_tot_mhis', 'tcp total matched historic', 'seat'), ('64', 'tco_tot_mhf', 'tcp total matched historic first pref', 'seat'), ('100', 'votes', 'all votes for the senate candidates', NULL), ('101', 'ticket', 'above the line votes', NULL), ('102', 'group', 'above the line plus candidate votes', NULL), ('103', 'state_formal', 'formal senate votes', NULL), ('104', 'state_informal', 'informal senate votes', NULL), ('105', 'state_total', 'total senate votes', NULL);")
		puts "VoteTypes table complete"
	end
end

def process_house

	load_XML("raw/xml/aec-mediafeed-results-detailed-verbose-#{$elec_id}.xml")

	contests = @doc.at_xpath(".//House/Contests")

	sql = "INSERT INTO " + $elec + "_results (result_id,area_id,entity_id,type,votes) VALUES "

	contests.children.each do | contest |
		contestid = contest.at_xpath("./ContestIdentifier")['Id']

		polplaces = contest.at_xpath(".//PollingPlaces")
		polplaces.children.each do | polplace |
			polplaceid = polplace.at_xpath(".//PollingPlaceIdentifier")['Id']

			firstprefs = polplace.at_xpath(".//FirstPreferences")

			firstprefcand = polplace.xpath("./FirstPreferences/Candidate")

			firstprefcand.each do | fpref |
				candid  = fpref.at_xpath(".//CandidateIdentifier")
				canid = candid['Id']
				fpvotes = fpref.at_xpath(".//Votes").text
				historic = fpref.at_xpath(".//Votes")['Historic']
				percentage = fpref.at_xpath(".//Votes")['Percentage'].to_f * 100
				swing = fpref.at_xpath(".//Votes")['Swing'].to_f * 100
				sql << "(NULL,#{polplaceid},#{canid},1,#{fpvotes}),"
				if $file_type == 'verbose'
					sql << "(NULL,#{polplaceid},#{canid},2,#{historic}),"
					sql << "(NULL,#{polplaceid},#{canid},3,#{percentage.round}),"
					sql << "(NULL,#{polplaceid},#{canid},4,#{swing.round}),"
				end
			end

			twocanpref = polplace.at_xpath(".//TwoCandidatePreferred")
		
			twocanpref.children.each do | tcpref |
				canid  = tcpref.at_xpath(".//CandidateIdentifier")['Id']
				tcpvotes = tcpref.at_xpath(".//Votes").text
				tcphistoric = tcpref.at_xpath(".//Votes")['Historic']
				tcppercentage = tcpref.at_xpath(".//Votes")['Percentage'].to_f * 100
				tcpswing = tcpref.at_xpath(".//Votes")['Swing'].to_f * 100
				sql << "(NULL,#{polplaceid},#{canid},5,#{tcpvotes}),"
				if $file_type == 'verbose'
					sql << "(NULL,#{polplaceid},#{canid},6,#{tcphistoric}),"
					sql << "(NULL,#{polplaceid},#{canid},7,#{tcppercentage.round}),"
					sql << "(NULL,#{polplaceid},#{canid},8,#{tcpswing.round}),"
				end
			end
		
			forvote = firstprefs.at_xpath("./Formal/Votes")
			inforvote = firstprefs.at_xpath("./Informal/Votes")
			totvote = firstprefs.at_xpath("./Total/Votes")
		
			sql << "(NULL,#{polplaceid},99990,1,#{forvote.text}),"
			sql << "(NULL,#{polplaceid},99991,1,#{inforvote.text}),"
			sql << "(NULL,#{polplaceid},99992,1,#{totvote.text}),"
			if $file_type == 'verbose'
				sql << "(NULL,#{polplaceid},99990,2,#{forvote['Historic']}),"
				sql << "(NULL,#{polplaceid},99990,3,#{forvote['Percentage'].to_f * 100}),"
				sql << "(NULL,#{polplaceid},99990,4,#{forvote['Swing'].to_f * 100}),"
				
				sql << "(NULL,#{polplaceid},99991,2,#{inforvote['Historic']}),"
				sql << "(NULL,#{polplaceid},99991,3,#{inforvote['Percentage'].to_f * 100}),"
				sql << "(NULL,#{polplaceid},99991,4,#{inforvote['Swing'].to_f * 100}),"
				
				sql << "(NULL,#{polplaceid},99992,2,#{totvote['Historic']}),"
				sql << "(NULL,#{polplaceid},99992,3,#{totvote['Percentage'].to_f * 100}),"
				sql << "(NULL,#{polplaceid},99992,4,#{totvote['Swing'].to_f * 100}),"
			end
		end
	end

	sql = sql[0..-2]
	sql << "ON DUPLICATE KEY UPDATE votes = values(votes);"

	log_report(2,'Starting Booth Results')
	sql_upload(sql)
	log_report(1,'Uploaded Booth Results')
	sleep 5
end

def two_party_preferred

	load_XML("raw/xml/aec-mediafeed-results-detailed-verbose-#{$elec_id}.xml")

	contests = @doc.at_xpath(".//House/Contests")

	sql = "INSERT INTO " + $elec + "_results (result_id,area_id,entity_id,type,votes) VALUES "

	contests.children.each do | contest |
		contestid = contest.at_xpath("./ContestIdentifier")['Id']
		tpp_coal = contest.xpath("./TwoPartyPreferred/Coalition")
		tpp_coal.each do | tppc |
			coalid = tppc.at_xpath("./CoalitionIdentifier")['Id']
			cvotes = tppc.at_xpath("./Votes")
			sql << "(NULL,#{contestid},#{coalid},34,#{cvotes.text}),"
			if $file_type == 'verbose'
				sql << "(NULL,#{contestid},#{coalid},35,#{cvotes['Historic']}),"
				sql << "(NULL,#{contestid},#{coalid},36,#{cvotes['Percentage'].to_f * 100}),"
				sql << "(NULL,#{contestid},#{coalid},37,#{cvotes['Swing'].to_f * 100}),"
				sql << "(NULL,#{contestid},#{coalid},38,#{cvotes['MatchedHistoric']}),"
			end
		end
	end

	sql = sql[0..-2]
	sql << "ON DUPLICATE KEY UPDATE votes = values(votes);"
	log_report(2,'Starting 2PP')
	sql_upload(sql)
	log_report(1,'Uploaded 2PP')
	sleep 5
end

def process_house_candidates

	load_XML("raw/xml/aec-mediafeed-results-detailed-verbose-#{$elec_id}.xml")

	contests = @doc.at_xpath(".//House/Contests")

	sql = "INSERT INTO " + $elec + "_results (result_id,area_id,entity_id,type,votes) VALUES "

	contests.children.each do | contest |
		contestid = contest.at_xpath("./ContestIdentifier")['Id']

		candidate = contest.xpath("./FirstPreferences/Candidate")
		candidate.each do | cand |
			candid  = cand.at_xpath("./CandidateIdentifier")
			totvotes = cand.at_xpath("./Votes")
			ordvotes = cand.at_xpath("./VotesByType/Votes[@Type='Ordinary']")
			absvotes = cand.at_xpath("./VotesByType/Votes[@Type='Absent']")
			provotes = cand.at_xpath("./VotesByType/Votes[@Type='Provisional']")
			prevotes = cand.at_xpath("./VotesByType/Votes[@Type='PrePoll']")
			posvotes = cand.at_xpath("./VotesByType/Votes[@Type='Postal']")

			sql << "(NULL,#{contestid},#{candid['Id']},29,#{totvotes.text}),"
			sql << "(NULL,#{contestid},#{candid['Id']},33,#{totvotes['MatchedHistoric']}),"
			sql << "(NULL,#{contestid},#{candid['Id']},9,#{ordvotes.text}),"
			sql << "(NULL,#{contestid},#{candid['Id']},13,#{absvotes.text}),"
			sql << "(NULL,#{contestid},#{candid['Id']},17,#{provotes.text}),"
			sql << "(NULL,#{contestid},#{candid['Id']},21,#{prevotes.text}),"
			sql << "(NULL,#{contestid},#{candid['Id']},25,#{posvotes.text}),"

			if $file_type == 'verbose'
				sql << "(NULL,#{contestid},#{candid['Id']},30,#{totvotes['Historic']}),"
				sql << "(NULL,#{contestid},#{candid['Id']},31,#{totvotes['Percentage'].to_f * 100}),"
				sql << "(NULL,#{contestid},#{candid['Id']},32,#{totvotes['Swing'].to_f * 100}),"
				sql << "(NULL,#{contestid},#{candid['Id']},10,#{ordvotes['Historic']}),"
				sql << "(NULL,#{contestid},#{candid['Id']},11,#{ordvotes['Percentage'].to_f * 100}),"
				sql << "(NULL,#{contestid},#{candid['Id']},12,#{ordvotes['Swing'].to_f * 100}),"
				sql << "(NULL,#{contestid},#{candid['Id']},14,#{absvotes['Historic']}),"
				sql << "(NULL,#{contestid},#{candid['Id']},15,#{absvotes['Percentage'].to_f * 100}),"
				sql << "(NULL,#{contestid},#{candid['Id']},16,#{absvotes['Swing'].to_f * 100}),"
				sql << "(NULL,#{contestid},#{candid['Id']},18,#{provotes['Historic']}),"
				sql << "(NULL,#{contestid},#{candid['Id']},19,#{provotes['Percentage'].to_f * 100}),"
				sql << "(NULL,#{contestid},#{candid['Id']},20,#{provotes['Swing'].to_f * 100}),"
				sql << "(NULL,#{contestid},#{candid['Id']},22,#{prevotes['Historic']}),"
				sql << "(NULL,#{contestid},#{candid['Id']},23,#{prevotes['Percentage'].to_f * 100}),"
				sql << "(NULL,#{contestid},#{candid['Id']},24,#{prevotes['Swing'].to_f * 100}),"	
				sql << "(NULL,#{contestid},#{candid['Id']},26,#{posvotes['Historic']}),"
				sql << "(NULL,#{contestid},#{candid['Id']},27,#{posvotes['Percentage'].to_f * 100}),"
				sql << "(NULL,#{contestid},#{candid['Id']},28,#{posvotes['Swing'].to_f * 100}),"									
			end
		end

		forvote = contest.at_xpath("./FirstPreferences/Formal/Votes")
		forord = contest.at_xpath("./FirstPreferences/Formal/VotesByType/Votes[@Type='Ordinary']")
		forabs = contest.at_xpath("./FirstPreferences/Formal/VotesByType/Votes[@Type='Absent']")
		forpro = contest.at_xpath("./FirstPreferences/Formal/VotesByType/Votes[@Type='Provisional']")
		forpre = contest.at_xpath("./FirstPreferences/Formal/VotesByType/Votes[@Type='PrePoll']")
		forpos = contest.at_xpath("./FirstPreferences/Formal/VotesByType/Votes[@Type='Postal']")

		infvote = contest.at_xpath("./FirstPreferences/Informal/Votes")
		inford = contest.at_xpath("./FirstPreferences/Informal/VotesByType/Votes[@Type='Ordinary']")
		infabs = contest.at_xpath("./FirstPreferences/Informal/VotesByType/Votes[@Type='Absent']")
		infpro = contest.at_xpath("./FirstPreferences/Informal/VotesByType/Votes[@Type='Provisional']")
		infpre = contest.at_xpath("./FirstPreferences/Informal/VotesByType/Votes[@Type='PrePoll']")
		infpos = contest.at_xpath("./FirstPreferences/Informal/VotesByType/Votes[@Type='Postal']")

		totvote = contest.at_xpath("./FirstPreferences/Total/Votes")
		totord = contest.at_xpath("./FirstPreferences/Total/VotesByType/Votes[@Type='Ordinary']")
		totabs = contest.at_xpath("./FirstPreferences/Total/VotesByType/Votes[@Type='Absent']")
		totpro = contest.at_xpath("./FirstPreferences/Total/VotesByType/Votes[@Type='Provisional']")
		totpre = contest.at_xpath("./FirstPreferences/Total/VotesByType/Votes[@Type='PrePoll']")
		totpos = contest.at_xpath("./FirstPreferences/Total/VotesByType/Votes[@Type='Postal']")

		sql << "(NULL,#{contestid},99990,29,#{forvote.text}),"
		sql << "(NULL,#{contestid},99991,29,#{infvote.text}),"
		sql << "(NULL,#{contestid},99992,29,#{totvote.text}),"

		sql << "(NULL,#{contestid},99990,33,#{forvote['MatchedHistoric']}),"
		sql << "(NULL,#{contestid},99991,33,#{infvote['MatchedHistoric']}),"
		sql << "(NULL,#{contestid},99992,33,#{totvote['MatchedHistoric']}),"

		sql << "(NULL,#{contestid},99990,9,#{forord.text}),"
		sql << "(NULL,#{contestid},99991,9,#{inford.text}),"
		sql << "(NULL,#{contestid},99992,9,#{totord.text}),"

		sql << "(NULL,#{contestid},99990,13,#{forabs.text}),"
		sql << "(NULL,#{contestid},99991,13,#{infabs.text}),"
		sql << "(NULL,#{contestid},99992,13,#{totabs.text}),"

		sql << "(NULL,#{contestid},99990,17,#{forpro.text}),"
		sql << "(NULL,#{contestid},99991,17,#{infpro.text}),"
		sql << "(NULL,#{contestid},99992,17,#{totpro.text}),"

		sql << "(NULL,#{contestid},99990,21,#{forpre.text}),"
		sql << "(NULL,#{contestid},99991,21,#{infpre.text}),"
		sql << "(NULL,#{contestid},99992,21,#{totpre.text}),"

		sql << "(NULL,#{contestid},99990,25,#{forpos.text}),"
		sql << "(NULL,#{contestid},99991,25,#{infpos.text}),"
		sql << "(NULL,#{contestid},99992,25,#{totpos.text}),"

		if $file_type == 'verbose'
			sql << "(NULL,#{contestid},99990,32,#{forvote['Swing'].to_f * 100}),"
			sql << "(NULL,#{contestid},99991,32,#{infvote['Swing'].to_f * 100}),"
			sql << "(NULL,#{contestid},99992,32,#{totvote['Swing'].to_f * 100}),"
		end

	end

	sql = sql[0..-2]
	sql << "ON DUPLICATE KEY UPDATE votes = values(votes);"
	log_report(2,'startomg Candidate Results')
	sql_upload(sql)
	log_report(1,'Uploaded Candidate Results')
	sleep 5
end

def process_house_tcp

	load_XML("raw/xml/aec-mediafeed-results-detailed-verbose-#{$elec_id}.xml")

	contests = @doc.at_xpath(".//House/Contests")

	sql = "INSERT INTO " + $elec + "_results (result_id,area_id,entity_id,type,votes) VALUES "
	tst = sql

	contests.children.each do | contest |
		contestid = contest.at_xpath("./ContestIdentifier")['Id']

		candidate = contest.xpath("./TwoCandidatePreferred/Candidate")
		candidate.each do | cand |
			candid  = cand.at_xpath("./CandidateIdentifier")
			totvotes = cand.at_xpath("./Votes")
			ordvotes = cand.at_xpath("./VotesByType/Votes[@Type='Ordinary']")
			absvotes = cand.at_xpath("./VotesByType/Votes[@Type='Absent']")
			provotes = cand.at_xpath("./VotesByType/Votes[@Type='Provisional']")
			prevotes = cand.at_xpath("./VotesByType/Votes[@Type='PrePoll']")
			posvotes = cand.at_xpath("./VotesByType/Votes[@Type='Postal']")

			sql << "(NULL,#{contestid},#{candid['Id']},59,#{totvotes.text}),"
			sql << "(NULL,#{contestid},#{candid['Id']},63,#{totvotes['MatchedHistoric']}),"
			sql << "(NULL,#{contestid},#{candid['Id']},39,#{ordvotes.text}),"
			sql << "(NULL,#{contestid},#{candid['Id']},43,#{absvotes.text}),"
			sql << "(NULL,#{contestid},#{candid['Id']},47,#{provotes.text}),"
			sql << "(NULL,#{contestid},#{candid['Id']},51,#{prevotes.text}),"
			sql << "(NULL,#{contestid},#{candid['Id']},55,#{posvotes.text}),"

			if $file_type == 'verbose'
				sql << "(NULL,#{contestid},#{candid['Id']},60,#{totvotes['Historic']}),"
				sql << "(NULL,#{contestid},#{candid['Id']},64,#{totvotes['MatchedHistoricFirstPrefsIn']}),"
				sql << "(NULL,#{contestid},#{candid['Id']},61,#{totvotes['Percentage'].to_f * 100}),"
				sql << "(NULL,#{contestid},#{candid['Id']},62,#{totvotes['Swing'].to_f * 100}),"
				sql << "(NULL,#{contestid},#{candid['Id']},40,#{ordvotes['Historic']}),"
				sql << "(NULL,#{contestid},#{candid['Id']},41,#{ordvotes['Percentage'].to_f * 100}),"
				sql << "(NULL,#{contestid},#{candid['Id']},42,#{ordvotes['Swing'].to_f * 100}),"
				sql << "(NULL,#{contestid},#{candid['Id']},44,#{absvotes['Historic']}),"
				sql << "(NULL,#{contestid},#{candid['Id']},45,#{absvotes['Percentage'].to_f * 100}),"
				sql << "(NULL,#{contestid},#{candid['Id']},46,#{absvotes['Swing'].to_f * 100}),"
				sql << "(NULL,#{contestid},#{candid['Id']},48,#{provotes['Historic']}),"
				sql << "(NULL,#{contestid},#{candid['Id']},49,#{provotes['Percentage'].to_f * 100}),"
				sql << "(NULL,#{contestid},#{candid['Id']},50,#{provotes['Swing'].to_f * 100}),"
				sql << "(NULL,#{contestid},#{candid['Id']},52,#{prevotes['Historic']}),"
				sql << "(NULL,#{contestid},#{candid['Id']},53,#{prevotes['Percentage'].to_f * 100}),"
				sql << "(NULL,#{contestid},#{candid['Id']},54,#{prevotes['Swing'].to_f * 100}),"	
				sql << "(NULL,#{contestid},#{candid['Id']},56,#{posvotes['Historic']}),"
				sql << "(NULL,#{contestid},#{candid['Id']},57,#{posvotes['Percentage'].to_f * 100}),"
				sql << "(NULL,#{contestid},#{candid['Id']},58,#{posvotes['Swing'].to_f * 100}),"
			end

		end
	end

		sql = sql[0..-2]
		sql << " ON DUPLICATE KEY UPDATE votes = values(votes);"
		log_report(2,'starting House TCP')
		sql_upload(sql)
		log_report(1,'Uploaded House TCP')
		sleep 5
end

def process_senate

	load_XML("raw/xml/aec-mediafeed-results-detailed-verbose-#{$elec_id}.xml")

	contests = @doc.at_xpath(".//Senate/Contests")

	sql = ""

	sql = "INSERT INTO " + $elec + "_senate_results (result_id,entity_id,type,`value`) VALUES "

	contests.children.each do | contest |
		contestid = contest.at_xpath("./ContestIdentifier")['Id']

		group = contest.xpath("./FirstPreferences/Group")

		group.each do | grou |
			groupid = grou.at_xpath("./GroupIdentifier")['Id']
			candidate = grou.xpath("./Candidate")
			candidate.each do | cand |
				candid = cand.at_xpath("./CandidateIdentifier")['Id']
				votes = cand.at_xpath("./Votes").text
				sql << "(NULL,#{candid},100,#{votes}),"
			end
			ticketVotes = grou.at_xpath("./TicketVotes/Votes").text
			groupVotes = grou.at_xpath("./GroupVotes/Votes").text
			sql << "(NULL,#{groupid},101,#{ticketVotes}),"
			sql << "(NULL,#{groupid},102,#{groupVotes}),"
		end

		ungroup = contest.xpath("./FirstPreferences/UngroupedCandidate")

		ungroup.each do | ungrou |
			candid = ungrou.at_xpath("./CandidateIdentifier")['Id']
			votes = ungrou.at_xpath("./Votes").text
			sql << "(NULL,#{candid},100,#{votes}),"
		end

		fvotes = contest.at_xpath("./FirstPreferences/Formal/Votes").text
		ivotes = contest.at_xpath("./FirstPreferences/Informal/Votes").text
		tvotes = contest.at_xpath("./FirstPreferences/Total/Votes").text
		sql << "(NULL,\'#{contestid}\',103,#{fvotes}),"
		sql << "(NULL,\'#{contestid}\',104,#{ivotes}),"
		sql << "(NULL,\'#{contestid}\',105,#{tvotes}),"

	end

	sql = sql[0..-2]
	sql << "ON DUPLICATE KEY UPDATE `value` = values(`value`);"
	log_report(2,'starting Senate Upload')
	sql_upload(sql)
	log_report(1,'Uploaded Senate')
end
