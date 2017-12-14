class AECFtp

	def initialize

		@ftp = Net::FTP.new('mediafeedarchive.aec.gov.au')		#results for 2013	#mediafeedtest for early	#mediafeed for actual
		@ftp.login

		xlink = $elec_id.to_s + "/Detailed/" + $file_type.to_s

		@ftp.chdir(xlink)
		@ftp.passive = true
	end

	def get_file_list

		all_files = @ftp.list('*')

		if all_files.empty? == true
			puts 'no files'
			return false
		end

		files = all_files

		return files
	end

	def find_most_recent_file

		files = get_file_list

		if files == false
			return false
		end

		max_time = 0
		max_file = nil

		files.each do | file |
			filetime = file.split("-").last.split(".").first

			if filetime.to_i > max_time.to_i
				max_time = filetime 
				max_file = file
			end
		end

		return parse_filename(max_file)
	end

	def parse_filename(filestring)
		return "aec" + filestring.split("aec").last
	end

	def download_file(filename)
 		@ftp.getbinaryfile(filename)
 		return pathify(filename)
	end

	def pathify(filename)
		return `pwd`.chomp + "/raw/" + filename
	end

	def local_version_of_file_exists?(filename)
 		return File.file?( pathify(filename) )
	end

	def download_file_if_newer

		 newest_file = find_most_recent_file

		 puts newest_file

		 if newest_file == false
		 	return false
		 end

		 if local_version_of_file_exists?(newest_file) then
		 	return false
		 else
		 	download_file(newest_file)
		 	return pathify(newest_file)
		 end
	end

end