#!/usr/bin/ruby

require 'net/ftp'

ftp = Net::FTP.new
ftp.connect("mediafeedarchive.aec.gov.au")
ftp.login
ftp.chdir("/13745/Detailed/Verbose")
# ftp.passive = true

puts ftp

puts ftp.list

exit

ftp = Net::FTP.new('mediafeedarchive.aec.gov.au')
ftp.login
puts ftp
files = ftp.chdir('/13745/Detailed/Verbose')
puts files
files = ftp.list('*')
puts files
ftp.getbinaryfile('nif.rb-0.91.gz', 'nif.gz', 1024)
ftp.close