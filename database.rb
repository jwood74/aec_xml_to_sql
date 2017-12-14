#require 'rubygems'
require 'mysql2'
require 'net/ssh/gateway'

class SqlDatabase
	def self.connect(database)
		#Uncomment the below bits if you need to connect via SSH
		#gateway = Net::SSH::Gateway.new(
		#  '192.168.40.100',	#The IP of the server
		#  'some_user'
		# )
		#sshtunnel = gateway.open('localhost', 3306, 3307)

		dbase = Mysql2::Client.new(:host => "127.0.0.1", :username => $mysql_user, :password => $mysql_pass, :database => database, :flags => Mysql2::Client::MULTI_STATEMENTS)
		#gateway.close(sshtunnel)
		return dbase
	end

	def self.runQuery(databasestring,querystring)
		db = self.connect(databasestring.to_s)
		result = db.query(querystring.to_s)
		db.close
		return result
	end

end