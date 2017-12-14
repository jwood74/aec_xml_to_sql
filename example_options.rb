## Example Options file
## Put your own options here

## this is a place you can set up the options of the parsing

## location
$location = '/home/user'


## database
$database = 'results'
## this is the database where all the resuls will be


## election prefix
$elec = 'elec'
## used to prefix each table
## e.g qld2015_ or fed2016_

## district or ward
$area = 'district'
## State Elections use districts
## Council Elections use wards

## do you want to use a MySQL table to determine if it should loop or not?
## TODO
$mysql_run = 'TRUE'
## it can just loop endlessly, or you could connect to MySQL table to check for RUN condition
## options are TRUE or FALSE


## AEC election ID
$elec_id = 20499
## the ID of the election event


## AEC file type
$file_type = 'verbose'
## the AEC offers different levels of granularity


## reporting level
$report_level = 10
## used for debugging, this will print different logs throughout the code
## 0 == none	5 == medium		10 == lots


## MySQL username
$mysql_user = 'user'


## MySQL password
$mysql_pass = 'pass'


## Run Method
$run_method = 'setup'
## whether the code should be looping through the results, or running the setup code
## setup			results


## Testing?
$testing = true
## true or false 