#!/usr/local/bin/python

import subprocess
import json
import yaml
import dateutil.parser
import datetime
import boto

def get_users_with_null_sites():

	apid_array = [
		"apid-cli",
		"ef",
		"user",
		"-c",
		"cnhi",
		"--filter",
		"sites.siteName is null",
		"--attributes",
		'["created","lastUpdated","uuid","sites","lastLogin"]',
		"--max-results",
		"10"	
	]

	jq_array = [
		"jq",
		".results"
	]

	apid_call = subprocess.Popen(apid_array, stdout=subprocess.PIPE)
	jq_call = subprocess.Popen(jq_array, stdin=apid_call.stdout, stdout=subprocess.PIPE)
	apid_call.stdout.close()
	filtered_apid_results = jq_call.communicate()[0]
	apid_call.wait()
	#print filtered_apid_results
	return filtered_apid_results

def calculate_last_user_events(user_list):
	#print len(user_list)
	for user in user_list:
		if user['lastUpdated'] > user['created']:
			user['trueDate'] = user['lastUpdated']
			user['lastEvent'] = 'entity_update'
		else:
			user['trueDate'] = user['created']
			user['lastEvent'] = 'entity_create'
	return user_list

def build_s3_url(user_object):
	temp_date = dateutil.parser.parse(user_object['trueDate'])

	date_string_array = [
		str(temp_date.year),
		str(temp_date.month),
		str(temp_date.day),
		str(temp_date.hour),
		'00',
		'00'
	]
	url_string = (
		'capture/' + user_object['lastEvent'] + '/'
		+ reduce(lambda x,y: x + '/' + y, date_string_array) 
		+ '/fdyc2rm7kvqcnftgyjzsrbawer/'
	)

	return url_string

def get_s3_analytics(s3_url):
	s3_key_array = [];
	s3 = boto.s3.connect_to_region('us-east-1')
	s3_bucket = s3.get_bucket('janrain.analytics')

	for item in s3_bucket.list(prefix=s3_url):
		temp_string = item.get_contents_as_string()
		temp_string = temp_string[temp_string.find('{'):-1]
		s3_key_array.append(yaml.load(temp_string))
	
	return s3_key_array

def main():
	
	user_list = yaml.load(get_users_with_null_sites())
	calculate_last_user_events(user_list)
	s3_url = build_s3_url(user_list[1])
	s3_results = get_s3_analytics(s3_url)
	print s3_results[0]['application_id']
	
	return

if __name__ == "__main__":
    main()

# function dateStringToArray {
# 	tempDateArray=()
# 	tempDate=$1
# 	# echo $tempDate
# 	# $tempDate=echo "${tempDate::-1}"
# 	echo "${tempDate::-1}"
# 	# echo $tempDate
# }

# function getNullSitesUsers {
# 	echo $(apid-cli ef user -c cnhi --filter "sites.siteName is null" --attributes '["created","lastUpdated","uuid","sites","lastLogin"]' --max-results 10 | jq .results)
# }

# function getLastUserUpdateTime {
# 	echo $userList | jq 'map(if .lastUpdated > .created
# 			then . + {"trueDate":.lastUpdated}
# 			else . + {"trueDate":.created}
# 			end
# 	   )'
# }

# userList=$(getNullSitesUsers)

# userList=$(getLastUserUpdateTime)

# dateStringToArray "2014-10-09 18:16:31.969098 +0000"

# # echo $userList | jq .
