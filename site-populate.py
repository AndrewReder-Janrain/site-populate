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
	later_date = temp_date + datetime.timedelta(hours=1)
	earlier_date = temp_date + datetime.timedelta(hours=-1)
	date_array = [
		temp_date,
		later_date,
		earlier_date
	]
	url_string_array = []
	for date in date_array:	
		date_string_array = [
			str(date.year),
			str(date.month),
			str(date.day),
			str(date.hour),
			'00',
			'00'
		]
		# url_string = (
		# 	'capture/' + user_object['lastEvent'] + '/'
		# 	+ reduce(lambda x,y: x + '/' + y, date_string_array) 
		# 	+ '/fdyc2rm7kvqcnftgyjzsrbawer/'
		# )

		url_string = (
			'capture/' + 'entity_create' + '/'
			+ reduce(lambda x,y: x + '/' + y, date_string_array) 
			+ '/fdyc2rm7kvqcnftgyjzsrbawer/'
		)
		url_string_array.append(url_string)
		url_string = (
			'capture/' + 'entity_update' + '/'
			+ reduce(lambda x,y: x + '/' + y, date_string_array) 
			+ '/fdyc2rm7kvqcnftgyjzsrbawer/'
		)
		url_string_array.append(url_string)
	return url_string_array

def get_s3_analytics(s3_url):
	print "URL: " + s3_url
	s3_key_array = [];
	s3 = boto.s3.connect_to_region('us-east-1')
	s3_bucket = s3.get_bucket('janrain.analytics')
	# print s3_bucket.get_all_keys(prefix=s3_url)
	for item in s3_bucket.list(prefix=s3_url):
		object_array = []
		temp_string = item.get_contents_as_string()
		num_objects = temp_string.count('\n')
		print num_objects
		for j in range(num_objects):
			print temp_string.find('{')
			print temp_string.find('\n')
			# print temp_string[temp_string.find('{'):temp_string.find('\n')]
			if temp_string.find('\n') > -1:
				s3_key_array.append(yaml.load(temp_string[temp_string.find('{'):temp_string.find('\n')]))
				temp_string = temp_string[temp_string.find('\n')+1:-1]
			else:
				print temp_string
				s3_key_array.append(yaml.load(temp_string[temp_string.find('{'):-1]))
			
			


		# try:
		# 	s3_key_array.append(yaml.load(temp_string))
		# except:
		# 	# temp_string = temp_string[temp_string.find('{'):-1]
		# 	print s3_url
		# 	print item
		# 	# s3_key_array.append(yaml.load(temp_string))
	
	return s3_key_array

def find_s3_event(s3_result_set, user):
	return

def main():
	user_list = yaml.load(get_users_with_null_sites())
	calculate_last_user_events(user_list)
	
	for i in range(3):	
		s3_url_array = build_s3_url(user_list[i])
		for s3_url in s3_url_array:
			s3_results = get_s3_analytics(s3_url)
			log_string = str(i) + ": "
			try:
				log_string += s3_results[0]['application_id']
			except:
				log_string += "No results for: " + user_list[i]['trueDate']
			print log_string
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
