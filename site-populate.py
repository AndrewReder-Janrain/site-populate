#!/usr/local/bin/python

import subprocess
import json
import yaml
import dateutil.parser
import datetime
import boto

json_file = open('test_data.json','w')

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
		user['events'] = []
		user['event_times'] = []
		if user['lastUpdated'] == user['created']:
			user['events'].append('entity_create')
			user['event_times'].append(user['created'])
			# user['trueDate'] = user['lastUpdated']
			# user['lastEvent'] = 'entity_update'
		else:
			user['events'].append('entity_create')
			user['event_times'].append(user['created'])
			user['events'].append('entity_update')
			user['event_times'].append(user['lastUpdated'])
	return user_list

def build_s3_url(user_object):
	url_string_array = []
	for j in range(len(user_object['event_times'])):
		temp_date = dateutil.parser.parse(user_object['event_times'][j])
		date_array = [temp_date]
		if temp_date.minute < 15:
			date_array.append(temp_date + datetime.timedelta(hours=-1))
		if temp_date.minute > 45:
			date_array.append(temp_date + datetime.timedelta(hours=1))
		
		for date in date_array:	
			date_string_array = [
				str(date.year),
				str(date.month),
				str(date.day),
				str(date.hour),
				'00',
				'00'
			]
			for i in range(1,len(date_string_array)):
				if len(date_string_array[i])==1:
					date_string_array[i]='0'+date_string_array[i]
			# url_string = (
			# 	'capture/' + user_object['lastEvent'] + '/'
			# 	+ reduce(lambda x,y: x + '/' + y, date_string_array) 
			# 	+ '/fdyc2rm7kvqcnftgyjzsrbawer/'
			# )

			url_string = (
				'capture/' + user_object['events'][j] + '/'
				+ reduce(lambda x,y: x + '/' + y, date_string_array) 
				+ '/fdyc2rm7kvqcnftgyjzsrbawer/'
			)
			url_string_array.append(url_string)
	return url_string_array

def log_user_info(info):
	json_file.write(info)

def get_s3_analytics(s3_url):
	global json_file
	#print "URL: " + s3_url
	s3_key_array = [];
	s3 = boto.s3.connect_to_region('us-east-1')
	s3_bucket = s3.get_bucket('janrain.analytics')
	# print s3_bucket.get_all_keys(prefix=s3_url)
	for item in s3_bucket.list(prefix=s3_url):
		object_array = []
		json_file.write('\t\t'+json.dumps(item.name)+'\n')
		temp_string = item.get_contents_as_string()
		num_objects = temp_string.count('\n')
		for j in range(num_objects):
			# print temp_string.find('{')
			# print temp_string.find('\n')
			# print temp_string[temp_string.find('{'):temp_string.find('\n')]
			if temp_string.find('\n') > -1:
				s3_key_array.append(yaml.load(temp_string[temp_string.find('{'):temp_string.find('\n')]))
				temp_string = temp_string[temp_string.find('\n')+1:-1]

	return s3_key_array

def find_s3_event(s3_result_set, user):
	return

def main():
	global json_file
	# json_file = open('test_data.json','w')
	user_list = yaml.load(get_users_with_null_sites())
	calculate_last_user_events(user_list)
	s3_results = []
	for i in range(2):	
		json_file.write(json.dumps(user_list[i])+'\n')
		

		s3_url_array = build_s3_url(user_list[i])
		for s3_url in s3_url_array:
			json_file.write('\t'+json.dumps(s3_url)+'\n')
			s3_results += get_s3_analytics(s3_url)
		log_string = str(i) + ": "
		try:
			log_string += s3_results[0]['application_id']
		except:
			log_string += "No results for: " + user_list[i]['trueDate']
		print log_string
	# json_string = '[' + ','.join(json.dumps(result) for result in s3_results) + ']'
	# json_file.write(json_string)
	json_file.close()
	# print len(s3_results)
	return

if __name__ == "__main__":
    main()

