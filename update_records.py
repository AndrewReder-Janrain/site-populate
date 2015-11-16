#!/usr/local/bin/python

import subprocess
import json
import yaml
import dateutil.parser
import datetime
import time

def make_update_calls(update_dict):
	update_log = open('update_log.txt','w+')
	for update in update_dict:
		update_log.write(str(update)+',')
		update_log.write(str(update_entity(update))+'\n')
		time.sleep(1)

	return

def update_entity(update_json):
	apid_array = [
		"apid-cli",
		"eu",
		"user",
		"{\"sites\":"+update_json['update_payload']+"}",
		"-c",
		"avidreder-dev",
		"--uuid",
		update_json['uuid']
	]
	print apid_array[3]
	apid_call = subprocess.Popen(apid_array, stdout=subprocess.PIPE)
	result = apid_call.communicate()[0]
	print result
	return result

def main():
	with open('sample_updates.txt') as json_file:
		update_dict = yaml.load(json_file)
	make_update_calls(update_dict)
	json_file.close()
	return

if __name__ == "__main__":
    main()

