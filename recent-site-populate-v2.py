#!/usr/local/bin/python

import subprocess
import json
import yaml
import dateutil.parser
import datetime
import boto

backup_file = open('backup_user_data.txt','w')
json_file = open('test_data.txt','w')
result_count = 0

# Makes an apid-cli call to get 10 users with null sites
def get_users_with_null_sites():
	global backup_file

	apid_array = [
		"apid-cli",
		"ef",
		"user",
		"-c",
		"cnhi",
		"--filter",
		"sites.siteName is null and created>'2014-10-10'",
		"--attributes",
		'["created","lastUpdated","uuid","sites","lastLogin"]',
		"--max-results",
		"10000"	
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
	backup_file.write(str(filtered_apid_results))
	return filtered_apid_results

# Looks at the user records, and determines what times and events we need to look for in s3
def calculate_last_user_events(user_list):
	for user in user_list:
		user['events'] = []
		user['event_times'] = []
		user['update_array'] = []

		user['events'].append('entity_create')
		user['event_times'].append(user['created'])
		user['events'].append('entity_update')
		user['event_times'].append(user['lastUpdated'])
	return user_list

# For a single user, builds the s3 urls we need to retrieve analytics events
# Looks ahead if the timestamp is within 15 minutes of a different hour
# Also calls get_s3_keys, which actually gets the data
def build_s3_url(user_object):
	global json_file
	s3_results = []
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

			url_string = (
				'capture/' + user_object['events'][j] + '/'
				+ reduce(lambda x,y: x + '/' + y, date_string_array) 
				+ '/fdyc2rm7kvqcnftgyjzsrbawer/'
			)
			s3_results += get_s3_keys(url_string,date,user_object)

	return s3_results

# This is pretty messy
# For each s3 url, goes and checks the analytics
def get_s3_keys(s3_url,date,user_object):
	global json_file
	global result_count

	time_buffer = 15
	
	s3_key_array = []
	s3 = boto.s3.connect_to_region('us-east-1')
	s3_bucket = s3.get_bucket('janrain.analytics')
	# json_file.write('\t'+json.dumps(s3_url)+'\n')
	print s3_url
	for item in s3_bucket.list(prefix=s3_url):
		object_array = []
		item_time = dateutil.parser.parse(item.name[-28:-9] + ' +0000')
		# json_file.write('\t\tUser time: '+str(date)+'\tItem time: '+str(item_time)+'\n')
		
		# Checks whether the date of the user event is close (15 min) to the analytics event time
		# if abs((date - item_time).total_seconds()/60) < time_buffer:
			# json_file.write('\t\tYes\n')
		temp_string = item.get_contents_as_string()
		num_objects = temp_string.count('\n')
		for j in range(num_objects):
			if temp_string.find('\n') > -1:
				result_object = yaml.load(temp_string[temp_string.find('{'):temp_string.find('\n')])
				
				# Checks whether the uuid of the analytics event matches the user
				# Also creates a timestamp to update the user record with, as not all s3 events have timestamps in the payload
				
				if user_object['uuid'] == result_object['value']['uuid']:
					result_count+=1
					print str(result_count)+": "+result_object['value']['uuid'] 
					result_object['backup_date']=""
					result_object['backup_date']=temp_string[0:10]+" "+temp_string[11:19] +" +0000"
					result_object['last_login']=user_object['lastLogin']
					result_object['created']=user_object['created']
					result_object['last_updated']=user_object['lastUpdated']
					s3_key_array.append(result_object)
					# print str(result_object)
					
				# json_file.write('\t\tUUID: '+result_object['value']['uuid']+'\tClient ID: '+result_object['client_id']+'\n')
				temp_string = temp_string[temp_string.find('\n')+1:-1]

	
	return s3_key_array

# Looks up what the site name should be based on the client_id
def lookup_site_name(client_id):
	site_list = {
		"zwktcurqkwxam4u9r7yq496zvvqssek4":"ncnewsonline",
		"fn78qanvyvndggfhwdsjb9gg7wwzb5yz":"CNHI",
		"kb7fnzc3zjpnrjfxts3ec25m3ktwaf4t":"CNHI",
		"t45xr2ewv345nmmyznpm24u76ktkhwdw":"CNHI",
		"bcmsbu6qranf5dawfw87nkk8dsh35x7b":"CNHI",
		"t8v88sp3hh4nc3ekkgnk2c4fsbsfa38y":"alliednews",
		"2ezsxt4fg2q25y4xxcxa5chpm9wspgdd":"americustimesrecorder",
		"t6q4qgv9dwe94eveun4vkwaya3qv9cd8":"andovertownsman",
		"m9racaxyawgguet2nz4aynwfxppq7f7z":"athensreview",
		"3ufzjj567ujh7mczx6juvgmmq45vfn7e":"batesvilleheraldtribune",
		"qdzm9hwt68f4fhte3vdjtd2yywthdjhu":"bdtonline",
		"j3xpjh3a87w68rqczjh54p3wkbf3dswj":"burlesoncrowley",
		"dwgbf5zdxc3799vaxdcwabqmwrgus6ga":"carriagetownenews",
		"jymzv8cqrrxxb8y7fv8y7cd7tfq2r42v":"chickashanews",
		"rrkta86fpc4k2qnvgz3z9jr9vgpn5y4d":"claremoreprogress",
		"svragezyaud8swajwwf3ypuye63dufdq":"cleburnetimesreview",
		"bvr6qqr9642vxjtg6c62ya8x8nm3x2hn":"clintonherald",
		"nj69wh3y6axh4787szf9p6q9psq837xb":"commercejournal",
		"mydk9vnstvcx9sryu3p6y3c6xz7fub9a":"commercial-news",
		"zdruzqc2rtfrg6u92zf8arzvtng3t4uc":"coopercrier",
		"h8bdhk6kby4hwwp5rjkrt9se4e7z426m":"cordeledispatch",
		"9wha3hju7y7ykmephfnn8dd3n75e4umj":"corsicanadailysun",
		"v9zyehwje8jtk9buw82kd69a8d88c6j2":"crossville-chronicle",
		"3vf7r2qu2v2tjhgz5neu28rwreucjq4p":"cullmantimes",
		"fc4dny25b8mxdfcc5t9cqsga8ub5c4fd":"dailyindependent",
		"pwmr8vmsexfw88vrkubqvgs2c3zj3dp2":"dailyiowegian",
		"vpf43dhbur8ndndv3uabkev68k8v44gw":"dailyitem",
		"vyhrgh3dh9dg6e64wd7kjgm6gvfcec6n":"dailysoutherner",
		"at989yc57qguhach5vn9tp4dmsnpe85t":"daltondailycitizen",
		"xct3hcq7qndjh48epaqs6z6j2g8xvgnw":"derrynews",
		"usc6nc3zpm9f3kwexdrc6ecd4vd62kr2":"duncanbanner",
		"pbjqgvq69chmn48axsz34qbfsramxvr2":"eagletribune",
		"7my8qmkhgd862885w866rzm99497wn4k":"edmondsun",
		"947ctgstdmcj4e8kysgxt26ckavpv3at":"effinghamdailynews",
		"f5eu3ce8mrtp3pazyzwmrccthd7aa2yj":"enewscourier",
		"f8sc3j484hntgmab2wtcs9ff2dkvbud6":"enidnews",
		"3atqtpg6zrvrdbgxa3fmjxuaasxw6hex":"farmtalknewspaper",
		"xjedk95ku3e3kx7us8vfaqvqn4jdkven":"fayettetribune",
		"nubhjz5fwak44g9wgdm4zpk2n4b2t9ug":"flyergroup",
		"y8r99rhvxn2bseys2s7jn6dknj67wgyk":"gainesvilleregister",
		"wvxhhkgt97esswy38tdqqtg2sgsv8rpm":"glasgowdailytimes",
		"ntxcbtmcc2escqvbnzd92h3hbdbzdg5h":"gloucestertimes",
		"xfejwq8s4a3dadxw2wcnvbhbwj3ba76x":"goshennews",
		"s4srfdzxmmbyp4f7tptgntzv7eayqmpx":"greensburgdailynews",
		"kg5u9wgp8sb5j8zru9hxn4un78kuvabt":"heraldbanner",
		"pwhysb78sj55weeucnhgf9xdwhhmdfmj":"heraldbulletin",
		"b9xh9n49kwxgcnt3h5da562ufm7v2mcz":"hgazette",
		"y25xdkn2gfutchqa59qcxpzrxecgbaz6":"homemagonline",
		"43ur2yc962jjsvbnfft3n6nbujybnuhg":"itemonline",
		"zn8pcqqy29brawppu75cewm253dtp6mw":"jacksonvilleprogress",
		"q8n9nkv4r22q3mcp6frnp3e3tge2xpe8":"joplinglobe",
		"qu6n2fab6m4p8jfy8axb7marx8vwcdy4":"journal-register",
		"cq6q4hfjb7232jqjbg523ccggpaf5g7g":"journal-times",
		"jy37a488cz8mdda27hdcg7qshd5mg44g":"journalexpress",
		"nrmkxyawf8ceaeerg3e27bcttw6wb9gc":"kokomotribune",
		"w2c7hc9pba7jtrzudm7m7s3jnphnwbzu":"lakeoconeebreeze",
		"vcs78f5crnhr77kx3tsuwy5pee3hghxs":"lockportjournal",
		"nnaaq8ha54hy43zpx873mmahtces4mzn":"mankatofreepress",
		"vkxhy42j9tsw3ep28jegzw9fj6ja2ann":"mcalesternews",
		"mu54ta883msg3xypbqq9vcftpzzfhf63":"mccrearyrecord",
		"7pu9s7rdpa99ce4srrmtmuhmm78eq4fr":"mcleansborotimesleader",
		"dx2gyuqg3j2ka4rygkvrwcvs7hbzg8ft":"meadvilletribune",
		"z9y8hs3ujekmqzz7cwdc2245vrs9g5hn":"meridianstar",
		"9ywrmtzpd97rqzxp7u2hyw3qjxtnsuty":"mineralwellsindex",
		"dp7utudm2afm8h4vesxz82apckfdjt9y":"montgomery-herald",
		"ab89gyewwz5cpu8mghptyvk6us59s9ac":"moultrieobserver",
		"a7sv94jkfkvuvvzs9tzrk3rwr8659rvx":"muskogeephoenix",
		"afn8hsk25k3ac8k9ywey73u3vmnwpupe":"newburyportnews",
		"w6dezs9a93xnx87umbqg95sq3pfpmxpe":"news-tribune",
		"c89n8z6xjztbd2petq8j4wbcfafr2etq":"newsaegis",
		"n8a3dyaetszzvytv9n579c4gvrnx2eg8":"niagara-gazette",
		"t9w2n3xyht8fkvwpzkectes97hhct7m5":"njeffersonnews",
		"h7hxyzv5ud5utjy9xn33fgzaysj3jeac":"normantranscript",
		"6y4qpn68pjnxfjmbm2fkwzx6cenjepua":"orangeleader",
		"vxgny2tre8r3tqb94dy38skxfd8wgkpd":"oskaloosa",
		"rp9qm5hp6g9kyujyq4xayp33wgvdcpaq":"ottumwacourier",
		"rqayuy6kpcyzkf73c79py5t4mpjnf56r":"palestineherald",
		"j96fq78tmtv5xdw6ggr8y7uuhxrdx3mf":"panews",
		"cnsbkx2bq4vmh4fnzwzysx2z3qweexum":"paulsvalleydailydemocrat",
		"j2wyzk2t6dw3e2x7qqn2aq548tfhh8wv":"pellachronicle",
		"7dqe6b4rb5udyjqgz72d9eskcj6v7t3d":"pharostribune",
		"jwe8ttct8kttjkkwq3sqh6bqyhmq6pbc":"pressrepublican",
		"kf3qhwpaqye5akmexthwcm2e3nvqrgmz":"pryordailytimes",
		"dgevszyq436v9hdeyr3ue6vyx2pqq39f":"ptonline",
		"ta94rffvb6254afvk5kumraqpax7mvuz":"randolphguide",
		"ywj5vvc7h79mjra62zkxbnkhrcf3keds":"record-eagle",
		"td8qh2rpa96beb8zw6z5g3szrfkka395":"register-herald",
		"j3hv3htj8h9ddka57ghg43f2sambmw9z":"register-news",
		"m4aqaaqj6ashy8pt985bzxrsuw4q5saz":"reporter",
		"3s2q889fqq9p69zmtf726hb9fubjd53f":"richmondregister",
		"sahmwx3qp97cpzxhctb2smw5esbeur2e":"rockwallheraldbanner",
		"4xkqyr7xjkyrtyt68tu778faeg8s7xmq":"roysecityheraldbanner",
		"9ejdjhwnt3sxwzedbnee3wqgmqmdvuds":"rushvillerepublican",
		"3svsu7mz8a5kqfush7bnmpmsfkyfwqwx":"salemnews",
		"79rh45vqz7at5pupzbj24peu6dccq592":"sentinel-echo",
		"wegrq8r59gc3pzqk3bujf4nqpfxtgcf8":"sharonherald",
		"v553szfk4y6hfnxxxdgqh5fxja7q2n73":"shelbyvilledailyunion",
		"7nq3tp94m5cqamdktdertc2eka8qhrs4":"somerset-kentucky",
		"jangca5vg6rt2x8gzfu9uq2ys6yz2ycb":"starbeacon",
		"5fwb6cmn5rgb43dycnc3nrwzndrztqrr":"stwnewspress",
		"5yrdnmm7w7ucc5s9z8kz7s78srxjuywh":"suwanneedemocrat",
		"gekbkhkw288bm9k2b76u5u8g7c54exbz":"tahlequahdailypress",
		"t7ejh9xdcet3jeagtwn4br3fj45crwqh":"theadanews",
		"8h8g3486ptzptqdpy88vrpnx79vun8ha":"thedailystar",
		"v6uny4fwwha43tug9uzmp2zgpxjkk4ds":"thelandonline",
		"fmnn3dxqk79syr5xe6cs9kkajemzg3rt":"themooreamerican",
		"688ymfb9wgtbymcvbntcaya6vszcxg9y":"themoreheadnews",
		"5xtfrea4u8xknsdbkdzhnue4san68uv8":"thesnaponline",
		"tuvw9cxr93fteyfmnfscey24kk2h3wpa":"thetimestribune",
		"93n6e66zzjy7pgyeerwbzr896hac3udj":"tiftongazette",
		"ekjp8j6ehv7fa7ywtavyyucdz8vu5uje":"times-news",
		"7ysrmvdyntt68g3hcfufuet5rqegpdmb":"timesenterprise",
		"jnwhevunccu7dmpfdb8fqpfq6pmvsw22":"timessentinel",
		"j54hqh7fjvtrr6jasuvmb8bqc92wszs2":"timeswv",
		"bm2hb7pzub3n6upzbvubnzgcjtxqt58x":"tonawanda-news",
		"78gpd5enb6hsz3wc98dw3yzar5edufyp":"tribdem",
		"ech7t5chx5rqqp5h3e576sebbmukn5dx":"tribstar",
		"9ycu8tfdzb6h7z8b6wfnvfhdcf7mqbdx":"unionrecorder",
		"xv7ye96rdk38bdkpxbmtrdg3ypjdxfdd":"valdostadailytimes",
		"92ac88g6ccb8sk63295vgstn27m848dw":"vanceairscoop",
		"2u2by33373er5wkwhe7qrv635ccdh4x9":"washtimesherald",
		"4vbqc6d4zsmnu957y5qzet3gbxtafama":"waurikademocrat",
		"6xh6jate82tncj6848agwtz2s3amw6cm":"wcoutlook",
		"m6p55yxuw7sxk2yy6m4x26w65vp6gbfw":"weatherforddemocrat",
		"vu4e3zznuuqterw6fjcx4jmj4n6r5bjg":"woodwardnews",
		"eqrt86qp5fqnbzs4waambxjjhryvbxfr":"wycoreport",
		"5wegfxdbreywskakdfnkwpc8efd643et":"CNHI"
	}

	return site_list[client_id]

# Given a set of s3 results for a single user, creates the JSON for updating the sites plural of a record
# def remove_duplicates(result_array):
# 	processed_results = []
# 	for result in result_array:
		
# 		# check result against other results for:
# 			# if uuid == uuid



def build_update_object(s3_result_set):
	global json_file

	result_array = []
	repr_array = []
	
	if len(s3_result_set) > 0:

		for result in s3_result_set:			
			update_object = {}
			print result
			update_object['uuid'] = result['value']['uuid']
			update_object['site_name']=lookup_site_name(result['client_id'])
			update_object['first_visit']=result['created']
			update_object['last_visit']=result['last_updated']
			update_object['update_attribute']='sites'
			update_object['update_payload']='[{"siteName":"'+update_object['site_name']+'","firstVisit":"'+update_object['first_visit']+'","lastVisit":"'+update_object['last_visit']+'"}]'

			if update_object != {}:
				result_array.append(update_object)
	result_array = {repr(item): item for item in result_array}.values()
	for result in result_array:
		json_file.write(str(result)+'\n')
	# return result_array
	return

# Just does the stuff
def main():
	global json_file

	user_list = yaml.load(get_users_with_null_sites())
	calculate_last_user_events(user_list)

	for user in user_list:	
		temp_array=build_update_object(build_s3_url(user))
		if temp_array != []:
			s3_results=s3_results+temp_array
	for result in s3_results:
		json_file.write(str(result)+'\n')
	json_file.close()
	return

if __name__ == "__main__":
    main()

