from google.cloud import firestore
from google.cloud import firestore_v1
import traceback
import re
import json
import sys
import codecs
import time
# from get_all_fragment_urls import get_all_fragment_urls
# from typing_speed_of_user_for_a_job import sum_of_time_deltas_for_keystroke

def get_all_fragment_urls():
	col_ref = db.collection(u'fragments')
	first_query = col_ref.order_by("url").limit(500)
	docs = first_query.get()
	urls = {}
	while True:
		docs_left = False
		for frag in docs:
			# print(frag.id)
			docs_left = True
			data = frag.to_dict()[u'url']
			urls[frag.id] = data
			last_pop = data
		if docs_left:
			next_query = col_ref.order_by("url").start_after({u'url': last_pop}).limit(500)
			docs = next_query.get()
		if not docs_left:
			break
	return urls

def sum_of_time_deltas_for_keystroke(log_data, duplicates):
	timedelta = 0
	lines = log_data.splitlines()
	prev_timestamp = 0
	current_timestamp = 0
	empty_action = 0
	no_of_keystrokes = 0
	no_of_inserts = 0
	no_of_deletes = 0
	processed_log_data = []
	for line in lines:
		if line in duplicates:
			# print("found duplicate of {}".format(line))
			continue
		duplicates.add(line)
		no_of_keystrokes += 1
		try:
			if "SP155" in line:
				line = line.replace("SP155", "SP|155")

			cols = line.split("|")
			if cols[0] == "INSERT":
				no_of_inserts += 1
			elif cols[0] == "DELETE":
				no_of_deletes += 1
			else:
				empty_action +=1

			if len(cols) == 3:
				# print("found culprit {}".format(cols))
				col2 = cols[1]
				timestamp_pattern = re.compile(r"\d{13}")
				matcher = timestamp_pattern.search(col2)
				if matcher and len(col2) == 13:
					cols.insert(1, "")
				elif col2 in ['SP', '']:
					col3 = cols[2]
					matcher = timestamp_pattern.search(col3)
					if matcher:
					   line = line.replace(matcher.group(0), "{}|".format(matcher.group(0)))
					   cols = line.split("|")
					else:
						continue
				else:
					continue
			elif len(cols) < 3:
				continue

			if prev_timestamp != 0 and current_timestamp != 0:
				timedelta += (current_timestamp - prev_timestamp)/1000.0
			prev_timestamp = current_timestamp
			current_timestamp = int(cols[2])
			processed_log_data.append("{}|{}|{}|{}".format(cols[0], cols[1], cols[2], cols[3]))
			# print(timedelta, prev_timestamp, current_timestamp)
		except ValueError as valueError:
			print("ValueError", line)
			traceback.print_exc()
		except Exception as e:
			traceback.print_exc()
			print("exception", line)
			# print(e, current_timestamp, prev_timestamp)

	return timedelta, no_of_keystrokes, no_of_inserts, no_of_deletes, processed_log_data

def get_user_email(userID):
	user = db.collection('users').document(userID).get()
	email = user.get('email')
	return email

def get_jobs_dict_by_user():
	# [START cursor_paginate]
	jobs_ref = db.collection(u'jobs')
	first_query = jobs_ref.order_by(u'user').order_by(u'assignment_ts').limit(100)
	docs = first_query.get()
	jobs = {}
	# print("job batch proces started")
	while True:
		docs_left = False
		# print("child job batch proces started")
		for job in docs:
			docs_left = True
			data = job.to_dict()
			if data['user'] not in jobs:
				jobs[data['user']] = []
			jobs[data['user']].append(data)
			last_pop = data[u'fragment']
			last_pop1 = data[u'assignment_ts']
		next_query = jobs_ref.order_by(u'user').order_by(u'assignment_ts').start_after({
				u'fragment': last_pop,
				u'assignment_ts': last_pop1,
				u'user': data['user']
			}).limit(100)
		docs = next_query.get()
		# print("child job batch proces completed")

		if not docs_left:
			break
	# print("job batch proces completed")

	return jobs

def write_to_file(filepath, data):
	try:
		with open(filepath, 'w') as file_handler:
			for item in data:
				file_handler.write("{}\n".format(item))
	except Exception as e:
		print("Exception while writing to file %s"%filepath)

if __name__ == '__main__':
	'''
		This script will get jobs collection and process it to create data analytics CSV file.
		Will store each job log data in a separate file
	'''
	# print(get_jobs_dict_by_user())
	# get_jobs_dict_by_user1()
	sample = codecs.open('typing_speed_of_user_for_a_job_paginated_040620191507.csv', 'w', encoding='utf-8')
	db1 = firestore_v1.Client()
	db = firestore.Client()
	fragment_audio_urls = {}
	fetch_fragment_urls = time.time()
	print('Getting fragment urls started')
	try:
		with open('fragment_urls.json') as json_file:
			fragment_audio_urls = json.load(json_file)

	except FileNotFoundError:
		print("Create a fragment_urls.json file")
		quit()
	if fragment_audio_urls == {}:
		print("fragment_urls are emtpy. Pulling new from datastore")
		fragment_audio_urls = get_all_fragment_urls()
	print('Getting fragment urls completed')
	# print(fragment_audio_urls)
	# sys.exit(0)
	#
	print("fragment urls collection time",time.time()-fetch_fragment_urls)

	fetch_jobs = time.time()
	print('getting jobs by all users started')
	user_jobs = get_jobs_dict_by_user()
	print('getting jobs by all users completed')

	print("time taken for getting jobs by users",time.time()-fetch_jobs)

	fetch_log_data = time.time()
	user_emails = {}
	print("{}|{}|{}|{}|{}|{}|{}|{}|{}|{}".format("user", "fragmentID", "assigned_ts", "audio_url", "no_of_chars", "no_of_keystrokes", "no_of_inserts", "no_of_deletes", "elapsed_time", "deliverable"), file = sample)
	print('Process jobs for each user started')
	for userID in user_jobs:
		duplicates = set()
		jobs = user_jobs[userID]
		for data in jobs:

			if not data:
				print("{}|{}|{}|{}|{}|{}|{}|{}|{}|{}".format(userID, "fragmentID", "", "No data found for given id", "", "", "", "", "", ""), file = sample)
				continue

			else:
				# print("data dictionary ", data)
				deliverable = data['deliverable']
				assignment_ts = data['assignment_ts'] if 'assignment_ts' in data else None
				user = data['user']
				if user in user_emails:
					user = user_emails[user]
				else:
					userID = user.split("/")[2]
					email = get_user_email(userID)
					user_emails[user] = email
					user = email
				fragment = data['fragment']
				fragmentID = fragment.split("/")[2]
				fragment = None
				if fragmentID in fragment_audio_urls:
					# print("fragment_audio_urls-->", fragment_audio_urls)
					# print("fragment_audio_urls[fragment]-->", fragment_audio_urls[fragmentID])
					fragment = fragment_audio_urls[fragmentID]

				log_data = data['log_data']
				sum_of_time_deltas = 0
				no_of_keystrokes = 0
				no_of_inserts = 0
				no_of_deletes = 0
				if log_data:
					sum_of_time_deltas, no_of_keystrokes, no_of_inserts, no_of_deletes, processed_log_data = sum_of_time_deltas_for_keystroke(
						log_data, duplicates)
				print("time taken to collect the log data",time.time()-fetch_log_data)

				write_to_excel = time.time()
				no_of_chars = 0
				if deliverable:
					no_of_chars = len(deliverable)
				print("writing logdata to user_fragment_log_data/{}_{}.txt started".format(user, fragmentID))
				write_to_file("user_fragment_log_data/{}_{}.txt".format(user, fragmentID), processed_log_data)
				print("writing logdata to user_fragment_log_data/{}_{}.txt completed".format(user, fragmentID))

				print("{}|{}|{}|{}|{}|{}|{}|{}|{}|{}".format(user, fragmentID, assignment_ts, fragment, no_of_chars, no_of_keystrokes, no_of_inserts, no_of_deletes, sum_of_time_deltas, deliverable), file=sample)
				# break
		# break
	print('Process jobs for each user completed')
	print("time taken to write data into excel", time.time()-write_to_excel)

	sample.close()