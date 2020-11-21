#fastscore.schema.0: input_schema
#fastscore.schema.1: output_schema

import threading
import time
import pickle
import pandas as pd
import os
import glob


def thread_function():
	"""
	Defines a thread to open a file, read its content, delete file,
	append content to pre-defined global DataFrame. Appending to the global DF
	is done in a locked thread for safety. File opening is done on a schedule.

	:return: augmented global DataFrame
	"""

	while True:
		global df_global
		file_not_found = False

		# read file
		try:
			infile = sorted(glob.glob('df_incremental*.pkl'))[0]
		# If file not found
		except IndexError:
			infile = ''
			file_not_found = True

		if file_not_found:
			# Nothing to append to global DF
			df_incremental = pd.DataFrame()
		else:
			# If file found, load its contents (DataFrame), then remove it
			df_incremental = pickle.load(open(infile, 'rb'))
			os.remove(infile)

		# Set the lock before updating DF
		lock.acquire()

		# get small pandas dataframe to add to global one
		df_global = pd.concat(
			[df_global, df_incremental]
		).reset_index(drop=True, inplace=False)

		# add some extra time to the locked thread for safetu
		time.sleep(0.1)

		# Release the lock after DF update
		lock.release()

		print('\nAdded file ' + infile)

		# Set the period of file opening
		time.sleep(15)


#modelop.init
def begin():
	"""
	A function to define global variable
	:return: Initialized global DataFrame and threading lock
	"""

	global df_global, lock

	# Initialize global DataFrame from file
	df_global = pickle.load(open('df_global.pkl', 'rb'))

	# Set and start the thread
	timer_thread = threading.Thread(
		target=thread_function,
		# args=(1, ),
		daemon=True
	)
	timer_thread.start()

	# Initialize a threading lock
	lock = threading.Lock()

	pass


#modelop.score
def action(data):
	"""
	A function to score an input
	:param data: Irrelevant (unused) in this case
	:return: metrics on global DF: last file added, sum of column B
	"""

	global df_global, lock

	# Set thread locking in case DF is being accessed here while
	# simultaneously being updated by thread_function
	lock.acquire()

	output = {
		'Last file added': max(df_global.A),
		'Sum of column B': df_global.B.sum()
	}

	# Release the lock once done with DF
	lock.release()

	print(output)
	# return output
	yield output


#modelop.metrics
def metrics(data):
	"""
	A function to return some metrics on input data
	:param data: data in test_data.json (unused by the function)
	:return: fixed dictionary
	"""

	yield {
		"f1": 0.9, "AUC": 0.8
	}
