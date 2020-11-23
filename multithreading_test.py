# fastscore.schema.0: input_schema.avsc
# fastscore.schema.1: output_schema.avsc

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

	# add a print to indicate when a new file is found and read

	while True:
		global df_global
		file_not_found = False

		# read file
		try:
			infile = sorted(glob.glob('df_incremental_*.pkl'))[0]

		# If file not found
		except IndexError as index_err:
			print("File note found: ", index_err)
			file_not_found = True

		if file_not_found:
			# Nothing to append to global DF
			df_incremental = pd.DataFrame()
            
		else:
			# If file found, load its contents (DataFrame), then remove it
			df_incremental = pickle.load(open(infile, 'rb'))
			os.remove(infile)

			print("\nfile found: " + infile)

		# Set the lock before updating DF
		lock.acquire()

		# get small pandas dataframe to add to global one
		try:
			df_global = pd.concat(
				[df_global, df_incremental]
			).reset_index(drop=True, inplace=False)

		except Exception as thread_err:
			print("Try clause in thread function failed: ", thread_err)

		finally:
			# Release the lock after DF update
			lock.release()

		# Set the period of file opening (some divisor of 5 min)
		time.sleep(2)


# modelop.init
def begin():
	"""
	A function to define global variable
	:return: Initialized global DataFrame and threading lock
	"""

	global df_global, lock

	# Initialize global DataFrame from file
	df_global = pickle.load(open('df_global.pkl', 'rb'))

	# Initialize a threading lock (before starting thread below)
	lock = threading.Lock()

	# Set and start the thread
	timer_thread = threading.Thread(
		target=thread_function,
		# args=(1, ),
		daemon=True  # as soon as main thread completes, this thread is taken down
	)

	# Try setting daemon=False and see how engine behaves
	timer_thread.start()

	pass


# modelop.score
def action(data: int):
	"""
	A function to score an input
	:param data: Irrelevant (unused) in this case
	:return: metrics on global DF: last file added, sum of column B
	"""

	global df_global, lock

	# Set thread locking in case DF is being accessed here while
	# simultaneously being updated by thread_function
	lock.acquire()
    
	try:
		output = {
			'Last_file_added': df_global.filename.iloc[-1],
			'Max_of_column_B': max(df_global.file_number)
		}

	except:
		print("Try clause in action failed!")
		output = {
			'Last_file_added': "ERROR",
			'Max_of_column_B': None
		}

	finally:
		# Release the lock once done with DF
		lock.release()

	print(output)
	return output
	# yield output


# modelop.metrics
def metrics(data):
	"""
	A function to return some metrics on input data
	:param data: data in test_data.json (unused by the function)
	:return: fixed dictionary
	"""

	yield {"f1": 0.9, "AUC": 0.8}
