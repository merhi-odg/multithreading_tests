#fastscore.slot.0: in-use
#fastscore.slot.1: in-use

import threading
import time
import pickle
import pandas as pd
import os
import glob


def thread_function():
	while True:
		global df_global
		file_not_found = False

		# read file
		try:
			infile = sorted(glob.glob('df_incremental*.pkl'))[0]
		except IndexError:
			infile = ''
			file_not_found = True

		if file_not_found:
			df_incremental = pd.DataFrame()
		else:
			df_incremental = pickle.load(open(infile, 'rb'))
			os.remove(infile)

		# Set the lock before updating DF
		lock.acquire()

		# get small pandas dataframe to add to big one
		df_global = pd.concat(
			[df_global, df_incremental]
		).reset_index(drop=True, inplace=False)
		
		time.sleep(0.1)

		# Release the lock after DF update
		lock.release()

		print('\nAdded file ' + infile)
		time.sleep(10)

#modelop.init	
def begin():

	global df_global, lock

	df_global = pickle.load(open('df_global.pkl', 'rb'))
	timer_thread = threading.Thread(
		target=thread_function,
		# args=(1, ),
		daemon=True
	)
	timer_thread.start()

	# threading lock
	lock = threading.Lock()

	pass

#modelop.score
def action(data):

	global df_global, lock

	lock.acquire()

	output = {
		'Max of A': max(df_global.A),
		'Sum of B': df_global.B.sum()
	}

	lock.release()

	print(output)
	yield output

#modelop.metrics
def metrics(data):
	return {
		"f1": .9, "AUC": .8
	}
