#!/usr/bin/env python
#
# THIS IS A REFERENCE FILE ONLY
#
# We adapted minirepo and created a resusable subclass called medusa.py
#
# No dependent on pandas and numpy for data structure usage.
# Adapted from project - github.com/ganis/minirepo.git on 12 OCT 2020
#

import sys
import os
import time
import shutil
import random
import logging
import tempfile
import multiprocessing as mp

# logging.basicConfig(level=logging.INFO)

# Temp folder, is needed for temporary files created by parallel processes
TEMP = tempfile.mkdtemp()

# Number of processes to run in parallel
PROCESSES = 10

PYMOD_NAME   = 'worker-template'
RESULTS_FNAME = 'results-' + PYMOD_NAME
RESULTS_DIR   = '/home/' + os.getlogin()
CONFIG_FNAME = PYMOD_NAME + '.conf'


def get_chunks(seq, num):
   """
      split seq in chunks of size num, used to divide tasks for workers
   """
   avg = len(seq)/float(num)
   out = []
   last = 0.0
   while last < len(seq):
      out.append(seq[int(last):int(last + avg)])
      last += avg
   return out


def worker(data_list):
   """
   """
   pid = os.getpid()
   wname = TEMP + '/worker.%s' % pid
   print('starting worker file %s...' % wname)
   afile = open(wname, 'a')
   i = 0

   for row in data_list:   
      # Process rows
      print(row) 
       
   afile.close()
   return (pid)


def get_config():
   """
      Worker configuration is stored as json in unix user home directory.
      Change to /etc if you want
   """
   # Get config values - 1st from home dir, 2nd from interactive stdin
   config_file = os.path.expanduser("~/." + str(CONFIG_FNAME))
   processes = 10
   try:
      config    = json.load(open(config_file))
   except:
      newprocesses = input('Number of processes [%s]: ' % processes)
      if newprocesses:
         processes = newprocesses
      config = {}
      config["processes"]         = processes
      with open(config_file, 'w') as w:
         json.dump(config, w, indent=2)

   # Print out config values
   for c in sorted(config):
      print('%-15s = %s' % (c,config[c]))

   print('Using config file %s ' % config_file)

   return config



def save_json(pids):
   # Concatenate output from each worker
   db = RESULTS_DIR + '/' + RESULTS_FNAME  +'.json'
   with open(db,'w') as w:
      w.write('[\n')
      for pid in pids:
         wfile = TEMP + '/worker.%s' % pid
         with open(wfile) as r:
            w.write(r.read())
         os.remove(wfile)
         print('deleted: %s' % wfile)
   
   # Remove tailing comma, remove last 2 characters (',\n')
   with open(db, 'rb+') as w:
      w.seek(-2, os.SEEK_END)
      w.truncate()

   # Complete json list
   with open(db, 'a') as a:
      a.write('\n]\n')


def main(processes=0):
   print('/******** ' + PYMOD_NAME + ' - Multi Process Worker Model ********/')
   
   # Get and set configuraton values
   global PROCESES
   config          = get_config()
   PROCESSES       = config["processes"]
   
   # Overwrite config with parameter if specified
   if processes:
      PROCESSES = processes
      print('Overridden:\nprocesses       = %s' % processes)

   assert PROCESSES

   logging.basicConfig(
      level=logging.WARNING, 
      format="%(asctime)s:%(levelname)s: %(message)s")
   
   logging.info('Starting ' + PYMOD_NAME + '...')
   start = time.time()   

   # Prepare
   names = get_names()
   pool = mp.Pool(int(PROCESSES))
   random.shuffle(names)
   chunks = list(get_chunks(names, PROCESSES))
   
   # Run in parallel
   # (pids)
   results = pool.map_async(worker, chunks).get(timeout=99999)
   pids             = [r[0] for r in results]

   # Store full list of results in json format for later analysis
   save_json(pids)

   # Cleanup
   shutil.rmtree(TEMP)
   logging.warning('Temp folder deleted: %s' % TEMP)

   # Print summary
   print('Summary:')
   print('time:', (time.time()-start))

   logging.info(PYMOD_NAME + 'completed.')
   


if __name__ == '__main__':
   main()
