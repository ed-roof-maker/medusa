#
# A inheritable super class enabling a multi worker model.
# Not dependent on pandas and numpy for data structure usage.
#
# Adapted from project - github.com/ganis/minirepo.git on 12 OCT 2020
#
import sys
import os
import time
import logging
import tempfile
# import json
import yaml
import glob
import traceback
import rlab_common as rlc
import multiprocessing as mp

DEBUG = False
VERBOSE = False

# Temp folder, is needed for temporary files created by parallel processes
TEMP = tempfile.mkdtemp()

# Number of processes to run in parallel
PROCESSES = 1
WORKER_TIMEOUT = 3600   # In Seconds - 1 hour - any more means you are too course grained!
WORKER_EXIT_ON_TIMEOUT = True
TEMP = '/tmp'
PYMOD_NAME = 'worker-template'
RESULTS_FNAME = 'results-' + PYMOD_NAME + '-' + rlc.filesys().whoami()
RESULTS_DIR = TEMP
CONFIG_FNAME = PYMOD_NAME + '.yaml'

POOL = None


def create_pool(processes):
   try:
      if DEBUG:
         print('MEDUSA: DEBUG: Creating pool with %s processes...' % processes)
      global POOL
      POOL = mp.Pool(int(processes))
      if DEBUG:
         print('MEDUSA: DEBUG: Creating pool...DONE')
   except Exception as e:
      print(
         'MEDUSA: ERROR: Exception while creating global POOL, ERR - \n' +
         str(traceback.format_exc()) + '\n' + str(e)
      )
      sys.exit(1)


class Medusa(object):
   def __init__(self):
      self.__pids = None
      self.__chunks = None
      self.__pool = None
      self.__start = None
      self.results_file = None
      self.TEMP = TEMP
      self.PYMOD_NAME = PYMOD_NAME
      self.RESULTS_FNAME = RESULTS_FNAME
      self.RESULTS_DIR = RESULTS_DIR
      self.CONFIG_FNAME = CONFIG_FNAME
      self.PROCESSES = PROCESSES
      self.WORKER_TIMEOUT = WORKER_TIMEOUT
      self.WORKER_EXIT_ON_TIMEOUT = WORKER_EXIT_ON_TIMEOUT

   def get_chunks(self, seq, num):
      """
         Split seq in chunks of size num, used to divide tasks for workers
      """
      if DEBUG:
         print('MEDUSA: DEBUG: Getting chunks...')
      avg = len(seq) / float(num)
      out = []
      last = 0.0
      tmp_arr = rlc.shuffle_list(seq)
      while last < len(seq):
         i = tmp_arr[int(last):int(last + avg)]
         out.append(i)
         last += avg
      if DEBUG:
         print('MEDUSA: DEBUG: Getting chunks...DONE')
      return out

   def worker_custom(self, data_list):
      """
          MANDATORY: Override me in a subclass
      """
      pid = os.getpid()
      wname = self.TEMP + '/worker.%s' % pid
      print('MEDUSA: Starting worker file %s...' % wname)
      afile = open(wname, 'a')

      for row in data_list:
         # Process rows
         print(row)

      afile.close()
      return (pid, )

   def get_config(self):
      """
         Worker configuration is stored as json in unix user home directory.
         Change to /etc if you want
      """
      # Get config values - 1st from home dir, 2nd from interactive stdin
      config_file = os.path.expanduser("~/." + str(self.CONFIG_FNAME))
      processes = 4
      try:
         config = yaml.safe_load(open(config_file))
      except Exception:
         newprocesses = input('MEDUSA: Number of processes [%s]: ' % processes)
         if newprocesses:
            processes = newprocesses
         config = {}
         config["processes"] = processes
         with open(config_file, 'w') as w:
            yaml.dump(config, w, default_flow_style=False, allow_unicode=True)

      # Print out config values
      for c in sorted(config):
         print('%-15s = %s' % (c, config[c]))

      print('MEDUSA: Using config file %s ' % config_file)
      return config

   def save_json(self, pids):
      try:
         # Concatenate output from each worker
         db = self.RESULTS_DIR + '/' + self.RESULTS_FNAME + '.json'
         with open(db, 'w') as w:
            w.write('[\n')
            for pid in pids:
               wfile = self.TEMP + '/worker.%s' % pid
               try:
                  with open(wfile) as r:
                     w.write(r.read())
                  os.remove(wfile)
                  print('MEDUSA: INFO: deleted: %s' % wfile)
               except Exception:
                  continue

         # Remove tailing comma, remove last 2 characters (',\n')
         with open(db, 'rb+') as w:
            w.seek(-2, os.SEEK_END)
            w.truncate()

         # Complete json list
         with open(db, 'a') as a:
            a.write('\n]\n')

      except Exception as e:
         print(
            'MEDUSA: ERROR: Exception while joining and saving all worker outputs, ERR - \n' +
            str(traceback.format_exc()) + '\n' + str(e)
         )
         sys.exit(1)

   def configure(self, processes):
      print('MEDUSA: /******** ' + self.PYMOD_NAME + ' - Multi Process Worker Model ********/')

      # Get and set configuraton values
      config = self.get_config()
      self.ENV_CONFIG = config
      self.PROCESSES = config["processes"]
      processes = self.PROCESSES
      # Overwrite config with parameter if specified
      if processes:
         self.PROCESSES = processes
         print('MEDUSA: Overridden:\nprocesses       = %s' % processes)

      assert self.PROCESSES

      logging.basicConfig(
         level=logging.WARNING,
         format="%(asctime)s:%(levelname)s: %(message)s"
      )

   def prepare(self, job_index=[]):
      print('MEDUSA: Preparing ' + self.PYMOD_NAME + '...')
      self.__start = time.time()

      # Prepare
      self.results_file = self.RESULTS_DIR + '/' + self.RESULTS_FNAME + '.json'
      names = job_index
      self.JOB_COUNT = names.__len__()
      create_pool(self.PROCESSES)
      self.__chunks = list(self.get_chunks(names, self.PROCESSES))
      print('MEDUSA: Preparing ' + self.PYMOD_NAME + '...DONE')

   def run(self):
      # Run in parallel
      # (pids)
      results = None
      try:
         print('MEDUSA: INFO: Running in parallel with %s process...' % self.PROCESSES)
         # Batch chunks of self.PROCESSES, so that Pool doesnt error at the end.
         if self.__chunks.__len__() == 0:
            print('MEDUSA: ERROR: CPU chunks could not be generated per CPU core.')
            sys.exit(1)
         size = self.__chunks[0].__len__()
         if size >= 100:
            batch_size = 100
         else:
            batch_size = None
         batch = self.__chunks
         results = []
         if batch_size:
            print('MEDUSA: INFO: Batch size is larger than 100. Performing chunking. OK.')
            # LARGE JOBS > 100 per core: 100 jobs per CPU core then store results, repeat
            cpu_batch_size = float(size) / float(batch_size)
            cpu_batch = []
            all_cpus = []
            for cpu_chunk in self.__chunks:
               job_batch = []
               for job in cpu_chunk:
                  if job_batch.__len__() > cpu_batch_size:
                     cpu_batch.append(job_batch)
                     job_batch = []
                  job_batch.append(job)
               if job_batch.__len__() > 0:
                  cpu_batch.append(job_batch)
               all_cpus.append(cpu_batch)
               cpu_batch = []
            print(
               'MEDUSA: INFO: ' +
               'TotalJobCount~=%s, CPUBatches=%s, BatchSize=%s, ExitOnWorkerTimeout=%s' % (
                  self.JOB_COUNT, all_cpus[0].__len__(), batch_size, self.WORKER_EXIT_ON_TIMEOUT
               )
            )
            if DEBUG:
               print('MEDUSA: DEBUG: all_cpus=%s' % all_cpus)
               print('MEDUSA: DEBUG: __chunks=%s' % self.__chunks)
            bad_batch_count = 0
            for cpu_batch in all_cpus:
               result = []
               batch = cpu_batch
               if DEBUG:
                  print('DEBUG: batch=%s' % batch)
               result = POOL.map_async(
                  self.worker_custom, batch
               ).get(timeout=self.WORKER_TIMEOUT)
               if not result:
                  print('MEDUSA: WARNING: This batch was empty. No results will be recorded.')
                  bad_batch_count = bad_batch_count + 1
               elif type(result) == list:
                  results.extend(result)
               else:
                  print('MEDUSA: ERROR: Fatal logical error in multi worker execution.')
                  sys.exit(1)
               print(
                  'MEDUSA: INFO: Batched Worker Results - count=%s, last 10 = %s' % (
                     results.__len__(), results[-10:]
                  )
               )
               if bad_batch_count > 1:
                  print(
                     'MEDUSA: ERROR: More than one batches have failed resulting in result loss. ' +
                     'Fatal error.'
                  )
                  sys.exit(1)
         else:
            # SMALL JOBS < 100 per core
            print(
               'MEDUSA: INFO: Batch size is less than 100. Single POOL execution for all' +
               ' workers. OK.'
            )
            results = POOL.map_async(
               self.worker_custom, batch
            ).get(timeout=self.WORKER_TIMEOUT)
            if not results:
               print('MEDUSA: ERROR: Pool.map_async() returned none. Exiting.')
               sys.exit(1)
         if results.__len__() > 0:
            self.__pids = [r[0] for r in results]
         else:
            print('MEDUSA: WARNING: No results created.')
            self.__pids = []
         print('MEDUSA: INFO: Running in parallel...DONE')

      except mp.context.TimeoutError:
         print(
            'MEUDSA: ERROR: Timeout while running in parallel. Timeout in secs - %s ' % (
               self.WORKER_TIMEOUT,
            )
         )
         if results:
            self.__pids = [r[0] for r in results]
         else:
            print('MEDUSA: WARNING: No results created.')
            self.__pids = []
         if self.WORKER_EXIT_ON_TIMEOUT:
            sys.exit(1)

      except Exception as e:
         if type(results) == list:
            print('MEDUSA: DEBUG: results=%s' % (results,))
         print(
            'MEDUSA: ERROR: Exception while running in parallel, ERR - \n' +
            str(traceback.format_exc()) + '\n' + str(e)
         )
         sys.exit(1)

   def save(self):
      # Store full list of results in json format for later analysis
      print('MEDUSA: INFO: Saving json result...')
      self.save_json(self.__pids)
      print('MEDUSA: INFO: Saving json result...DONE')

   def clean(self):
      # Cleanup
      for f in glob.glob(self.TEMP + '/worker*'):
         for pid in self.__pids:
            if str(pid) in f:
               os.remove(f)
      logging.warning('MEDUSA: INFO: Temp folder files deleted. Folder: %s' % self.TEMP)

      # Print summary
      print('MEDUSA: INFO: Summary:')
      self.__end = time.time() - self.__start
      duration = rlc.seconds_human(self.__end)
      print('MEDUSA: INFO: Time: %s' % duration)
      logging.info('MEDUSA: INFO: ' + self.PYMOD_NAME + 'completed.')

   def run_pre(self):
      pass

   def run_post(self):
      pass

   def configure_post(self):
      pass

   def orchestrate(self, processes=None, job_index=[]):
      """Orchestrate the parallel job.
      """
      try:
         self.configure(processes)
         self.configure_post()

         self.prepare(job_index)

         self.run_pre()
         self.run()
         self.save()
         self.run_post()

         self.clean()

      except Exception as e:
         print(
            'MEDUSA: ERROR: Exception while orchestrating, ERR - \n' +
            str(traceback.format_exc()) + '\n' + str(e)
         )
         sys.exit(1)


class MedusaTestJob(Medusa):
   def __init__(self):
      # We want the super class's initialisation method to be called
      super().__init__()

   def configure_post(self):
      print('*** Process configuration is completed.')

   def run_pre(self):
      print('*** START OF JOB - We are now ready to run our parallel orchestration.')

   def worker_custom(self, job_data):
      """ Mandatory overriden procedure.
          We override this for custom instance processing.
      """
      pid = os.getpid()
      wname = self.TEMP + '/worker.%s' % pid
      print('Starting worker file %s...' % wname)

      # We open a file and can write our results here for later joining
      # We run an enrichment unit of work as a separate python3 daemon process so that pandas
      # has its own memory space reducing dataframe merging errors and malformation
      afile = open(wname, 'a')
      row_result = None
      print('DEBUG: job_data=%s' % job_data)
      for row in job_data:
         try:
            # Process rows
            print('%s - Processing Orchestration %s ' % (wname, row))
            row_result = None
            row_result = 2 + int(row)  # Add two
         except Exception as e:
            print(
               'Exception for job ' + row + ', ERR - \n' + str(traceback.format_exc()) + '\n' +
               str(e)
            )
            row_result = 'ERROR: Could not enrich job ' + row
            sys.exit(1)

         if row_result == '' or row_result is None:
            row_result = 'ERROR: Could not enrich job ' + row

         # Write job result for later analysis
         afile.write(str(row_result) + ',\n')
         print('%s - Job Result - \n %s ' % (wname, row_result))

      afile.close()
      print('Ended  worker file %s' % wname)
      return (pid, )

   def run_post(self):
      print('*** END OF JOB - Workers finished processing.')
      print('*** Results file is located here ' + self.results_file)
      # Do what ever you want with the results here
      # After this method, a clean up of temp will be performed.
      with open(self.results_file) as r:
         r = eval(r.read())
         print('*** RESULT - ' + str(r))
         self.TEST_RESULT = int(r[0])
         for i in r[1:]:
            # Deduct values via product
            self.TEST_RESULT = int(self.TEST_RESULT) + int(i)


def test():
   """Test medusa parallel worker abstraction object class.
   """
   #
   # =============================================================================================
   global DEBUG
   global VERBOSE
   DEBUG = True
   VERBOSE = True
   # Batch modules and settings
   try:
      import rlab_common as rlcom
   except Exception:
      print('WARNING: Test case requires rlab_common module. Skipping tests.')
      sys.exit(0)
   # Run Test Medusa Abstraction
   test_obj = MedusaTestJob()
   # We rely on ~/.worker-template.conf to adjust concurrent jobs without rebuilding the wheel
   # We default to 1 so that the user session config can be modified
   test_obj.TEMP = TEMP
   test_obj.PROCESSES = 4  # Default: 10 | Set to none to let the json conf file control it
   test_obj.WORKER_TIMEOUT = 72000
   test_obj.WORKER_EXIT_ON_TIMEOUT = True  # TODO: In production this should be True
   test_obj.PYMOD_NAME = 'MedusaTestJob'
   test_obj.RESULTS_FNAME = 'results-' + test_obj.PYMOD_NAME + '-' + rlcom.filesys().whoami()
   test_obj.RESULTS_DIR = '/tmp'  # Default: /tmp
   test_obj.CONFIG_FNAME = 'WorkerMedusa-' + test_obj.PYMOD_NAME + '.yaml'
   #
   # =============================================================================================

   # Small Size Test - Even
   print('Testing MedusaTestJob - SMALL JOB - Even.......')
   job_data = [x for x in range(0, 10)]  # Test data per unit of work
   expected = 0
   batch_add_two = []
   for i in job_data:
      batch_add_two.append(i + 2)
   for i in batch_add_two:
      expected = expected + i    # Expected result
   print('**** Number of jobs ' + str(job_data.__len__()))
   r = test_obj
   r.orchestrate(processes=4, job_index=job_data)
   result = r.TEST_RESULT
   print('TEST > DEBUG: MedusaTestJob.TEST_RESULT=%s' % (result,))
   if expected != result:
      print('TEST > ERROR: MedusaTestJob> expected=%s, result=%s' % (expected, result))
      print('Testing MedusaTestJob - SMALL JOB - Even.......FAILED')
      sys.exit(1)
   else:
      print('Testing MedusaTestJob - SMALL JOB - Even.......OK')

   # Small Size Test - Odd
   print('Testing MedusaTestJob - SMALL JOB - Odd.......')
   job_data = [x for x in range(0, 51)]  # Test data per unit of work
   expected = 0
   batch_add_two = []
   for i in job_data:
      batch_add_two.append(i + 2)
   for i in batch_add_two:
      expected = expected + i    # Expected result
   print('**** Number of jobs ' + str(job_data.__len__()))
   r = test_obj
   r.orchestrate(processes=4, job_index=job_data)
   result = r.TEST_RESULT
   print('TEST > DEBUG: MedusaTestJob.TEST_RESULT=%s' % (result,))
   if expected != result:
      print('TEST > ERROR: MedusaTestJob> expected=%s, result=%s' % (expected, result))
      print('Testing MedusaTestJob - SMALL JOB - Odd.......FAILED')
      sys.exit(1)
   else:
      print('Testing MedusaTestJob - SMALL JOB - Odd.......OK')

   # Large Size Test - Even
   print('Testing MedusaTestJob - LARGE JOB - Even.......')
   job_data = [x for x in range(0, 1000)]  # Test data per unit of work
   expected = 0
   batch_add_two = []
   for i in job_data:
      batch_add_two.append(i + 2)
   for i in batch_add_two:
      expected = expected + i    # Expected result
   print('**** Number of jobs ' + str(job_data.__len__()))
   r = test_obj
   r.orchestrate(processes=4, job_index=job_data)
   result = r.TEST_RESULT
   print('TEST > DEBUG: MedusaTestJob.TEST_RESULT=%s' % (result,))
   if expected != result:
      print('TEST > ERROR: MedusaTestJob> expected=%s, result=%s' % (expected, result))
      print('Testing MedusaTestJob - LARGE JOB - Even.......FAILED')
      sys.exit(1)
   else:
      print('Testing MedusaTestJob - LARGE JOB - Even.......OK')

   # Large Size Test - Even
   print('Testing MedusaTestJob - LARGE JOB - Odd.......')
   job_data = [x for x in range(0, 2001)]  # Test data per unit of work
   expected = 0
   batch_add_two = []
   for i in job_data:
      batch_add_two.append(i + 2)
   for i in batch_add_two:
      expected = expected + i    # Expected result
   print('**** Number of jobs ' + str(job_data.__len__()))
   r = test_obj
   r.orchestrate(processes=4, job_index=job_data)
   result = r.TEST_RESULT
   print('TEST > DEBUG: MedusaTestJob.TEST_RESULT=%s' % (result,))
   if expected != result:
      print('TEST > ERROR: MedusaTestJob> expected=%s, result=%s' % (expected, result))
      print('Testing MedusaTestJob - LARGE JOB - Odd.......FAILED')
      sys.exit(1)
   else:
      print('Testing MedusaTestJob - LARGE JOB - Odd.......OK')


if __name__ == '__main__':
   test()
