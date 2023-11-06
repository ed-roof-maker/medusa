
# This is a template for using the mp.Medusa multi worker super class
# In this example we just generate a bunch of random numbers, add only the batched numbers, and print them 
#

# MultiProcess module configurations
import medusa as mp
mp.PROCESSES = 10				# Default: 10
mp.PYMOD_NAME = 'BatchCalculateJob'
#mp.RESULTS_FNAME = 'results-' + mp.PYMOD_NAME	# Default: results-worker-template
#mp.RESULTS_DIR = '/temp'	# Default: /temp 
#mp.CONFIG_FNAME = mp.PYMOD_NAME + '.conf'	# Default: worker-template.conf

TEMP = mp.TEMP

# Batch Calculator modules and settings
import os

WORKER_PARENT_DATA_TEST='I am a global string.'


class BatchCalculateJob(mp.Medusa):
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
      wname = TEMP + '/worker.%s' % pid
      print('Starting worker file %s...' % wname)

      # We open a file and can write our results here for later joining
      afile = open(wname, 'a')
      prev_row = 0
      row_result = 0
      for row in job_data:
         # Process rows
         print('%s - Data %s ' % (wname, row))
         row_result = (row + prev_row)
         prev_row = row
      afile.write(str(row_result) + ',\n')
      print('%s - Data %s ' % (wname, WORKER_PARENT_DATA_TEST)) 
      afile.close()

      print('Ended  worker file %s' % wname)
      return (pid, )

   def run_post(self): 
      print('*** END OF JOB - Workers finished processing.')
      print('*** Results file is located here ' + self.results_file)
      # Do what ever you want with the results here
      # After this method, a clean up of temp will be performed.
      with open(self.results_file) as r:
         print('*** RESULT - ' + r.read())


# If this module is run as a script then we run the job with 4 processes
#
if __name__ == '__main__':
   # A list of data you want to process by debatching and parallel processing
   # The below list will be split and be given to multiple workers
   job_data=[
      2,3245,123,12,4,756,45,34,23,90,32,67,12,6,87,1,34,4,1234,3
   ]
   BatchCalculateJob().orchestrate(processes=4, job_index=job_data)





