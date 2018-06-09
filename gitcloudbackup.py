import os
import zlib
import zipfile
import re
import tempfile
from google.cloud import storage
from absl import app
from absl import flags
from absl import logging

FLAGS = flags.FLAGS
flags.DEFINE_string('bucket',None,'Bucket to use on Google Cloud Storage')
flags.DEFINE_string('keyfile',None,'Google Cloud keyfile')
flags.DEFINE_string('filedir',None,'Directory to zip when changes are found')
flags.DEFINE_string('zipname',None,'Name of zip file(without extensions ie just repos not repos.zip')

def main(argv):
  logging.set_verbosity(logging.INFO)
  zip_filename = FLAGS.zipname
  zip_directory = FLAGS.filedir
  client = storage.Client.from_service_account_json(FLAGS.keyfile)
  bucket = client.get_bucket(FLAGS.bucket)
  if AreLocalFilesNewer(bucket, zip_directory):
    UploadZippedDirectory(bucket,zip_directory,zip_filename)
    logging.info("Files uploaded to cloud storage")
  else:
    logging.info("No local file changes")

def UploadZippedDirectory(bucket,zipdir,zipname):
  """ This method zips a directory and uploads it to a
  google cloud storage bucket
  Parameters
  ----------
  bucket : google.cloud.storage.bucket
    The bucket to upload to
  zipdir : str
    The directory to zip
  zipname : str
    The name of the zip file(Only the left most portions, ie
    repos.zip would just come in as repos)
  """
  wd = tempfile.gettempdir()
  os.chdir(wd)
  time_str = str(FindNewestTimeLocal(zipdir))
  zipf = '.'.join([zipname,time_str,'zip'])
  ZipDirectory(zipdir,zipf)
  blob = bucket.blob(zipf)
  blob.upload_from_filename(zipf)

def AreLocalFilesNewer(bucket,filedir):
  """ This method compares the most recent local file timestamp
  to the most recent cloud timestamp and returns True if the 
  local timestamps are newer.
  Paramters
  --------
  bucket : google.cloud.storage.bucket
    bucket to check
  filedir - str
    local directory to check
  Returns
  -------
  bool
    True if local filers are newer.
  """
  changes = False
  if FindNewestTimeLocal(filedir) > FindNewestTimeCloud(bucket):
    changes = True
  return changes

def GetTimeFromFilename(filename):
  """ This method takes a filename and parses out our timestamp.
  Parameters
  ---------
  filename : str
    filename to search
  Returns
  -------
  float
    A float representing the time since epoch.  If 0.0 then no timestamp found
  """
  regexc = r'\d+\.\d+'
  try:
    ftime = float(re.findall(regexc,filename)[0])
  except IndexError:
    ftime = 0.0
  return ftime

def ZipDirectory(dirz, zipf): # Further research suggests importing shutil and using shutil.make_archive()... lolol
  """ This method takes a directory and zips it
  Parameters
  ----------
  dirz : str
    directory to zip
  zipf : str
    filename for the zip file
  """
  with zipfile.ZipFile(zipf, 'w',zipfile.ZIP_DEFLATED) as myzip:
    for dirpath,_,filenames in os.walk(dirz):
      for f in filenames:
        myzip.write(os.path.join(dirpath,f))
    

def FindNewestTimeLocal(dirc):
  """ This method recursively searches a directory for files, returning
  the time since epoch of the newest created file.
  Parameters
  ----------
  dirc : str
    The directory to check
  Returns
  -------
  float
    A float representing the newest creation time since epoch
  """
  ftime = 0
  for dirpath,_,filenames in os.walk(dirc):
    for f in filenames:
      cfile = os.path.abspath(os.path.join(dirpath,f))
      file_time = os.path.getmtime(cfile)
      logging.info("Local File Timestamp: " + str(ftime))
      if file_time > ftime:
        ftime = file_time
  return ftime

def FindNewestTimeCloud(bucket):
  """ This method recursively searches a bucket for files, returning
  the time encoded in a file name.
  Parameters
  ----------
  bucket : google.cloud.storage.bucket
    The bucket to check
  Returns
  -------
  float
    A float representing the newest creation time since epoch.
    A value of 0.0 means no timestamp found
  """
  ftime = 0.0
  for blob in bucket.list_blobs():
    file_time = float(GetTimeFromFilename(blob.name))
    logging.info("Cloud File Timestamp: " + str(ftime))
    if file_time > ftime:
      ftime = file_time
  return ftime

if __name__ =="__main__":
  app.run(main)