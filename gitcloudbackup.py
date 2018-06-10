import os
import zlib
import zipfile
import re
import tempfile
import json
import requests
import platform
from google.cloud import storage
from absl import app
from absl import flags
from absl import logging

FLAGS = flags.FLAGS
flags.DEFINE_string('config',None,"Location of a JSON formatted configuration file")
flags.DEFINE_bool('debug',False,"Flag to debug application")

def main(argv):
  if FLAGS.debug:
    logging.set_verbosity(logging.DEBUG)
  else:
    logging.set_verbosity(logging.INFO)
  config = LoadConfigFile(FLAGS.config)
  zfname = config['zipname']
  zdir = config['filedir']
  keyf = config['keyfile']
  bname = config['bucket']
  saurl = config['saurl']
  client = storage.Client.from_service_account_json(keyf)
  bucket = client.get_bucket(bname)
  if AreLocalFilesNewer(bucket, zdir):
    UploadZippedDirectory(bucket,zdir,zfname)
    SendStackMessage(saurl, "Files uploaded to cloud starge")
    logging.info("Files uploaded to cloud storage")
  else:
    logging.info("No local file changes")

def SendStackMessage(saurl, text):
  """ This method takes a Stack authenticated url and sends the text to it.
  Parameters
  ----------
  saurl : str
    The authenticated URL
  text : str
    The text to send
  """
  PAYLOAD = {"text": text,}
  requests.post(saurl, json=PAYLOAD)

def LoadConfigFile(jsonf=None):
  """ This method takes a file name and loads a JSON config from it.  
  If the value of the file is None then it tries to load a config.json
  file located in the same path as the program.

  Paramters:
  ---------
  jsonf : str
    string representing the filename to load
  Return
  ------
  dict
    A dictionary with key pairs from the JSON file
    """
  if jsonf == None:
    delimiter = "/" if platform.system() != "Windows" else "\\"
    file_path = "/".join(
      os.path.realpath(__file__).split(delimiter)[0:-1] +
        [ "config.json", ])
  else:
    file_path = jsonf
  logging.debug('JSON Config File: ' + file_path)
  with open(file_path, "r") as file:
    config = json.load(file)
  logging.debug(config)
  return config

def UploadZippedDirectory(bucket,zipdir,zipname):
  """ This method zips a directory and uploads it to a
  google cloud storage bucket

  Parameters:
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
  local_time = FindNewestTimeLocal(filedir)
  cloud_time = FindNewestTimeCloud(bucket)
  if local_time > cloud_time:
    logging.debug('Newest local time: '+ str(local_time))
    logging.debug('Cloud local time: '+ str(cloud_time))
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
  ftime = 0.0
  for dirpath,_,filenames in os.walk(dirc):
    for f in filenames:
      cfile = os.path.abspath(os.path.join(dirpath,f))
      file_time = os.path.getmtime(cfile)
      if file_time > ftime:
        ftime = file_time
        logging.debug("Local File Timestamp: " + str(ftime))
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
    if file_time > ftime:
      ftime = file_time
      logging.debug("Cloud File Timestamp: " + str(ftime))
  return ftime

if __name__ =="__main__":
  app.run(main)