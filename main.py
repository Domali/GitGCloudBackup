import os
from google.cloud import storage
def main():
    dir_to_check = r''
    json_key = r'D:\Seans\CloudKeys\MyFirstProject-e2cc2e0f8671.json'
    client = storage.Client.from_service_account_json(json_key)
    bucket = client.get_bucket('dom-git-bucket')
    blob = bucket.blob('testfile.txt')
    blob.upload_from_filename(r'C:\Users\domal.DESKTOP-08KELV1\Desktop\testoftestyay.txt')

if __name__ =="__main__":
    main()