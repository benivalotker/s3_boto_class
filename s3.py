#/path/path/
#title           :AWS S3 Class
#description     :AWS S3 services.
#update date     :01/01/2020 12:10
#version         :1.0
#changes         :veriosn 1.0.
#python_version  :3.6  
#==============================================================================

import boto3
import json
from datetime import datetime

'''
the class create object instance per one bucket

NOTE: not all method was tested 
'''
class S3():
    # constructor
    def __init__(self, bucket_name):
        self.__s3 = boto3.client('s3')
        self.__bucket_name = bucket_name


    # getter and setter
    @property
    def bucket(self):
        return self.__bucket_name


    @bucket.setter
    def bucket(self, new_bucket_name):
            self.__bucket_name = new_bucket_name
            return True


    # download data 
    def download_data(self, bucket_object_name):
        try:
            obj_res = self.__s3.get_object(Bucket=self.__bucket_name, Key=bucket_object_name)
            obj_data = json.loads(obj_res['Body'].read())
        except Exception as ex:
            raise ex

        return obj_data


    # upload data to new file
    def upload_data(self, bucket_file_name=None, file_data=None):
        if bucket_file_name == None:
            bucket_file_name = f"S3 {datetime.now().strftime('%Y_%m_%d %H_%M_%S')}.txt"

        try:
            self.__s3.put_object(Body=json.dumps(file_data), Bucket=self.__bucket_name, Key=bucket_file_name)
        except Exception as ex:
            raise ex

        return bucket_file_name


    # object list
    def get_object_list(self):
        obj_file_name = []

        try:
            bucket_list = self.__s3.list_objects_v2(Bucket=self.__bucket_name)

            for obj in bucket_list['Contents']:
                obj_file_name.append(obj['Key'])
        except Exception as ex:
            raise ex

        return obj_file_name


    # check if bucket exist
    def check_exist_bucket(self, object_name):
        try:
            self.__s3.get_object(Bucket=self.__bucket_name, Key=object_name)
        except Exception as ex:
            raise ex

        return True


    # delete object from bucket 
    def delete_object(self, object_name):
        try:
            self.__s3.delete_object(Bucket=self.__bucket_name, Key=object_name)
        except Exception as ex:
            raise ex

        return True


    # copy data to new file
    def copy_data(self, dest_bucket_name, dest_object_name, src_object_name):
        try:
            copy_source = {'Bucket': self.__bucket_name, 'Key': src_object_name}
            res = self.__s3.copy_object(CopySource=copy_source, Bucket=dest_bucket_name, Key=dest_object_name)
        except Exception as ex:
            raise ex

        return res

