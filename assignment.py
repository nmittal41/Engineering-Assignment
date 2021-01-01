from threading import Thread
from pathlib import Path
import time
import sys
import os
import json
from filelock import Timeout, FileLock
from json.decoder import JSONDecodeError

"""
This class contain all the functions that will be used to perform
CRD (create, read, delete) operations in the file.
"""
class datastore(Thread):

    # To initialize the class so that operations can be performed.
    def __init__(self): 
        ch=input("Want to perform CRD operations on a specific path ? (y/n): ")
        if(ch=='y'):
            path=input("Enter the path: ") # Provide path of your choice like: desktop/xyz
        else:
            path='.'

        file1="file.txt"
        file2="file.txt.lock"

        if(path=='.'):
            self.path=file1
            self.lock_path=file2
        else:
            self.path=path+"/"+file1
            self.lock_path=path+"/"+file2

        if(os.path.exists(self.path)==False):

            if(os.path.exists(path)): #Initializing the file on path if it does not exist
                open(os.path.join(path, file1), 'w')
                open(os.path.join(path, file2), 'w')
            else:
                print("Error: Invalid Path")
                return
        else:
            pass


        """
        To lock the file so that only one process can perform
        operations at a time.
        """
        try:
            self.lock=FileLock(self.lock_path, timeout=-1)
            self.lock.acquire(timeout=10)
        except TimeoutError:
            print("Error: More than one process cannot access same file at a time")


    """
    To enter the data in file in form of key-value pair.
    Here timelimit property is optional
    e.g: 1. object.create('Person1','{"name":"Nikhil", "age":26}') without timelimit
            object.create('Person2','{"name":"Suresh", "age":30}', 20) This will have timelimit of 20 seconds.
    """
    def create(self,key,value,timeLimit=0):

        if type(key)!=str and len(key)>32: #checking if key is of string type and have less than 32 chars
            print("Error: Key is always a string-capped at 32 chars")
            return

        try: # checking that value is provided in proper json format or not
            json_object=json.loads(value)
        except ValueError:
            print("Error: Provide value in proper json format")
            return 

        #To check size of value is less than 16KB or not
        if((sys.getsizeof(value)/1024)>16):
            print("Error: Value should be less than 16KB")
            return

        fp=open(self.path,"r")
        try:
            data=json.load(fp)
        except JSONDecodeError:
            data={}
            pass
            
        # To check size of file is less than 1GB or not
        if((sys.getsizeof(data))>1024**3):
            print("Size Limit of File Exceeded")
            return

        if key in data:
            print("Error: This key already exists")
            return
        
        if timeLimit:
            data[key]=[value,time.time()+timeLimit]

        else:
            data[key]=[value,0]

        json_object=json.dumps(data)
        with open(self.path,"w") as outfile:
            outfile.write(json_object)
        print("Data inserted succesfully")

    """
    To retrieve the data using key
    e.g: object.read('Person1')
    """
    def read(self,key):

        #To check key is in string format and have max 32 chars
        if type(key)!=str and len(key)>32:
            print("Error: Key is always a string-capped at 32 chars")
            return

        fp=open(self.path,"r")
        try:
            data=json.load(fp)
        except JSONDecodeError:
            data={}
            pass


        #To check if key is present or not
        if key in data:
            temp=data[key]

            if temp[1]!=0: #checking if key has time-to-live property or not
                if time.time()<temp[1]: #checking if key's time-to-live property expired or not
                    print(temp[0])
                else:
                    print("Time-To-Live for this key has expired")
            else:
                print(temp[0])

        else:
            print("Key not found")


    """
    To delete the data from file if it exists using key
    e.g: object.delete('Person1')
    """
    def delete(self,key):
        #To check key is in string format and have max 32 chars
        if type(key)!=str and len(key)>32:
            print("Error: Key is always a string-capped at 32 chars")
            return

        fp=open(self.path,"r")
        try:
            data=json.load(fp)
        except JSONDecodeError:
            data={}
            pass
        
        #To check if key is present or not
        if key in data:
            temp=data[key]

            if temp[1]!=0:#checking if key has time-to-live property or not
                if time.time()<temp[1]: #checking if key's time-to-live property expired or not
                    flag=1
                else:
                    flag=0
                    print("Time-To-Live for this key has expired")
            else:
                flag=1
        else:
            print("Key not found")
            return

        #deleting key value pair from file
        if (flag==1):
            data.pop(key)
            json_object=json.dumps(data)
            with open(self.path, "w") as outfile:
                outfile.write(json_object)
            print("Key Value Pair deleted successfully")


        



















