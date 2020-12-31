from threading import Thread
from pmc.singleton import singleton
from pathlib import Path
import time
import sys
import os
import json
from filelock import Timeout, FileLock

ch=input("Want to perform operations on a specified path ? (y/n): ")
if(ch=='y'):
    path=input("Enter the path: ")
else:
    path="file_store.txt"

# lock_path="file_store.txt.lock"

# lock=FileLock(lock_path,timeout=-1)
# lock.acquire()



class datastore(Thread):

    def create(self,key,value,timeLimit=0):

        if type(key)!=str and len(key)>32:
            print("Error: Key is always a string-capped at 32 chars")
            return()

        if((sys.getsizeof(value)/1024)>32):
            print("Error: Value should be less than 16KB")
            return()

        file=Path(path)
        if file.exists():
            fp=open(path,"r")
            data=json.load(fp)

            if key in data:
                print("Error: This key already exists")
                return()
            
            if timeLimit:
                data[key]=[value,time.time()+timeLimit]

            else:
                data[key]=[value,0]

            json_object=json.dumps(data)
            with open(path,"w") as outfile:
                outfile.write(json_object)
            print("Data inserted succesfully")

    def read(self,key):

        if type(key)!=str and len(key)>32:
            print("Error: Key is always a string-capped at 32 chars")
            return()
        fp=open(path,"r")
        data=json.load(fp)
        if key in data:
            temp=data[key]

            if temp[1]!=0:
                if time.time()<temp[1]:
                    print(temp[0])
                else:
                    ##Delete operation for key
                    print("Time for this key expired")
            else:
                print(temp[0])

    # def delete(self,key):

    #     if type(key)!=str and len(key)>32:
    #         print("Error: Key is always a string-capped at 32 chars")
    #         return()
        



















