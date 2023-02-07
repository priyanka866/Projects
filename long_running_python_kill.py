#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Created on Tue Feb  7 17:59:06 2023

@author: priyankapandey
"""
import os
import sys
from datetime import timedelta, datetime
import subprocess
import re
import json
script_begin_time = datetime.now()
print(script_begin_time)


def connect(config_path):
    '''Connecting to json file credentials'''
    with open(config_path) as json_data:
        creds = json.load(json_data)
        return creds
    print("=== Config file loaded")

#config file contains my creds in json format
#email/slack notification can also be created instead of killing the process

CONFIG_PATH = os.environ.get('CONFIG_PATH', None)
db_connect = connect(CONFIG_PATH)
print("===import complete")

###long running python code 
possible_date_format = ["%H:%M", "%b%d","%Y"]
x=os.path.dirname(__file__)+'/'+'python_grep_' + str(datetime.date(datetime.now())).replace("-","")+'.csv'

py_grep = subprocess.check_output('ps aux|grep -i python|grep "\.py" > {}'.format(x), shell=True)

with open(x, "r") as file:
    csvFile = file.readlines()
    for lines in csvFile[:]:
        rgx=re.sub(' +', '|', lines)
        splt= rgx.split('|')
        pid= splt[1]
        start= splt[8]
        tim= splt[9]
        cmd=" ".join(splt[10:])
        error_heading="Long Running Python ID : " +'PID-'+pid +' -Start time- '+ start   
        for date_format in possible_date_format:
            try:
                start_tm=datetime.strptime(start,date_format)
                start_te = start_tm.strftime("%H:%M:%S")
                now = datetime.now()
                current_time = now.strftime("%H:%M:%S")
                tim_delta=datetime.strptime(current_time,"%H:%M:%S")-datetime.strptime(start_te,"%H:%M:%S")
                error_type="Run Time : " + str(tim_delta)
                if tim_delta > timedelta(seconds=10800):
                    print(f"Time limit exceeded for {pid} for {cmd}")
                else:
                    pass
            except ValueError:
                pass                    

os.remove(x)

print("Script Ended at: ", datetime.now() - script_begin_time)
print("==========script ended=============")
