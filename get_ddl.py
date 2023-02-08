#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb  7 17:59:06 2023

@author: priyankapandey
"""

from ruamel import yaml
import json
import sys
import paramiko
from sshtunnel import SSHTunnelForwarder
import mysql.connector

#reading YML file that conatins all tables name
YML_PATH = "/Users/priyankapandey/Documents/data-science/data-lake/data-pipeline/config/prod_servify_in.yml"
with open(YML_PATH, 'r') as stream:
    data_loaded = yaml.safe_load(stream)

def connect(config_path):
    '''Connecting to json file credentials'''
    with open(config_path) as json_data:
        creds = json.load(json_data)
        return creds
    print("=== Config file loaded")

CONFIG_PATH = '/Users/priyankapandey/Documents/data-science/data-lake/data-pipeline/config/config.json' #os.environ.get('CONFIG_PATH', None)
db_connect = connect(CONFIG_PATH)
sys.path.append(db_connect['Path']['utilities'])
print("===import complete")

src_db= 'metricDB'


def ddl(mysql_json,server,table):
    with open(mysql_json) as file:
        mysqldb_connect= json.load(file)
        mysqldb=mysqldb_connect[server]
        pkeyfilepath = '/Users/priyankapandey/.ssh/id_rsa'
        mypkey = paramiko.RSAKey.from_private_key_file(pkeyfilepath, password='')
        ssh_host = 'ssh.servify.tech'
        ssh_user = 'jumpuser'
        ssh_port = 22
        tunnel_parts = SSHTunnelForwarder((ssh_host, ssh_port), ssh_username=ssh_user, ssh_pkey=mypkey, remote_bind_address=(mysqldb['host'],  mysqldb['port']))
        tunnel_parts.start()
        connection= mysql.connector.connect(host='127.0.0.1',
                                            database=mysqldb['database'],
                                            user=mysqldb['user'],
                                            password=mysqldb['password'],
                                            port = tunnel_parts.local_bind_port)

        connection._open_connection()
        cursor = connection.cursor(buffered=True)
        cursor.execute('''SHOW CREATE TABLE {} ;'''.format(table))
        output = cursor.fetchone()[1]
        cursor.close()
        connection.close()
        return output



for table in data_loaded:
    print(table)
    ddl=  ddl(CONFIG_PATH,src_db,table)
    

#and write it to the file 
with open('/Users/priyankapandey/Desktop/ddl.sql', 'w') as file: 
        file.write(ddl)   








