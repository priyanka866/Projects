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
YML_PATH = "YML path"
with open(YML_PATH, 'r') as stream:
    data_loaded = yaml.safe_load(stream)

def connect(config_path):
    '''Connecting to json file credentials'''
    with open(config_path) as json_data:
        creds = json.load(json_data)
        return creds
    print("=== Config file loaded")

CONFIG_PATH = os.environ.get('CONFIG_PATH', None)
db_connect = connect(CONFIG_PATH)
sys.path.append(db_connect['Path']['utilities'])
print("===import complete")

src_db= 'your database name'


def ddl(mysql_json,server,table):
    with open(mysql_json) as file:
        mysqldb_connect= json.load(file)
        mysqldb=mysqldb_connect[server]
        pkeyfilepath = 'ssh key'
        mypkey = paramiko.RSAKey.from_private_key_file(pkeyfilepath, password='')
        ssh_host = 'hostname'
        ssh_user = 'user'
        ssh_port = 22
        tunnel_parts = SSHTunnelForwarder((ssh_host, ssh_port), ssh_username=ssh_user, ssh_pkey=mypkey, remote_bind_address=(mysqldb['host'],  mysqldb['port']))
        tunnel_parts.start()
        connection= mysql.connector.connect(host='127.0.0.1',
                                            database=mysqldb['database'],
                                            user=mysqldb['user'],
                                            password=mysqldb['password'],
                                            port = tunnel_parts.local_bind_port)
        connection._open_connection()
        cursor = connection.cursor()
        cursor.execute('''SHOW CREATE TABLE {} ;'''.format(table))        
        output = cursor.fetchone()[1]
        # print(output)
        cursor.close()
        connection.close()
        return output

res = []

for table in data_loaded:
    print(table)
    ddl_data=  ddl(CONFIG_PATH,src_db,table.lower()) + ";"
    res.append ("\n"+"#########"+ table +"#########" +"\n")
    res.append("\n")
    res.append(ddl_data) 
    res.append("\n")
    #and write it to the file 
    with open('file_name', 'w') as file:     
            file.writelines(res) 
            file.write("\n")
            file.close()




