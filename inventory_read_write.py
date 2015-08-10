# -*- coding: utf-8 -*-
"""
Created on Wed Jan 14 10:29:04 2015

@author: Administrator
"""

import pyodbc
from os import getenv
import gdata.docs.client
import re

ROW_RE = re.compile('<span>(.*?)</span>')
ALARM_TEMP_FILE = 'temp_alarm.txt' # temp file for alarm data
INV_TEMP_FILE = 'temp_inv.txt' # temp file for inventory data
ALARM_DOC_NAME = 'Alarm Data' # google docs name
INV_DOC_NAME = 'Inventory Data' # google docs name
username = getenv('GOOG_UID') # google/gmail login id
passwd = getenv('GOOG_PWD') # google/gmail login password

# Parameters for SQL Server connection
server = getenv('SSA_SERVER')
db = getenv('SSA_DATABASE')
uid = getenv('SSA_UID')
pwd = getenv('SSA_PWD')

connection_string = 'DRIVER={{SQL Server}};SERVER={0};DATABASE={1};UID={2};'\
                    'PWD={3}'.format(server, db, uid, pwd)


def get_latest_updates():
    # Get numbers from LOG_INVENTORY, JOIN with STORAGE to get tank and product
    # names, then JOIN with SITE to get store names, excluding 
    # STORAGE_TYPE_ID 102, which doesn't show up in SSA website
    test_query = """SELECT tank_names.*, SITE.NAME FROM 
        (SELECT last.*, sto.NAME, sto.PRODUCT_NAME
        FROM (SELECT I1.SITE_ID, I1.STORAGE_ID, I1.STORAGE_TYPE_ID, 
        I1.GROSS_VOLUME, I1.ULLAGE, I1.GROSS_WATER_VOLUME, I1.WATER_LEVEL, 
        I1.LAST_UPDATED
            FROM LOG_INVENTORY I1
            JOIN (
                SELECT MAX(LAST_UPDATED) AS LAST_UPDATED, SITE_ID, 
                STORAGE_ID, STORAGE_TYPE_ID
                FROM LOG_INVENTORY
                GROUP BY SITE_ID, STORAGE_ID, STORAGE_TYPE_ID) as I2
                ON I1.SITE_ID = I2.SITE_ID AND
                I1.STORAGE_ID = I2.STORAGE_ID AND
                I1.STORAGE_TYPE_ID = I2.STORAGE_TYPE_ID AND
                I1.LAST_UPDATED = I2.LAST_UPDATED
            WHERE I1.STORAGE_TYPE_ID <> 102) last
        RIGHT JOIN STORAGE sto
        ON last.STORAGE_ID = sto.STORAGE_ID AND
        last.STORAGE_TYPE_ID = sto.STORAGE_TYPE_ID AND
        last.SITE_ID = sto.SITE_ID) tank_names
        RIGHT JOIN SITE
        ON tank_names.SITE_ID = SITE.SITE_ID
    ORDER BY tank_names.SITE_ID
    """
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    cursor.execute(test_query)
    rows = cursor.fetchall()
    cursor.close()
    del cursor
    conn.close()
    
    return rows

    
def get_todays_active_alarms():
    query = """
        SELECT site.NAME, a.CAT_NAME, 
            CONCAT(a.DEV_NAME, ' ', a.DEV) as DEV_NAME, a.DEVICE_NAME, 
            a.CODE_NAME, a.STAT_NAME, a.DATE_TIME, a.LAST_UPDATED FROM
        	(SELECT b.*, stat.NAME as STAT_NAME FROM
        		(SELECT z.*, devs.NAME as DEV_NAME FROM
        			(SELECT y.*, cats.NAME as CAT_NAME FROM 
        				(SELECT x.SITE_ID, codes.CATEGORY, codes.DEVICE, 
                                 x.DEVICE as DEV, x.DEVICE_NAME, 
                                 codes.NAME as CODE_NAME, x.STATUS, 
                                 x.DATE_TIME, x.LAST_UPDATED
        					FROM (SELECT SITE_ID, CODE, DATE_TIME, DEVICE,
        						STATUS, LAST_UPDATED, DEVICE_NAME
        						FROM LOG_ALARMS
        						WHERE IS_ACTIVE = 1 AND CODE <> 7112 AND
        						CONVERT(DATE, LAST_UPDATED) = CONVERT(DATE, GetDate())) x
        					JOIN CONST_ALARM_CODES codes ON x.CODE = codes.CODE) y
        			JOIN CONST_ALARM_CATEGORIES cats ON y.CATEGORY = cats.CODE) z
        		JOIN CONST_ALARM_DEVICES devs ON z.DEVICE = devs.CODE) b
        	JOIN CONST_ALARM_STATUS_LIST as stat ON b.STATUS = stat.CODE) a
        JOIN SITE ON a.SITE_ID = SITE.SITE_ID
    """
    
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    del cursor
    conn.close()
    
    return rows


def load_docs():
    client = gdata.docs.client.DocsClient(source='shortstop-dash')
    client.ssl = True
    client.ClientLogin(username, passwd, client.source)
    return client


def update_doc(doc_name, temp_file):
    client = load_docs()
    doc_list = client.get_resources()
    docs = []
    for i in doc_list.entry:
        docs.append(i.title.text)
    doc_num = None
    for i, j in enumerate(docs):
        if j == doc_name:
            doc_num = i
    if doc_num == None:
        return 0
        
    entry = doc_list.entry[doc_num]
    media = gdata.data.MediaSource()
    media.set_file_handle(temp_file, 'text/txt')
    client.update_resource(entry, media=media, update_metadata=False)
    

# Write rows to temporary file
# TODO - exception handling
def write_file(temp_file, rows):
    with open(temp_file, 'wt') as f:
        for row in rows:
            f.write('|'.join(str(e) for e in row) + '\n')  # Commas in data


if __name__ == '__main__':
    rows = get_latest_updates()   # Get data from SQL Server
    write_file(INV_TEMP_FILE, rows)   # Write data to temp file
    rows = get_todays_active_alarms()
    write_file(ALARM_TEMP_FILE, rows)
    status = update_doc(INV_DOC_NAME, INV_TEMP_FILE)  # Copy temp file to Google Docs
    status = update_doc(ALARM_DOC_NAME, ALARM_TEMP_FILE)
    #status = retrieve_doc()
    
    if status == 1:
        print 'Success'
    elif status == 0:
        print 'Crushing failure!'
    