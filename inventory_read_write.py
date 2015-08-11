# -*- coding: utf-8 -*-
"""
Created on Wed Jan 14 10:29:04 2015

@author: Administrator
"""

import pyodbc
from os import getenv

import httplib2
import argparse

from apiclient import discovery, errors
import oauth2client
from oauth2client import client
from oauth2client import tools

from apiclient.http import MediaFileUpload

SCOPES = 'https://www.googleapis.com/auth/drive'
CLIENT_SECRET_FILE = 'E:/projects/tests/client_secret.json'
APPLICATION_NAME = 'Shortstop Dashboard'
ALARM_FILE_ID = '1kQ9Bfs7S-hd_NiPTZa0JQLYWg0FNVNgGl-9yRe3fI0c'
INV_FILE_ID = '1FfsSGg2XUExzPmggwvKSxRqG6sUnau7Ik53CSocKNjM'
TEST_FILE_ID = '1Uc3m5SKkH_fJ52CN8Cso367158CgF7PsjA_8BSpF-cA'

INV_TEMP_FILE = 'temp_inv.txt'
ALARM_TEMP_FILE = 'temp_alarm.txt'

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
    

def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    store = oauth2client.file.Storage('shortstop-dash.json')
    credentials = store.get()

    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        parser = argparse.ArgumentParser(parents=[tools.argparser])
        flags = parser.parse_args()
        credentials = tools.run_flow(flow, store, flags)
        if not credentials:
            print('Could not obtain valid credentials')

    return credentials
    

def list_files():
    """List all files in Drive account"""
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('drive', 'v2', http=http)

    results = service.files().list().execute()
    items = results.get('items', [])
    for item in items:
        print('{0} ({1})'.format(item['title'], item['id']))
    

def update_file(file_id, upload_file):
    """Upload temp file upload_file to Google drive file file_id
    
    Google drive document file designated by file_id are updated with
    the contents of text file designated by upload_file.
    
    Args:
        file_id: String representing the Google Drive id of the file to 
            be updated.
        upload_file: String representing the name of the text file to be
            uploaded to Google Drive
    
    Returns:
        The new content of the updated Google Drive file.
    """
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('drive', 'v2', http=http)

    try:
        file_meta = service.files().get(fileId=file_id).execute()

        # File's new metadata.
        file_meta['mimeType'] = 'text/plain'
    
        # File's new content.
        media_body = MediaFileUpload(upload_file, mimetype='text/plain')
    
        # Send the request to the API.
        updated_file = service.files().update(
            fileId=file_id,
            body=file_meta,
            newRevision=False,
            media_body=media_body).execute()
        return updated_file
    except errors.HttpError as error:
        print('An error occurred: {}'.format(error))
    

# Write rows to temporary file
# TODO - exception handling
def write_file(temp_file, rows):
    with open(temp_file, 'wt') as f:
        for row in rows:
            f.write('|'.join(str(e) for e in row) + '\n')  # Commas in data


if __name__ == '__main__':
    # Get inventory and alarm data from SQL Server and write data to temp files
    rows = get_latest_updates()
    write_file(INV_TEMP_FILE, rows)
    rows = get_todays_active_alarms()
    write_file(ALARM_TEMP_FILE, rows)
    
    # Copy temp files to Google Docs
#    update_file(INV_FILE_ID, INV_TEMP_FILE)
#    update_file(ALARM_FILE_ID, ALARM_TEMP_FILE)
    update_file(TEST_FILE_ID, INV_TEMP_FILE)
    