# -*- coding: utf-8 -*-
from __future__ import print_function
import pandas as pd
pd.set_option('display.max_columns', 150)       #pandas setting 顥示列數上限
pd.set_option('display.width', 5000)           #pandas setting 顯示列的闊度
pd.set_option('display.max_colwidth',20)      #pandas setting 每個數據顥示上限
pd.set_option('display.max_rows', 5000)       #pandas setting 顯示行的闊度
pd.options.mode.chained_assignment = None
import mysql.connector
from mysql.connector import Error
import base64
from email.message import EmailMessage
from google.auth.transport.requests import Request
import google.auth
import mimetypes
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/gmail.send']

import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.dirname(os.path.abspath(__file__)) +"/dauntless-brace-355907-ccb9311dcf58.json"

def main():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    try:
        # Call the Gmail API
        service = build('gmail', 'v1', credentials=creds)
        results = service.users().labels().list(userId='me').execute()
        labels = results.get('labels', [])

        if not labels:
            print('No labels found.')
            return
        print('Labels:')
        for label in labels:
            print(label['name'])

    except HttpError as error:
        # TODO(developer) - Handle errors from gmail API.
        print(f'An error occurred: {error}')

def gmail_create_draft(con):
    """Create and insert a draft email.
       Print the returned draft's message and id.
       Returns: Draft object, including draft id and message meta data.

      Load pre-authorized user credentials from the environment.
      TODO(developer) - See https://developers.google.com/identity
      for guides on implementing OAuth2 for the application.
    """
    creds, _ = google.auth.default()

    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    try:
        service = build('gmail', 'v1', credentials=creds)
        message = EmailMessage()

        message.set_content(str(con))

        message['To'] = 'alphax.lys@gmail.com'
        message['From'] = 'origin.sunrise@gmail.com'
        message['Subject'] = 'Automated draft'

        # encoded message
        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()

        create_message = {
            'raw': encoded_message
        }

        # pylint: disable=E1101
        send_message = (service.users().messages().send
                        (userId="me", body=create_message).execute())
        print(F'Message Id: {send_message["id"]}')

    except HttpError as error:
        print(F'An error occurred: {error}')
        send_message = None
    return send_message

def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection


def data_check():
    connection = create_server_connection('103.68.62.116', 'root', '630A78e77?')
    cursor = connection.cursor()

    watchlist = pd.read_csv('watchlist.csv', encoding='Big5')
    symbol = watchlist['Futu symbol'].tolist()
    symbol = symbol[:1]
    symbol_dict = {i: i.replace('.', '_') for i in symbol}   # 轉變成Dictionary

    for i in symbol_dict:
        try:

            sql = "USE %s" %(symbol_dict[i])
            cursor.execute(sql)

            '''
            sql = "DELETE FROM Day WHERE date='2023-01-16'"
            cursor.execute(sql)
            connection.commit()
            
            sql = "DELETE FROM Mins WHERE time_key>'2023-01-15'"
            cursor.execute(sql)
            connection.commit()   

            sql = "DROP TABLE IF EXISTS 2023_01_16"
            cursor.execute(sql)
            connection.commit()'''

        except: pass

        print(i)
        #df = pd.read_sql("SELECT * FROM Mins", connection)
        #print(df.tail(3))


def model3(df, P_level = None):   # P_level應是現價
    df.drop(df[df['ticker_direction'] == 'NEUTRAL'].index, inplace=True)
    distribution_T = df.groupby('price')['turnover'].sum()

    if P_level is None:
        import statistics
        list_P = distribution_T.index
        P_level = statistics.median(list_P)

    distribution_T = pd.DataFrame(distribution_T)
    #distribution_T.reset_index(inplace=True)
    distribution_T['index'] = distribution_T.index.map(lambda x: x - P_level)
    distribution_T['distribution'] = distribution_T['turnover'] * distribution_T['index']

    an = (distribution_T['distribution'].sum() / distribution_T['distribution'].abs().sum()) * 100
    m3_value = round(an, 4)
    return m3_value


if __name__ == '__main__':
    #gmail_create_draft('F5')
    data_check()











