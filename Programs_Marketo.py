from marketorestpython.client import MarketoClient
from pandas.io.json import json_normalize
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine
import csv
import json
import sys
import psycopg2
import datetime
import subprocess
import pandas as pd
import smtplib
import time

def sendErrorMail(variable, e):
    smtpserver = smtplib.SMTP("smtp.gmail.com", 587)
    smtpserver.ehlo()
    smtpserver.starttls()
    smtpserver.ehlo()
    smtpserver.login(variable['sender'], variable['password'])
    subject = "Subject: Error in Script\n"
    msg = 'Subject: {}\n\n{}'.format(subject, str(e))
    smtpserver.sendmail(variable['sender'], variable['reciever'], msg)
    print('Error Sent in email')
    smtpserver.close()


def creating_database(conn, postgresDatabaseName):
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    statement = "SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname = '" + postgresDatabaseName + "';"
    cursor.execute(statement)
    not_exists_row = cursor.fetchone()
    not_exists = not_exists_row[0]
    if not_exists == False:
        cursor = conn.cursor()
        cursor.execute('CREATE DATABASE ' + postgresDatabaseName + ';')
        conn.commit()
    else:
        print("Database Already Exists....")


def main():
    # getting Marketo Credentials
    with open('config.json') as Variable_file:
        variable = json.load(Variable_file)

    munchkin_id = variable["Marketo_Credentials"]["Munchkin_Id"]
    client_id = variable["Marketo_Credentials"]["Client_Id"]
    client_secret = variable["Marketo_Credentials"]["Client_Secret"]
    try:
        mc = MarketoClient(munchkin_id, client_id, client_secret)
    except Exception as e:
        # sendErrorMail(variable['email'], e)
        print("error sent in email", e)

    postgresHost = variable["PostgreSQL"]["PostgreSQL_Host"]
    postgresDatabaseName = variable["PostgreSQL"]["PostgreSQL_Database_Name"]
    postgresSchemaName = variable["PostgreSQL"]["PostgreSQL_Schema_Name"]
    postgresUserName = variable["PostgreSQL"]["PostgreSQL_User_Name"]
    postgresPassword = variable["PostgreSQL"]["PostgreSQL_Password"]
    postgresPort = variable["PostgreSQL"]["PostgreSQL_Port"]
    tableProgramsInMarketo = variable["PostgreSQL"]["Table"]["Table_programs_in_marketo"]
    tableProgramData = variable["PostgreSQL"]["Table"]["Table_programs_data"]
    tableProgramsMembersData = variable["PostgreSQL"]["Table"]["Table_programs_members_data"]


    ''' Creating Schema and Using it as default'''
    try:
        conn = psycopg2.connect(dbname=postgresDatabaseName, user=postgresUserName, password=postgresPassword,
                                port=postgresPort)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        cursor.execute('CREATE SCHEMA IF NOT EXISTS ' + postgresSchemaName + ';')
        cursor.execute("SET search_path TO " + postgresSchemaName)

    except:
        print("Error! Connection Failure.")
        # sendErrorMail(variable['email'], e)
        sys.exit()

    engine = create_engine(
        r'postgresql://' + postgresUserName + ':' + postgresPassword + '@' + postgresHost + ':' + str(
            postgresPort) + '/' + postgresDatabaseName)

    try:
        cursor.execute("Truncate Table " + tableProgramData)
        cursor.execute("Truncate Table " + tableProgramsInMarketo)
        cursor.execute("Truncate Table " + tableProgramsMembersData)
        programIds = get_programIds(mc)
        check_new_programs(engine, postgresSchemaName, tableProgramsInMarketo, programIds)
        get_programsData(mc, cursor, engine, postgresSchemaName, tableProgramsInMarketo, tableProgramData)
        get_membersData(mc, cursor, engine, postgresSchemaName, tableProgramsInMarketo, tableProgramsMembersData)
    except Exception as e:
        # sendErrorMail(variable['email'], e)
        print("error sent in email", e)

    # membersData = pd.DataFrame(mc.execute(method='get_multiple_leads_by_program_id', programId='1137'))

def get_programsData(mc, cursor, engine, postgresSchemaName, tableProgramsInMarketo, tableProgramData):
    query = "SELECT program_id from " + postgresSchemaName + "." + tableProgramsInMarketo + " WHERE fetched_program_data is FALSE"
    df = pd.read_sql_query(query, engine)
    pid = list(df['program_id'])
    print("New Programs Data for our database : ", len(pid))
    # programData = pd.DataFrame()

    for id in pid:
        cost = 0
        print("fetching {}'s data".format(id))
        data = mc.execute(method='get_program_by_id', id=id)
        time.sleep(2)
        if (len(data[0]['costs']) < 1):
            cost = 0
        else:
            cost = []
            for each in data[0]['costs']:
                cost += each['costs']

        programData = pd.DataFrame(data)
        # programData = pd.concat([programData, df], axis=0, ignore_index=True)
        #    df = pd.DataFrame(programData)
        temp = programData[['id', 'tags']]
        temp = unstack(temp, 'tags').dropna(subset=['tags']).to_json(orient='records')
        temp = json_normalize(json.loads(temp))
        if len(temp):
            temp = temp.drop('tags.tagType', axis=1)
            temp = temp.groupby('id')['tags.tagValue'].apply(', '.join).reset_index()
            programData = programData.merge(temp, left_on='id', right_on='id', how='left').drop('tags', axis=1).rename(
                index=str, columns={'tags.tagValue': 'tags'})
        else:
            programData['tags'] = ''
        programData = programData.rename(index=str, columns={'id': 'program_id'}).drop('folder', axis=1)
        # cost = pd.Series(costs)
        programData['costs'] = cost
        programData.columns = map(str.lower, programData.columns)
        programData.to_sql(tableProgramData, engine, if_exists='append', schema=postgresSchemaName, index=False)
        cursor.execute(
            "Update " + postgresSchemaName + "." + tableProgramsInMarketo + " set fetched_program_data ='TRUE' where program_id = " + str(
                id))
    print("Program Data is updated")

def get_membersData(mc, cursor, engine, postgresSchemaName, tableProgramsInMarketo, tableProgramsMembersData):
    query = "SELECT program_id from " + postgresSchemaName + "." + tableProgramsInMarketo + " WHERE fetched_members_data is FALSE"
    df = pd.read_sql_query(query, engine)
    pid = list(df['program_id'])
    print("Members data need to update of {} programs : ".format(len(pid)))
    for id in pid:
        members_data = pd.DataFrame(mc.execute(method='get_multiple_leads_by_program_id', programId=id,
                                               fields=['createdAt', 'email', 'firstName', 'id', 'lastName',
                                                       'leadStatus', 'sfdcAccountId', 'sfdcContactId', 'sfdcId',
                                                       'sfdcLeadId', 'updatedAt', 'mktoName', 'title', 'company',
                                                       'phone',
                                                       'leadSource', 'sfdcType', 'True_Market_Name__c',
                                                       'Market_Region__c', 'Lead_Source_Detail__c',
                                                       'Account_Owner_Email__c'], batchSize=None))
        members_data['program_id'] = id
        members_data = members_data.to_json(orient='records')
        members_data = json_normalize(json.loads(members_data)).rename(index=str, columns={'id': 'leadid'}).rename(
            index=str, columns=lambda x: x.replace('.', '_'))
        members_data.columns = map(str.lower, members_data.columns)
        members_data.to_sql(tableProgramsMembersData, engine, if_exists='append', schema=postgresSchemaName,
                            index=False)
        cursor.execute(
            "Update " + postgresSchemaName + "." + tableProgramsInMarketo + " set fetched_members_data ='TRUE' where program_id = " + str(
                id))
        print("Member inserted of program no. ", id)
    print("Program Data is updated")

def get_programIds(mc):
    subprocess.call(['pythonw.exe', 'check_API_calls.py'])
    programs = pd.DataFrame(mc.execute(method='browse_programs', maxReturn=200))
    print("Total Program in Marketo : ", len(programs))
    programs = list(programs['id'].drop_duplicates())
    return programs

def unstack(dataframe, colum):
    temp = dataframe[colum].apply(pd.Series).unstack().reset_index(level=0, drop=True)
    temp.name = colum
    dataframe = dataframe.drop(colum, axis=1).join(temp)
    del temp
    return dataframe

def check_new_programs(engine, postgresSchemaName, table, pid):
    query = "SELECT program_id from " + postgresSchemaName + "." + table
    df = pd.read_sql_query(query, engine)
    programsInTable = list(df['program_id'])
    programsInTable = set(sorted(programsInTable))
    programsFromMarketo = set(sorted(pid))
    new_pid = list(programsFromMarketo.difference(programsInTable))
    print("new_pid :", new_pid)
    print("New programs for our database : ", len(new_pid))
    df = pd.DataFrame(new_pid).rename(index=str, columns={0: 'program_id'})
    df['fetched_program_data'] = False
    df['fetched_members_data'] = False
    df.to_sql(table, engine, if_exists='append', schema=postgresSchemaName, index=False)
    return new_pid

if __name__ == '__main__':
    main()
