import os, sys
import datetime
import requests
import pysnmp
import time
import smtplib
import csv
import logging
import pandas as pd
import numpy as np
from numbers import Number
from pysnmp.entity.rfc3413.oneliner import cmdgen
from pysnmp.proto.rfc1902 import Integer, IpAddress, OctetString


def getSNMP(generator,comm_data,transport,value, multiple):
    if multiple is True:
        real_fun = getattr(generator, 'nextCmd')
        res = (errorIndication, errorStatus, errorIndex, varBinds) = real_fun(comm_data, transport, value)

        if not errorIndication is None  or errorStatus is True:
            handleError (res, loggerE, ip)
            if str(errorIndication) == "No SNMP response received before timeout":
                raise str(errorIndication)
            else:
                raise "GENERIC_ERROR"
        else:
            ret=[]
            for i in varBinds:
                ret+=[i[0][1].prettyPrint()]
            return ret
    else:
        real_fun = getattr(generator, 'getCmd')
        res = (errorIndication, errorStatus, errorIndex, varBinds) = real_fun(comm_data, transport, value)

        if not errorIndication is None  or errorStatus is True:
            handleError (res, loggerE, ip)
            if str(errorIndication) == "No SNMP response received before timeout":
                raise str(errorIndication)
            else:
                raise "GENERIC_ERROR"
        else:
            return varBinds[0][1].prettyPrint()


def sendMail(subject, body, appl):
    sender = "XXXXXXXXXXX"
    receivers = ["ZZZZZZZZZZZZZ","YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY"]

    message = """From:"""+sender+"""
To: ZZZZZZZZZZZZZZZZZZZZZZZZZ; YYYYYYYYYYYYYYYYYYYYYYYYYY
Subject:"""+subject+"""
"""+body

    try:
        smtpObj = smtplib.SMTP('ip_server_smtp')
        smtpObj.sendmail(sender, receivers, message)
    except SMTPException:
        handleError ("Error: unable to send email", loggerE, appl)

def testThresh(metric,value):
    thresholds = {
        # hash map to set the thresholds for each parameter obtained from SNMP
    }
    ret ="CLEAR"
    for sev,thres in thresholds[metric].items():
        if thres:
            # compare threshold with value obtained and set ret variable
    return ret

def testRaiseAlert(prev_alert,ip_addr,met,value,new_sev):
    # retrive the values ​​of the previous alarms (also the count if needed) to understand whether to raise an alert (send an email)
    # if  :
    #    sendMail(new_sev+' ALARM - '+met+' '+str(value), 'blablabla.......\n\n'+new_sev+' state for '+met+'\nNow the status of '+met+' is '+str(value), hostname)

def handleError(error_message, logger, ip):
    loggerE.error(error_message+' from IP: '+ip)

############################### MAIN #########################################
def main():
    loggerE = logging.getLogger('error')
    loggerE.setLevel(logging.ERROR)
    error = logging.FileHandler('path_to_error_log')
    error.setLevel(logging.ERROR)
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    error.setFormatter(formatter)
    loggerE.addHandler(error)

    while True:

        find_alarm=False
        prev_alert=pd.DataFrame({})
        write_rows=[]
        NOT_REACHED=[]
        #import csv file with active alarm
        if os.path.exists("path_to_file_of_active_alert"):
            prev_alert= pd.read_csv('path_to_file_of_active_alert',delimiter=';',header=None, names = ['time','name1','name2'],parse_dates=['time'])

        with open("file_with_ip_host_to_query_via_SNMP") as csvfile:
            readCSV = csv.reader(csvfile, delimiter=';')
            for row in readCSV:
                #parse input line

                community='comunity'
                generator = cmdgen.CommandGenerator()
                comm_data = cmdgen.CommunityData('server', community, 1) # 1 means version SNMP v2c
                transport = cmdgen.UdpTransportTarget((ip_addr, 161))

                timestamp=np.datetime64('now') 

                metric = {
                    #same as hash map in testThresh method, to populate with SNMP value
                }

                try:
                    metric1=int(getSNMP(generator,comm_data,transport,(1,3,6,1,2,1,25,2,3,1,5,3),False))
                    metric2=int(getSNMP(generator,comm_data,transport,(1,3,6,1,2,1,25,2,3,1,6,3),False))
                    #----

                    #for SNMP multiple level response
                    disk={}
                    i=0
                    diskUsedPct=getSNMP(generator,comm_data,transport,(1,3,6,1,4,1,2021,9,1,9),True)
                    for path in getSNMP(generator,comm_data,transport,(1,3,6,1,4,1,2021,9,1,3),True):
                        disk.update({path+'_Used_pct':int(diskUsedPct[i])})
                        i+=1
                    
                    metric["DISK"].update(disk)

                    for met in metric:
                        for key, value in metric[met].items():
                            outRow=[str(timestamp)]+row+[met]+[key]+[value]
                            write_rows+=[outRow]
                    NOT_REACHED+=[[str(timestamp)]+row+["NOT_REACHED"]+["Last_time"]+[10]]
                except:
                    NOT_REACHED+=[[str(timestamp)]+row+["NOT_REACHED"]+["Last_time"]+[0]]


        with open("path_to_file_where_archive_snmp_results", 'a',newline='') as csvFileOut:
            writer = csv.writer(csvFileOut, delimiter=';')
            for r in write_rows:
                writer.writerow(r)
        csvFileOut.close()

        write_rows+=NOT_REACHED

        for r in write_rows:
            severity= testThresh(r[4],r[5])
            if severity!="CLEAR":
                find_alarm=True
                if not prev_alert.empty:
                    if len((prev_alert.loc[(prev_alert["ip_addr"] == r[4]) & (prev_alert["metric"] == r[5])]).index) > 0 :
                        prev_alert.loc[(prev_alert["ip_addr"] == r[4]) & (prev_alert["metric"] == r[5]), ["count"]]+=1
                        testRaiseAlert(prev_alert,r[4],r[5],r[7],severity)
                        prev_alert.loc[(prev_alert["ip_addr"] == r[4]) & (prev_alert["metric"] == r[5]), ["severity"]]=severity
                    else:
                        prev_alert=prev_alert.append({'time': str(r[0]),'ip_addr': r[4],'metric': r[5],'count':1,'severity':severity}, ignore_index=True)
                        testRaiseAlert(prev_alert,r[4],r[5],r[7],severity)
                else:
                    prev_alert=pd.DataFrame({'time': [str(r[0])],'ip_addr': [r[4]],'metric': [r[5]],'count':[1],'severity':[severity]})
                    testRaiseAlert(prev_alert,r[4],r[5],r[7],severity)
            else:
                if not prev_alert.empty:
                    if len((prev_alert.loc[(prev_alert["ip_addr"] == r[4]) & (prev_alert["metric"] == r[5])]).index) > 0 :
                        find_alarm=True
                            if prev_alert.loc[(prev_alert["ip_addr"] == r[4]) & (prev_alert["metric"] == r[5]), ["count"]].values[0][0] >= 6:
                            sendMail('CLEAR ALARM - '+r[5]+' alert: '+r[6]+' '+str(r[7]), 'dscdscdscdsc\n\nAlert on '+r[5]+' is cleared.\nNow the status of '+r[5]+' is '+str(r[5]), r[3])
                        prev_alert=prev_alert.drop((prev_alert.loc[(prev_alert["ip_addr"] == r[4]) & (prev_alert["metric"] == r[5])]).index)

        if os.path.exists("path_to_file_of_active_alert"):
            os.remove("path_to_file_of_active_alert")
        if find_alarm:
            prev_alert.to_csv('path_to_file_of_active_alert',sep=';',header=False,index=False)

        time.sleep(480)


if __name__ == "__main__":
    main()
