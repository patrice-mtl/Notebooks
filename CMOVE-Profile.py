#!/usr/bin/python
import os
import gzip
import re
import pandas as pd
import numpy as np
filed_logs_path = "/home/popere/Projects/DICOMPrefetch/logs/filed_logs/"
path = "/home/popere/Projects/DICOMPrefetch/logs/"

def generateMasterFile(historicalProfile):
    CleanedFile = []
    dirList=os.listdir(path)
    filedDirList=os.listdir(filed_logs_path)
    for file in dirList:
        AeTitleList = []
        file = path + file
        if os.path.isfile(file):
            f = open(file)
            file_content = f.read() #read returns entire file as a string
            file_content = file_content.strip()
            file_content = file_content.split('\n')
            f.close()
            for i in file_content:
                CleanedFile.append(i)
    for file in filedDirList:
        if not historicalProfile:
            continue
        file = filed_logs_path + file
        f = gzip.open(file, 'rb')
        file_content = f.read() #read returns entire file as a string
        file_content = file_content.strip()
        file_content = file_content.split('\n')
        f.close()
        for i in file_content:
            CleanedFile.append(i)
    return CleanedFile 


def extractEvents(CleanedFile):
    Result = {}
    timestamp = []
    application = []
    host = []
    sourceAE = []
    port = []
    destAE = []
    success = []
    failure = []
    lines = CleanedFile
    for i in range(len(lines)):
        if 'Step 6b' in lines[i]:
            #m = re.match("\[(.{23}) ([^ ]*) ([^ ]*) [^ ]* [^ ]* (.*)", logline)
            m = re.match("\[(.{23}) ([^ ]*) ([^ ]*) [^ ]* [^ ]* [^ ]* Step 6b: Issuing DICOM C-Move request to AE:(.*)  Port:(.*)\.  Destination for study is:(.*)\.", lines[i])
            Timestamp = m.group(1)
            Application = m.group(2)
            Host = m.group(3)
            SourceAE = m.group(4)
            Port = m.group(5)
            DestAE = m.group(6)
            #print "%s,%s,%s,%s,%s,%s"%(Timestamp,Application,Host,SourceAE,Port,DestAE)
            j = i + 1
            #print "%s,%s,%s,%s,%s,%s"%(Timestamp,Application,Host,SourceAE,Port,DestAE)
            for line in lines[j:]:
                Success = 0
                Failure = 0
                if 'Step 6b' in line:
                    break
                elif 'Response indicates' in line:
                    Success  = 1
                    Failure = 0
                    str =  "%s,%s,%s,%s,%s,%s,%s,%s"%(Timestamp,Application,Host,SourceAE,Port,DestAE,Success,Failure)
                    timestamp.append(Timestamp)
                    application.append(Application)
                    host.append(Host)
                    sourceAE.append(SourceAE)
                    port.append(Port)
                    destAE.append(DestAE)
                    success.append(Success)
                    failure.append(Failure)
                    break
                elif 'tried 1 times' in line:
                    Success = 0
                    Failure = 1
                    str = "%s,%s,%s,%s,%s,%s,%s,%s"%(Timestamp,Application,Host,SourceAE,Port,DestAE,Success,Failure)
                    timestamp.append(Timestamp)
                    application.append(Application)
                    host.append(Host)
                    sourceAE.append(SourceAE)
                    port.append(Port)
                    destAE.append(DestAE)
                    success.append(Success)
                    failure.append(Failure)#Result.append(str)
                    #print str
                    break
    Result['Timestamp'] = timestamp
    Result['Application'] = application
    Result['Host'] = host
    Result['SourceAE'] = sourceAE
    Result['Port'] = port
    Result['DestAE'] = destAE
    Result['Success'] = success
    Result['Failure'] = failure
    return Result

def generateStats(Events):
    events = Events
    infile = pd.DataFrame(events)
    Date = infile['Timestamp']
    Date = Date.str.split().str[0]
    Timestamp = pd.DatetimeIndex(infile['Timestamp'])
    Hour = Timestamp.hour
    DailyActivity = infile[['Host','SourceAE','Success','Failure']]
    DailyActivity['Date'] = Date
    DailyReport = DailyActivity.groupby(['Date','Host','SourceAE'],sort=False).agg([np.sum,np.mean,np.size])
    DailyReport.columns = ['Successes','SuccessRate','Total','Failures','FailureRate','Total2']
    cols = ['Total','Successes','Failures','SuccessRate','FailureRate']
    DailyReport = DailyReport[cols]
    DailyReport['SuccessRate'] = DailyReport['SuccessRate'].map(lambda x: '%0.3f' % x)
    DailyReport['FailureRate'] = DailyReport['FailureRate'].map(lambda x: '%0.3f' % x)
    return DailyReport

def main():
        cleanedFile = generateMasterFile(True)
        events = extractEvents(cleanedFile)
        Report = generateStats(events)
        print Report.to_string()
        Report.to_csv('DICOMPrefetch-cmoves-report.csv',header=True)

main()



