from ftplib import FTP
import psycopg2
import os
import csv
from datetime import datetime
import chardet
import pandas as pd
import io


##### SET PROGRAM_MODE ####
###########################
# 1 = Business Type
PROGRAM_MODE = 1

#### SET PATH AND FILENAME ####
###############################
pathFolder = "IOTDB/"
fileName = "dtesterror.csv"

#### SET PATH FOLDER ####
#########################
pathFolderSuccess = pathFolder + "Success/"
pathFolderFail = pathFolder + "Fail/"
pathFolderTemp = pathFolder + "Temp/"

##### CREATE BY ####
####################
CB = "System"

#### ACTIVE STATUS ####
#######################
ACT = "A"

def connection():
    try:
        connection = psycopg2.connect(user = "postgres",
                                      password = "1234",
                                      host = "127.0.0.1",
                                      port = "5432",
                                      database = "iotdb_final")
        print("PostgreSQL, Connection is connected");
        return connection
    except (Exception, psycopg2.Error) as error :
        print ("PostgreSQL, Error while connecting :", str(error).strip())

def ftpconnection():
    ftp = FTP()
    ftp.connect('61.19.250.10', 12122)
    ftp.login('hino', 'hino1234')
    ftp.encoding='utf-8'
    return ftp

tmpLogs = []
def ADDLOG(n):
    tmpLogs.append(n)
    
def getNewFileName(n, d = None, e = 0):
    t = n.split(".")
    if(len(t) > 1):
        if(t[1] == "csv"):
            if e == 0 :
                return t[0] + "_" + str(d) + "." + t[1]
            elif e == 1 :
                return t[0] + "_error" + "_" + str(d) + "." + t[1]
            elif e == 2 :
                return t[0] + "_data_not_exists" + "_" + str(d) + "." + t[1]
            elif e == 3 :
                return t[0] + "_data_is_not_utf_8" + "_" + str(d) + "." + t[1]
            elif e == 4 :
                return t[0] + "_column_is_differenced" + "_" + str(d) + "." + t[1]
            elif e == 5 :
                return t[0] + "_check_require_error" + "_" + str(d) + "." + t[1]
            elif e == 6 :
                return t[0] + "_error" + "_" + str(d) + "_log." + t[1]
        else:
            print("Error : String file name \"" + n + "\" format is not .csv")
            return False
    else :
        print("Error : String file name \"" + n + "\" not have extension ")
        return False

def getDate(n = None):
    if(n == 0):
        return datetime.now().strftime('%Y%m%d')
    elif(n == 1):
        return datetime.now().strftime('%H%M')
    elif(n == 2):
        return datetime.now().strftime('%Y%m%d_%H%M')
    elif(n == 3):
        return datetime.now().strftime('%H%M%S')
    elif(n == 4):
        return datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    else:
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
def moveFile(source, destination):
    try :
        os.rename(source, destination)
        return True
    except Exception as e:
        print(e)
        return False

def removeFile(n):
    try :
        os.remove(n)
        return True
    except Exception as e:
        print("REMOVE FILE : \""+ n + "\" " + str(e).strip())
        return False

def ftpMoveFile(ftp, source, destination):
    try :
        ftp.rename(source, destination)
        return True
    except Exception as e:
        print("FTP MOVE FILE : \""+ source + "\" " + str(e).strip())
        return False

def LOG(c, l, fName, AStatus):
    t = CExecute(c, "INSERT INTO process (process_name, active_status, created_by) VALUES (%s, %s, %s)  RETURNING id;", (str(fName).strip(), str(AStatus).strip(), str(CB).strip()), True);
    t = t[0][0]
    print("LOG : INSERT PROCESS ID : " + str(t))
    for r in l:
        PResult = str(r[0]).strip() + " : "+ str(r[5]).strip()
        print("LOG : PROCESS LOG > " + str(PResult))
        CExecute(c, "INSERT INTO process_log (process_id, start_date_time, end_date_time, is_success, process_result, created_by) VALUES (%s, %s, %s, %s, %s, %s)  RETURNING id;", (str(t).strip(), str(r[3]).strip(), str(r[4]).strip(), str(r[2]).strip(), str(PResult).strip(), str(CB).strip()), True);

def fileExists(n):
    PStart = TimeStamp()
    try:
        fexist = os.path.isfile(n)
        if fexist:
            PResult = "File : \"" + n + "\" is exist"            
            ISSuccess = True
            AStatus = "A"
        else:
            PResult = "File : \"" + n + "\" is not exist"
            ISSuccess = False
            AStatus = "A"
    except Exception as error:
        PResult = "File : " + n + str(error).strip()
    finally:
        ADDLOG(["File Exist", AStatus, ISSuccess, PStart, TimeStamp(), PResult])
        print ("FILE EXIST,", PResult)
        return fexist

def ftpPathExists(ftp, n):
    n = n[:-1]
    if not n in ftp.nlst(pathFolder):
        print("ftpPathExists CREATE : " + n)
        ftp.mkd(n)
    else :
        if n in ftp.nlst(pathFolder):
            print("ftpPathExists FOUND : " + n)
            return True
        else :
            print("ftpPathExists NOT FOUND : " + n)
            return False

def localPathExists(n):
    if not os.path.exists(n):
        os.makedirs(n)
        return True
    else :
        if os.path.exists(n):
            return True
        else :
            return False
        
def TimeStamp():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

def CExecute(c,sth,value, t = None):
    try:
        cursor = c.cursor()
        cursor.execute(sth,value)
        if t == True:
            return cursor.fetchall()
        else:
            return [True,""]
    except (Exception, psycopg2.Error) as error :
        print ("PostgreSQL, Error while execute statement \"" + sth + "\" :", str(error).strip())
        c.rollback()
        print("PostgreSQL, Transection Rollback");
        return [False, str(error).strip()]

def IS_UTF8(fname):
    PStart = TimeStamp()
    try:
        f = open(fname,"rb")
        r = f.read()
        result = chardet.detect(r)
        if(result['encoding'] == "UTF-8-SIG" or result['encoding'] == "UTF-8"):
            #File is UTF-8 or UTF-8-SIG
            PResult = "File : \"" + fname + "\" format is " + result['encoding']
            ISSuccess = True
            AStatus = "A"
        else:
            #File is not UTF-8 or UTF-8-SIG
            PResult = "File : \"" + fname + "\" formar is not UTF-8 or UTF-8-SIG"
            ISSuccess = False
            AStatus = "A"
    except Exception as error:
        #Error cannot open file
        PResult = "File : \"" + fname +"\" " + str(error).strip()
        ISSuccess = False
        AStatus = "A"
    finally:
        f.close()
        ADDLOG(["File Format", AStatus, ISSuccess, PStart, TimeStamp(), PResult])
        print ("FILE FORMAT," , PResult)
        return ISSuccess
        
def HAS_DATA(fname):
    PStart = TimeStamp()
    try:
        df = pd.read_csv(fname)
        if not df.empty:
            PResult = "File : \"" + fname +"\" Data is exists"
            ISSuccess = True
            AStatus = "A"
        else:
            PResult = "File : \"" + fname +"\" Data is not exists"
            ISSuccess = False
            AStatus = "A"
            
    except Exception as error:
        PResult = "File : \"" + fname +"\" " + str(error).strip()
        ISSuccess = False
        AStatus = "A"
    finally:
        ADDLOG(["Has Data", AStatus, ISSuccess, PStart, TimeStamp(), PResult])
        print ("HAS DATA," , PResult)
        return ISSuccess

def getTableName(n):
    #n = 0 : normal table
    #n = 1 : interface table
    if n == 0:
        if PROGRAM_MODE == 1:
            return "vendor_business_type"
    elif n == 1:
        if PROGRAM_MODE == 1:
            return "vendor_business_type_interface"

def getSchema(c, n):
    tbn = getTableName(n)
    sth = "SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = '"+tbn+"';"
    return CExecute(c, sth, None, True)

def getHeaderCSVFile(fn):
    with open(fn, encoding = "UTF-8-SIG") as f:
        d_reader = csv.DictReader(f)
        return d_reader.fieldnames
    
def CHK_COLUMN(c, fname):
    PStart = TimeStamp()
    try:
        cdb = len(getSchema(c, 1))
        f = getHeaderCSVFile(fname)
        cf = len(f)
        if cf == cdb:
            PResult = "File : \"" + fname +"\" has " + str(cf) + "columns equal in the database"
            ISSuccess = True
            AStatus = "A"
        else:
            PResult = "File : \"" + fname +"\" has " + str(cf) + " columns not equal in the database : " + str(f)
            ISSuccess = False
            AStatus = "A"
    except Exception as error:
        PResult = "File : \"" + fname +"\" has some error : " + str(error).strip()
        ISSuccess = False
        AStatus = "A"
    finally:
        ADDLOG(["Check Column", AStatus, ISSuccess, PStart, TimeStamp(), PResult])
        print ("CHK COLUMN," , PResult)
        return ISSuccess

def CLEAR_INTERFACE(c):
    if PROGRAM_MODE == 1:
        CExecute(c, "DELETE FROM vendor_business_type_interface", None, False)

def GET_STH_INSERT_INTERFACE():
    if PROGRAM_MODE == 1:
        return "INSERT INTO vendor_business_type_interface (business_type_code, business_type_name, created_date_time, created_by) VALUES(%s , %s, %s, %s);"

def GET_STH_INSERT_NORMAL():
    if PROGRAM_MODE == 1:
        return "INSERT INTO vendor_business_type (business_type_code, business_type_name, created_date_time, created_by, active_status) VALUES(%s , %s, %s, %s, %s);"

def GET_STH_UPDATE():
    if PROGRAM_MODE == 1:
        return "UPDATE vendor_business_type SET business_type_name = %s, updated_date_time = %s, updated_by = %s, active_status = %s WHERE business_type_code = %s;"

def GET_STH_SELECT(chkcode):
    if chkcode:
        if PROGRAM_MODE == 1:
            return "SELECT COUNT(*) FROM vendor_business_type WHERE business_type_code = %s;"
        
def createLogFile(pFile, r, ftp, ftpPath, ftpFileName):
    try :
        with open(pFile, 'w', newline='') as myfile:
            print("CREATED CSV LOG FILE : " + pFile)
            wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
            a = ["row", "error", "values"]
            wr.writerow(a)
            i = 0;
            for v in r:
                i = i + 1
                if not v[0] == True:
                    wr.writerow([i] + [v[1]] + [list(v[2].items())])
                    
        with open(pFile, 'rb') as myfile:
            print('STOR ' + ftpFileName)
            ftp.cwd(ftpPath)
            ftp.storbinary('STOR ' + ftpFileName, myfile)
            ftp.cwd("/")
            print("UPLOAD LOG FILES ERROR TO : " + ftpPath + ftpFileName + " successed")
        return True
    except Exception as error:
        print("CREATE LOG FILES ERROR : " + str(error).strip())
        return False

def CHK_REQUIRE(pFile):
    try:
        c = True
        t = []
        with open(pFile, encoding = "UTF-8-SIG") as f:
            for r in csv.DictReader(f):
                if PROGRAM_MODE == 1:
                    if (not r['business_type_name'] == None) and (not r['business_type_code'] == None) and (not r['business_type_name'] == "") and (not r['business_type_code'] == ""):
                        t.append([True, "", r])
                    else:
                        if (r['business_type_name'] == None) or (r['business_type_name'] == "") and (r['business_type_code'] == None) or (r['business_type_code'] == ""):
                            e = "business_type_name and business_type_code is None"
                        elif (r['business_type_name'] == None) or (r['business_type_name'] == ""):
                            e = "business_type_name is None"
                        else:
                            e = "business_type_code is None"
                        c = False
                        t.append([False, e, r])
    except Exception as error :
        print("CHECK REQUIRE : " + str(error).strip())
    finally :
        if c == True:
            return True
        else:
            localFile = pathFolderTemp + getNewFileName(fileName, getDate(2), 5)
            ftpFile = pathFolderFail
            createLogFile(localFile, t, ftp, ftpFile, getNewFileName(fileName, getDate(2), 6))
            removeFile(localFile)
            return False

def INSERT_INTERFACE(c, pFile, ftp):
    PStart = TimeStamp()
    print("INSERT INTERFACE : Transection Created")
    if PROGRAM_MODE == 1:
        try:
            chk = True
            t = []
            with open(pFile, encoding = "UTF-8-SIG") as f:
                for r in csv.DictReader(f):
                    print("INSERT INTERFACE : Insert code \"" + str(r['business_type_code']).strip() + "\" to interface table")
                    v = CExecute(c, GET_STH_INSERT_INTERFACE(), [str(r['business_type_code']).strip(), str(r['business_type_name']).strip(), str(PStart).strip(), str(CB).strip()], False)
                    if v[0] == False:
                        chk = False
                        t.append([False, v[1], r])
        except Exception as error :
            print("INSERT INTERFACE : " + str(error).strip())
        finally :
            if chk == True:
                print("INSERT INTERFACE : Successed")
                ADDLOG(["Insert Interface", "A", chk, PStart, TimeStamp(), "Insert Interface Successed"])
                return True
            else:
                localFile = pathFolderTemp + getNewFileName(fileName, getDate(2), 1)
                ftpFile = pathFolderFail
                createLogFile(localFile, t, ftp, ftpFile, getNewFileName(fileName, getDate(2), 6))
                removeFile(localFile)
                ADDLOG(["Insert Interface", "A", chk, PStart, TimeStamp(), "Insert Interface Falied Transection rollback"])
                return False

def INSERT_NORMAL(c, pFile):
    PStart = TimeStamp()
    print("INSERT NORMAL : Transection Created")
    if PROGRAM_MODE == 1:
        try:
            with open(pFile, encoding = "UTF-8-SIG") as f:
                for r in csv.DictReader(f):
                    v = CExecute(c, GET_STH_SELECT(True), [str(r['business_type_code']).strip()], True)
                    if(v[0][0] == 1):
                        print("INSERT NORMAL : Code \"" + str(r['business_type_code']).strip() + "\" is duplicate, Update where code = \"" + str(r['business_type_code']).strip() + "\" to normal table")
                        CExecute(c, GET_STH_UPDATE(), [str(r['business_type_name']).strip(), str(PStart).strip(), str(CB).strip(), str(ACT).strip(), str(r['business_type_code']).strip()], False)
                    else:
                        print("INSERT NORMAL : Insert code \"" + str(r['business_type_name']).strip() + "\" to normal table")
                        CExecute(c, GET_STH_INSERT_NORMAL(), [str(r['business_type_code']).strip(), str(r['business_type_name']).strip(), str(PStart).strip(), str(CB).strip(), str(ACT).strip()], False)
        except Exception as error :
            print("INSERT NORMAL : Error > " + str(error).strip())
            ADDLOG(["Insert Normal", "A", False, PStart, TimeStamp(), "Insert Normal Falied Transection rollback"])
        finally :
            print("INSERT NORMAL : Successed")
            ADDLOG(["Insert Normal", "A", True, PStart, TimeStamp(), "Insert Normal Successed Transection commit"])

def getFile(ftp, fName, lName):
    try:
        localfile = open(lName, 'wb')
        ftp.retrbinary('RETR ' + fName, localfile.write, 1024)
        localfile.close()
        return True
    except Exception as error:
        print("Get File Error : " + str(error).strip())
        return False

def Process():
    c = connection()
    ftp = ftpconnection()
    PROCESSPASSED = False
    tmp = getNewFileName(fileName, getDate(4), 0)
    pathFile = pathFolderTemp + tmp
    ftpFile = pathFolder + fileName
    fn = ""
    try:
        if(ftpPathExists(ftp, pathFolderSuccess) and ftpPathExists(ftp, pathFolderFail) and localPathExists(pathFolderTemp)):
            if(getFile(ftp, ftpFile, pathFile)):
                if(fileExists(pathFile)):
                    if(IS_UTF8(pathFile)):
                        if HAS_DATA(pathFile):
                            if CHK_COLUMN(c, pathFile):
                                if CHK_REQUIRE(pathFile):
                                    if INSERT_INTERFACE(c, pathFile, ftp):
                                        INSERT_NORMAL(c, pathFile)
                                        CLEAR_INTERFACE(c)
                                        PROCESSPASSED = True
                                        fn = pathFolderSuccess + getNewFileName(fileName, getDate(2), 0)
                                        ftpMoveFile(ftp, ftpFile, fn)
                                        print("MOVED FILE \"" + fileName + "\" to : " + fn)
                                    else:
                                        #INSERT INTERFACE FAILED
                                        fn = pathFolderFail + getNewFileName(fileName, getDate(2), 0)
                                        ftpMoveFile(ftp, ftpFile, fn)
                                        print("MOVED FILE \"" + fileName + "\" to : " + fn)
                                else:
                                    #CHECK REQUIRE FAILED
                                    print("CREATED CSV LOG FILE : " + pathFolderFail + getNewFileName(fileName, getDate(2), 5))
                                    fn = pathFolderFail + getNewFileName(fileName, getDate(2), 0)
                                    ftpMoveFile(ftp, ftpFile, fn)
                                    print("MOVED FILE \"" + fileName + "\" to : " + fn)
                            else:
                                #COLUMN IS DIFFERENCED
                                fn = pathFolderFail + getNewFileName(fileName, getDate(2), 4)
                                print("MOVED FILE \"" + fileName + "\" to : " + fn)
                                ftpMoveFile(ftp, ftpFile, pathFolderFail + fn)
                        else:
                            #DATA IS NOT EXIST
                            fn = pathFolderFail + getNewFileName(fileName, getDate(2), 2)
                            print("MOVE FILE \"" + fileName + "\" to : " + fn)
                            ftpMoveFile(ftp, ftpFile, fn)
                    else:
                        #FILE IS NOT UTF8
                        fn = pathFolderFail + getNewFileName(fileName, getDate(2), 3)
                        print("MOVE FILE \"" + fileName + "\" to : " + fn)
                        ftpMoveFile(ftpFile, fn)
            else:
                print("Cannot download file form ftp server.")
    except (Exception) as error :
        print ("Process, Some thing error while processing : ", str(error).strip())
    finally:
        if c is not None:
            if PROCESSPASSED:
                c.commit()
                if len(tmpLogs) > 0:
                    print("PostgreSQL, Create Transection Logs");
                    LOG(c, tmpLogs, fn, "A")
                print("PostgreSQL, All Transection Commit");
            else:
                c.rollback()
                print("PostgreSQL, Transection Rollback");
                if len(tmpLogs) > 0:
                    print("PostgreSQL, Create Transection Logs");
                    if(LOG(c, tmpLogs, fn, "A")):
                        print("PostgreSQL, Transection Logs Commit");
                    else:
                        print("PostgreSQL, Transection Logs Rollback");
            removeFile(pathFile)
            c.close()
            print("PostgreSQL, Connection is closed");
            
if(__name__ == '__main__'):
    Process()
