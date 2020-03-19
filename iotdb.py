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
fileName = "utf16.csv"

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
        connection = psycopg2.connect(user = "admin",
                                      password = "admingps",
                                      host = "18.139.21.250",
                                      port = "5432",
                                      database = "iotdb_final")
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

def INS_LOGID(fName, AStatus):
    c = connection()
    t = CExecute(c, "INSERT INTO process (process_name, active_status, created_by) VALUES (%s, %s, %s)  RETURNING id;", (str(fName).strip(), str(AStatus).strip(), str(CB).strip()), True);
    c.commit()
    print("LOG : INSERT PROCESS ID : " + str(t[0][0]))
    return t[0][0]

def LOG(r, t):
    con = connection()
    try:
        CExecute(con, "INSERT INTO process_log (process_id, start_date_time, end_date_time, is_success, process_result, created_by) VALUES (%s, %s, %s, %s, %s, %s)  RETURNING id;", (str(t).strip(), str(r[3]).strip(), str(r[4]).strip(), str(r[2]).strip(), str(r[0]).strip(), str(CB).strip()), True);
        #print("LOG : PROCESS LOG > " + str(r[0]).strip() + ", " + str(r[2]).strip())
    except Exception as error:
        con.rollback()
        print("LOG : Error " + str(error).strip())
    finally:
        con.commit()

def fileExists(n, logid):
    PStart = TimeStamp()
    try:
        fexist = os.path.isfile(n)
        if fexist:
            PResult = "File : \"" + n + "\" is exist"
        else:
            PResult = "File : \"" + n + "\" is not exist"
            LOG(["File Exist", "A", False, PStart, TimeStamp(), ""], logid)
    except Exception as error:
        PResult = "File : " + n + str(error).strip()
        ADDLOG(["File Exist", "A", False, PStart, TimeStamp(), ""], logid)
    finally:
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
    finally:
        pass
    

def IS_UTF8(fname, logid):
    PStart = TimeStamp()
    ISSuccess = False
    try:
        f = open(fname,"rb")
        r = f.read()
        result = chardet.detect(r)
        if(result['encoding'] == "UTF-8-SIG" or result['encoding'] == "UTF-8"):
            #File is UTF-8 or UTF-8-SIG
            PResult = "File : \"" + fname + "\" format is " + result['encoding']
            ISSuccess = True
        else:
            #File is not UTF-8 or UTF-8-SIG
            PResult = "File : \"" + fname + "\" formar is not UTF-8 or UTF-8-SIG"
            LOG(["File not utf-8 format", "A", ISSuccess, PStart, TimeStamp(), ""], logid)
    except Exception as error:
        #Error cannot open file
        PResult = "File : \"" + fname +"\" " + str(error).strip()
        #LOG([str(error).strip(), "A", ISSuccess, PStart, TimeStamp(), ""], logid)
    finally:
        f.close()
        print (PResult)
        return ISSuccess
        
def HAS_DATA(fname, logid):
    PStart = TimeStamp()
    ISSuccess = False
    try:
        with open(fname, encoding = "UTF-8-SIG") as f:
            readCSV = csv.reader(f, delimiter='|')
            row_count = sum(1 for row in readCSV)
            if row_count > 0:
                PResult = "File : \"" + fname +"\" Data is exists"
                ISSuccess = True
            else:
                PResult = "File : \"" + fname +"\" Data is not exists"
                LOG(["No data in file", "A", ISSuccess, PStart, TimeStamp(), ""], logid)
    except Exception as error:
        PResult = "File : \"" + fname +"\" No data in file"
        LOG(["No data in file", "A", ISSuccess, PStart, TimeStamp(), ""], logid)
    finally:
        print ("HAS_DATA > " + PResult)
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
    sth = "SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = '"+tbn+"' and column_name != 'created_date_time' and column_name != 'created_by';"
    #sth = "SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = '"+tbn+"';"
    return CExecute(c, sth, None, True)

def getHeaderCSVFile(fn):
    with open(fn, encoding = "UTF-8-SIG") as f:
        d_reader = csv.DictReader(f)
        return d_reader.fieldnames

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

def CHK_COLUMN(c, fname, logid):
    PStart = TimeStamp()
    try:
        tmpchk = True
        cdb = len(getSchema(c, 1))
        with open(fname, encoding = "UTF-8-SIG") as csvfile:
            readCSV = csv.reader(csvfile, delimiter='|')
            i = 0
            for row in readCSV:
                i = i + 1
                if len(row) != cdb:
                    tmpchk = False
                    print ("CHK_COLUMN > " + "File : \"" + fname +"\", ROW : \"" + str(i) +"\" has " + str(len(row)) + " columns not equal in the database")
        if tmpchk:
            print ("CHK_COLUMN > " + "File : \"" + fname +"\" all row has " + str(cdb) + " columns equal in the database")
        else:
            LOG(["Check Column", "A", tmpchk, PStart, TimeStamp(), ""], logid)
    except Exception as error:
        print ("CHK_COLUMN > " + "File : \"" + fname +"\" has some error : " + str(error).strip())
        LOG(["Data not match table column", AStatus, False, PStart, TimeStamp(), ""], logid)
    finally:
        return tmpchk

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
                    wr.writerow([i] + [v[1]] + [list(v[2])])
                    
        with open(pFile, 'rb') as myfile:
            ftp.cwd(ftpPath)
            ftp.storbinary('STOR ' + ftpFileName, myfile)
            ftp.cwd("/")
            print("createLogFile > UPLOAD LOG FILES ERROR TO : " + ftpPath + ftpFileName + " successed")
        return True
    except Exception as error:
        print("createLogFile > ERROR : " + str(error).strip())
        return False

def CHK_REQUIRE(pFile, logid, ftp):
    PStart = TimeStamp()
    c = True
    t = []
    try:
        with open(pFile, encoding = "UTF-8-SIG") as f:
            for r in csv.reader(f, delimiter='|'):
                if PROGRAM_MODE == 1:
                    if (not r[0] == None) and (not r[1] == None) and (not r[0] == "") and (not r[1] == ""):
                        t.append([True, "", r])
                    else:
                        c = False
                        t.append([False, "Check require failed", r])
    except Exception as error :
        print("CHECK_REQUIRE : " + str(error).strip())
    finally :
        if c == True:
            print("CHECK_REQUIRE : Successed")
            return True
        else:
            print("CHECK_REQUIRE : Failed")
            localFile = pathFolderTemp + getNewFileName(fileName, getDate(2), 5)
            ftpFile = pathFolderFail
            createLogFile(localFile, t, ftp, ftpFile, getNewFileName(fileName, getDate(2), 6))
            removeFile(localFile)
            LOG(["Reqiure data failed", "A", False, PStart, TimeStamp(), ""], logid)
            return False

def INSERT_INTERFACE(c, pFile, ftp, logid):
    PStart = TimeStamp()
    print("INSERT INTERFACE : Transection Created")
    if PROGRAM_MODE == 1:
        try:
            chk = True
            t = []
            with open(pFile, encoding = "UTF-8-SIG") as f:
                for r in csv.reader(f, delimiter='|'):
                    print("INSERT INTERFACE : Insert code \"" + str(r[0]).strip() + "\" to interface table")
                    v = CExecute(c, GET_STH_INSERT_INTERFACE(), [str(r[0]).strip(), str(r[1]).strip(), str(PStart).strip(), str(CB).strip()], False)
                    if v[0] == False:
                        chk = False
                        t.append([False, v[1], r])
        except Exception as error :
            chk = False
            print("INSERT INTERFACE : " + str(error).strip())
        finally :
            if chk == True:
                print("INSERT INTERFACE : Successed")
            else:
                localFile = pathFolderTemp + getNewFileName(fileName, getDate(2), 1)
                ftpFile = pathFolderFail
                createLogFile(localFile, t, ftp, ftpFile, getNewFileName(fileName, getDate(2), 6))
                removeFile(localFile)
            return chk
            LOG(["Insert Interface", "A", chk, PStart, TimeStamp(), ""], logid)

def INSERT_NORMAL(c, pFile, logid):
    PStart = TimeStamp()
    print("INSERT NORMAL : Transection Created")
    if PROGRAM_MODE == 1:
        try:
            with open(pFile, encoding = "UTF-8-SIG") as f:
                for r in csv.reader(f, delimiter='|'):
                    v = CExecute(c, GET_STH_SELECT(True), [str(r[0]).strip()], True)
                    if(v[0][0] == 1):
                        print("INSERT NORMAL : Code \"" + str(r[0]).strip() + "\" is duplicate, Update where code = \"" + str(r[0]).strip() + "\" to normal table")
                        CExecute(c, GET_STH_UPDATE(), [str(r[1]).strip(), str(PStart).strip(), str(CB).strip(), str(ACT).strip(), str(r[0]).strip()], False)
                    else:
                        print("INSERT NORMAL : Insert code \"" + str(r[0]).strip() + "\" to normal table")
                        CExecute(c, GET_STH_INSERT_NORMAL(), [str(r[0]).strip(), str(r[1]).strip(), str(PStart).strip(), str(CB).strip(), str(ACT).strip()], False)
        except Exception as error :
            print("INSERT NORMAL : Error > " + str(error).strip())
            LOG(["Insert Normal", "A", False, PStart, TimeStamp(), ""], logid)
        finally :
            print("INSERT NORMAL : Successed")
            LOG(["Insert Normal", "A", True, PStart, TimeStamp(), ""], logid)

def getFile(ftp, fName, lName, logid):
    PStart = TimeStamp()
    try:
        localfile = open(lName, 'wb')
        ftp.retrbinary('RETR ' + fName, localfile.write, 1024)
        localfile.close()
        return True
    except Exception as error:
        LOG(["File not found", "A", False, PStart, TimeStamp(), ""], logid)
        print("Get File Error : " + str(error).strip())
        return False

def Process():
    print("PostgreSQL, Connection is connected");
    c = connection()
    ftp = ftpconnection()
    PROCESSPASSED = False
    tmp = getNewFileName(fileName, getDate(4), 0)
    pathFile = pathFolderTemp + tmp
    ftpFile = pathFolder + fileName
    fn = ""
    LOGID = INS_LOGID(tmp, "A")
    try:
        if(ftpPathExists(ftp, pathFolderSuccess) and ftpPathExists(ftp, pathFolderFail) and localPathExists(pathFolderTemp)):
            if(getFile(ftp, ftpFile, pathFile, LOGID)):
                if(fileExists(pathFile, LOGID)):
                    if(IS_UTF8(pathFile, LOGID)):
                        if HAS_DATA(pathFile, LOGID):
                            if CHK_COLUMN(c, pathFile, LOGID):
                                if CHK_REQUIRE(pathFile, LOGID, ftp):
                                    if INSERT_INTERFACE(c, pathFile, ftp, LOGID):
                                        INSERT_NORMAL(c, pathFile, LOGID)
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
                                fn = pathFolderFail + getNewFileName(fileName, getDate(2), 0)
                                print("MOVED FILE \"" + fileName + "\" to : " + fn)
                                ftpMoveFile(ftp, ftpFile, fn)
                        else:
                            #DATA IS NOT EXIST
                            fn = pathFolderFail + getNewFileName(fileName, getDate(2), 0)
                            print("MOVE FILE \"" + fileName + "\" to : " + fn)
                            ftpMoveFile(ftp, ftpFile, fn)
                    else:
                        #FILE IS NOT UTF8
                        fn = pathFolderFail + getNewFileName(fileName, getDate(2), 0)
                        print("MOVE FILE \"" + fileName + "\" to : " + fn)
                        ftpMoveFile(ftp, ftpFile, fn)
            else:
                print("Cannot download file form ftp server.")
    except (Exception) as error :
        print ("Process, Some thing error while processing : ", str(error).strip())
    finally:
        if c is not None:
            if PROCESSPASSED:
                c.commit()
                print("PostgreSQL, All Transection Commit");
            else:
                c.rollback()
                print("PostgreSQL, All Transection Rollback");
            removeFile(pathFile)
            c.close()
            print("PostgreSQL, Connection is closed");
            
if(__name__ == '__main__'):
    Process()
