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
fileName = "dtest.csv"

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
        print(e)
        return False

def ftpMoveFile(ftp, source, destination):
    try :
        ftp.rename(source, destination)
        return True
    except Exception as e:
        print(e)
        return False

def fileExists(c, n, PStart):
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
        LOG(c, "File Exist", AStatus, ISSuccess, PStart, TimeStamp(), PResult)
        print ("FILE EXIST,", PResult)
        c.commit() 
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

def LOG(c, PName, AStatus, ISSuccess, SDate, EDate, PResult):
    t = CExecute(c, "INSERT INTO process (process_name, active_status, created_by) VALUES (%s, %s, %s)  RETURNING id;", (str(PName).strip(), str(AStatus).strip(), str(CB).strip()), True);
    t = t[0][0]
    CExecute(c, "INSERT INTO process_log (process_id, start_date_time, end_date_time, is_success, process_result, created_by) VALUES (%s, %s, %s, %s, %s, %s)  RETURNING id;", (str(t).strip(), str(SDate).strip(), str(EDate).strip(), str(ISSuccess).strip(), str(PResult).strip(), str(CB).strip()), True);

def IS_UTF8(c, fname, PStart):
    try:
        f = open(fname,"rb")
        r = f.read()
        result = chardet.detect(r)
        if(result['encoding'] == "UTF-8-SIG" or result['encoding'] == "UTF-8"):
            #File is UTF-8 or UTF-8-SIG
            PResult = "File : \"" + fileName + "\" format is " + result['encoding']
            ISSuccess = True
            AStatus = "A"
        else:
            #File is not UTF-8 or UTF-8-SIG
            PResult = "File : \"" + fileName + "\" formar is not UTF-8 or UTF-8-SIG"
            ISSuccess = False
            AStatus = "A"
    except Exception as error:
        #Error cannot open file
        PResult = "File : \"" + fileName +"\" " + str(error).strip()
        ISSuccess = False
        AStatus = "A"
    finally:
        f.close()
        LOG(c, "File Format", AStatus, ISSuccess, PStart, TimeStamp(), PResult)
        c.commit()
        print ("FILE FORMAT," , PResult)
        return ISSuccess
        
def HAS_DATA(c, fname, PStart):
    try:
        df = pd.read_csv(fname)
        if not df.empty:
            PResult = "File : \"" + fileName +"\" Data is exists"
            ISSuccess = True
            AStatus = "A"
        else:
            PResult = "File : \"" + fileName +"\" Data is not exists"
            ISSuccess = False
            AStatus = "A"
            
    except Exception as error:
        PResult = "File : \"" + fileName +"\" " + str(error).strip()
        ISSuccess = False
        AStatus = "A"
    finally:
        LOG(c, "File Format", AStatus, ISSuccess, PStart, TimeStamp(), PResult)
        c.commit()
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
    
def CHK_COLUMN(c, fname, PStart):
    try:
        cdb = len(getSchema(c, 1))
        f = getHeaderCSVFile(fname)
        cf = len(f)
        if cf == cdb:
            PResult = "File : \"" + fileName +"\" has " + str(cf) + "columns equal in the database"
            ISSuccess = True
            AStatus = "A"
        else:
            PResult = "File : \"" + fileName +"\" has " + str(cf) + " columns not equal in the database : " + str(f)
            ISSuccess = False
            AStatus = "A"
    except Exception as error:
        PResult = "File : \"" + fileName +"\" has some error : " + error
        ISSuccess = False
        AStatus = "A"
    finally:
        LOG(c, "Check Column", AStatus, ISSuccess, PStart, TimeStamp(), PResult)
        c.commit()
        print ("CHK COLUMN," , PResult)
        return ISSuccess

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
            createLogFile(pathFolderFail + getNewFileName(fileName, getDate(2), 5), t)
            return False
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
        
def createLogFile(pFile, r):
    try :
        with open(pFile, 'w', newline='') as myfile:
            wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
            a = ["row", "error", "values"]
            wr.writerow(a)
            i = 0;
            for v in r:
                i = i + 1
                if not v[0] == True:
                    wr.writerow([i] + [v[1]] + [list(v[2].items())])
        return True
    except Exception as error:
        print("CREATE LOG FILES ERROR : " + str(error).strip())
        return False

def INSERT_INTERFACE(c, pFile, dCreate):
    if PROGRAM_MODE == 1:
        try:
            chk = True
            t = []
            with open(pFile, encoding = "UTF-8-SIG") as f:
                for r in csv.DictReader(f):
                    v = CExecute(c, GET_STH_INSERT_INTERFACE(), [str(r['business_type_code']).strip(), str(r['business_type_name']).strip(), str(dCreate).strip(), str(CB).strip()], False)
                    if v[0] == False:
                        chk = False
                        t.append([False, v[1], r])
        except Exception as error :
            print("INSERT INTERFACE : " + str(error).strip())
        finally :
            if chk == True:
                print("INSERT INTERFACE PASSED")
                return True
            else:
                createLogFile(pathFolderFail + getNewFileName(fileName, getDate(2), 1), t)
                return False

def INSERT_NORMAL(c, pFile, dCreate):
    if PROGRAM_MODE == 1:
        try:
            with open(pFile, encoding = "UTF-8-SIG") as f:
                for r in csv.DictReader(f):
                    v = CExecute(c, GET_STH_SELECT(True), [str(r['business_type_code']).strip()], True)
                    if(v[0][0] == 1):
                        CExecute(c, GET_STH_UPDATE(), [str(r['business_type_name']).strip(), str(dCreate).strip(), str(CB).strip(), str(ACT).strip(), str(r['business_type_code']).strip()], False)
                    else:
                        CExecute(c, GET_STH_INSERT_NORMAL(), [str(r['business_type_code']).strip(), str(r['business_type_name']).strip(), str(dCreate).strip(), str(CB).strip(), str(ACT).strip()], False)
        except Exception as error :
            print("INSERT NORMAL : " + str(error).strip())
        finally :
            print("INSERT NORMAL PASSED")

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
    PStart = TimeStamp()
    c = connection()
    ftp = ftpconnection()
    PROCESSPASSED = False
    tmp = getNewFileName(fileName, getDate(4), 0)
    pathFile = pathFolderTemp + tmp
    ftpFile = pathFolder + fileName
    try:
        if(ftpPathExists(ftp, pathFolderSuccess) and ftpPathExists(ftp, pathFolderFail) and localPathExists(pathFolderTemp)):
            if(getFile(ftp, ftpFile, pathFile)):
                if(fileExists(c, pathFile, PStart)):
                    if(IS_UTF8(c, pathFile, PStart)):
                        if HAS_DATA(c, pathFile, PStart):
                            if CHK_COLUMN(c, pathFile, PStart):
                                if CHK_REQUIRE(pathFile):
                                    if INSERT_INTERFACE(c, pathFile, PStart):
                                        INSERT_NORMAL(c, pathFile, PStart)
                                        CLEAR_INTERFACE(c)
                                        PROCESSPASSED = True
                                        ftpMoveFile(ftp, ftpFile, pathFolderSuccess + getNewFileName(fileName, getDate(2), 0))
                                        print("MOVED FILE \"" + fileName + "\" to : " + pathFolderSuccess + getNewFileName(fileName, getDate(2), 0))
                                    else:
                                        #INSERT INTERFACE FAILED
                                        print("CREATED CSV LOG FILE : " + pathFolderFail + getNewFileName(fileName, getDate(2), 1))
                                        ftpMoveFile(ftp, ftpFile, pathFolderFail + getNewFileName(fileName, getDate(2), 0))
                                        print("MOVED FILE \"" + fileName + "\" to : " + pathFolderFail + getNewFileName(fileName, getDate(2), 0))
                                else:
                                    #CHECK REQUIRE FAILED
                                    print("CREATED CSV LOG FILE : " + pathFolderFail + getNewFileName(fileName, getDate(2), 5))
                                    ftpMoveFile(ftp, ftpFile, pathFolderFail + getNewFileName(fileName, getDate(2), 0))
                                    print("MOVED FILE \"" + fileName + "\" to : " + pathFolderFail + getNewFileName(fileName, getDate(2), 0))
                            else:
                                #COLUMN IS DIFFERENCED
                                print("MOVED FILE \"" + fileName + "\" to : " + pathFolderFail + getNewFileName(fileName, getDate(2), 4))
                                ftpMoveFile(ftp, ftpFile, pathFolderFail + getNewFileName(fileName, getDate(2), 4))
                        else:
                            #DATA IS NOT EXIST
                            print("MOVE FILE \"" + fileName + "\" to : " + pathFolderFail + getNewFileName(fileName, getDate(2), 2))
                            ftpMoveFile(ftp, ftpFile, pathFolderFail + getNewFileName(fileName, getDate(2), 2))
                    else:
                        #FILE IS NOT UTF8
                        print("MOVE FILE \"" + fileName + "\" to : " + pathFolderFail + getNewFileName(fileName, getDate(2), 3))
                        ftpMoveFile(ftpFile, pathFolderFail + getNewFileName(fileName, getDate(2), 3))
            else:
                print("Cannot download file form ftp server.")
    except (Exception) as error :
        print ("Process, Some thing error while processing : ", str(error).strip())
    finally:
        if c is not None:
            if PROCESSPASSED:
                c.commit()
                removeFile(pathFile)
                print("PostgreSQL, Transection Commit");
            else:
                c.rollback()
                print("PostgreSQL, Transection Rollback");
            c.close()
            print("PostgreSQL, Connection is closed");

if(__name__ == '__main__'):
    Process()
    
