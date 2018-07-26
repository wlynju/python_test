#!/usr/bin/python  
# -*- coding: utf-8 -*-

'''
 (*) Upload GFS from local to 26

 wly,2018/07/25
'''

import os, sys, platform, glob, argparse
import socket,ftplib
from datetime import *
import time, shutil, threading

#-- Global path
GFSGribPath='F:/data/output/fruit/grib_gfs'

#--Script path
ScriptPath = os.path.dirname(os.path.realpath(__file__)) 

#-----------------------------------------------------------
# Cd a ftp path, if path not exist, create it recusively
# e.g.
#    MFtpCd( ftp, '/this/is/a/example'
# 
# Code copy from stackoverflow, by lecnt
# sunzhang, 2017-09-09
#-----------------------------------------------------------
def CheckSize( filePath ):
    return True if os.path.getsize(filePath) > 1024*1024*5 else False
    #return True if os.path.getsize(filePath) > 1 else False
	
def MFtpCd( ftp, path ):
    if path == '':
        return
    
    print( 'Current path = ' + path )

    try:
        ftp.cwd( path )
    except:
        MFtpCd( ftp, '/'.join( path.split('/')[:-1]) )
        ftp.mkd( path )
        ftp.cwd( path )

#---------------------------------------
def MFtpUpload( server, port, ftpUser, ftpPass, localFile, remoteFile ) : 
    #-- Connect to server
    try:  
        ftp = ftplib.FTP(server)  
    except (socket.error, socket.gaierror):  
        print('ERROR:cannot reach " %s"' % server)  
        return
    print('***Connected to host "%s"' % server)
    try:  
        ftp.login(ftpUser,ftpPass)  
    except ftplib.error_perm:  
        print('ERROR: login failed')  
        ftp.quit()  
        return
    print('***Login ok ' + server)
    if not os.path.exists( localFile ):
        ftp.quit()
        print( 'Local file not exist : ' + localFile )
        return

   #-- Create remote dir if need
    MFtpCd( ftp, os.path.dirname(remoteFile) )
    if (CheckSize( localFile )):
        file = open( localFile ,'rb') 
        ftp.storbinary('STOR ' + remoteFile, file)
        file.close()
        print( '[MFtp] Upload done, remote file is ' + remoteFile )
        return
    ftp.quit()

#-------------------------------------
def FtpToServer( dtTask, localFile ):
    #print('send to 91')
    #return
	taskStr = dtTask.strftime('%Y.%m/%Y%m%d%H')+"/"	
	remoteFile = os.path.join('/disk/cat/fruit/grib_gfs/',taskStr, os.path.basename( localFile ))
	MFtpUpload( '10.135.34.26', '527', 'root', '86042300', localFile, remoteFile )

#------------------------------------
def ToDoTask( dtTask, fh, fileType = 'grib2',isSend = False ):
    taskStr = dtTask.strftime('%Y.%m/%Y%m%d%H')	
    LocalDir = os.path.join( GFSGribPath, taskStr)
  #  print("OK2"+LocalDir)
    if os.path.isdir(LocalDir) == False:	return False
    typePrefix = 'gfs' if fileType == 'grib2' else 'gfs'
    typeSuffix = 'grib2' if fileType == 'grib2' else 'nc'
    typeDirName = 'grib_gfs' if fileType == 'grib2' else 'nc_gfs'

    baseFile = typePrefix + '.I' + dtTask.strftime('%Y%m%d%H') + '.%03d'%fh + '.F' + (dtTask + timedelta(hours=fh)).strftime('%Y%m%d%H') + '.' + typeSuffix
    srcFile = baseFile
    #os.system( )
    t_path = os.path.join( GFSGribPath, taskStr, baseFile)
    files = glob.glob(os.path.join( GFSGribPath, taskStr, baseFile))
    print(t_path)
    if len( files ) < 1 :
        print('[Error] No GFS grib generated !')
        return

    for f in files:
        print( '[Info] Found generated file : ' + f )
       # print("ok4"+os.path.abspath(f))
        if isSend:
            FtpToServer( dtTask,f )

    return False
	
#------------------------------------
def DoTask( dtTask, fileType = 'grib2', isSend = False ):
    #-- First download every 3h
    for fh in range(0, 97, 3):
       ToDoTask( dtTask, fh, fileType, isSend )
    return
	
#------------------------------------
#- Main program
if __name__ == '__main__':
    #-- Argument 
    if len(sys.argv) == 1 :
        os.system('python ' + os.path.realpath(__file__) + ' -h')
        quit()
    parser = argparse.ArgumentParser( description = 'Direct model output')
    parser.add_argument('-u', help='Upload GFS GRIB2 file to server', action='store_true')
    parser.add_argument('--grib', help='Upload grib', action='store_true')
    parser.add_argument('--nc', help='Upload nc, this is default', action='store_true')
    parser.add_argument('-d', metavar='N', help='Latest N days', type=int)
    parser.add_argument('-s', metavar='yymmdd', help='Start date of historical run')
    parser.add_argument('-e', metavar='yymmdd', help='End date of historical run')
    parser.add_argument('timeString', metavar='yymmddHH', help='Initial date in CST, 00 for real for real time task', nargs='*', default='00')
      
    args = parser.parse_args()
    isSend = True if args.u else False
    fileType = 'grib2' if args.grib or not args.nc else 'nc'
	
    #-- Give out time stamp
    print( '|================================>' )
    print( datetime.now().strftime('%Y-%m-%d %H:%M:%S') )
    print( 'Run script : ' + os.path.realpath( __file__ ))
    print( '<================================|' )

    #-- Enter script path
    scriptPath = os.path.split( os.path.realpath( __file__ ) )[0]
    os.chdir( scriptPath )

    #-- Latest N days
    if ( args.d ) :
        dt = datetime.now()
        for d in range(0, args.d):
            t = dt + timedelta( days=-d)
            DoTask( datetime(t.year, t.month, t.day,  12), fileType, isSend )
            DoTask( datetime(t.year, t.month, t.day, 0), fileType, isSend )

        quit()

    #-- History run, from start date to end date
    if ( args.s and args.e ):
        if len(args.s)!=6 and len(args.e)!=6  :
            print("[Error] Argument is incorret")
            quit()

        dtEnd = datetime.strptime(args.e,'%y%m%d')
        dtStart = datetime.strptime(args.s,'%y%m%d')
        while dtEnd >= dtStart:
            DoTask( datetime(dtEnd.year, dtEnd.month, dtEnd.day, 12), fileType, isSend )
            DoTask( datetime(dtEnd.year, dtEnd.month, dtEnd.day, 0), fileType, isSend )
            dtEnd = dtEnd + timedelta( days = -1)

        quit()

    #-- Now, just has a timestring argument
    if len(args.timeString)==1 :
        args.timeString = args.timeString[0]

    #-- History run, for a single init date
    if ( len(args.timeString)==8 ):
        dt = datetime.strptime(args.timeString,'%y%m%d%H')
        DoTask( dt, fileType,isSend )
        quit()

    #-- Real time run, choose proper initdate
    if ( args.timeString=='00' ):
        dt = datetime.now()
        if dt.strftime('%H%M') < '1200':
            dt = datetime(dt.year,dt.month,dt.day,20) + timedelta( days=-1)
        else:
            dt = datetime(dt.year,dt.month,dt.day,8)
        DoTask( dt, fileType, isSend )
        quit()
		
  
#Show help
os.system('python ' + os.path.realpath(__file__) + ' -h')
quit()
