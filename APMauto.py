# -*- coding: utf-8 -*-
"""
Created on Wed Jul 20 11:08:05 2022

@author: Engineering
"""

import pandas as pd 
import numpy as np
import pyodbc
from datetime import datetime
import glob
import os
import shutil
import subprocess
import csv
import pyodbc


conn36=pyodbc.connect('Driver={SQL Server};'
                          'Server=localhost;'
                          'Database=FuelMasterDB;'
                          'Trusted_Connection=yes;'
                          )


b738path=r'C:/Users/Engineering/TURISTIK HAVA TASIMACILIK A.S/Gökmen Düzgören - FOE_2019/phyton/db_python/APM/APM Module/B738/'
b738SFPpath=r'C:/Users/Engineering/TURISTIK HAVA TASIMACILIK A.S/Gökmen Düzgören - FOE_2019/phyton/db_python/APM/APM Module/B738SFP/'
MAXpath=r'C:/Users/Engineering/TURISTIK HAVA TASIMACILIK A.S/Gökmen Düzgören - FOE_2019/phyton/db_python/APM/APM Module/MAX/'


B738AC=['9H-TJA','9H-TJC','9H-TJD','9H-TJE','9H-TJF','TC-TJI','TC-TJJ','TC-TJO','TC-TJP','TC-TJS','TC-TJU','TC-TJV','TC-TJY']
B738SFPAC=['9H-CXA','9H-CXB','9H-CXC','9H-CXD','9H-CXE','9H-CXF','9H-CXG','9H-CXH','9H-TJB','9H-TJH','TC-COE','TC-COH','TC-CON','TC-COR','TC-TJR','TC-TJT']
MAXAC=['TC-MKS','TC-MKB','TC-MKC','TC-MKD','TC-MKE','TC-MKF']



def APM(pathtomodule, AClist ):
    
    files=glob.glob('\\\\WIN-K3MC58CJDD4/Aerobytes/APM Exports/*.APM')
    files.sort(key=os.path.getmtime, reverse=True)    
    
    for cntdays in range(7):
        
        try:
            shutil.copy(files[cntdays], pathtomodule+"APM_B737.APM")
            
            
            #737 için
            p=subprocess.Popen(os.path.join(pathtomodule, "APM.exe"), cwd=pathtomodule)
            p.wait()
            DF738=pd.read_csv(os.path.join(pathtomodule, "SPREAD.csv"),  skiprows=[0], header=None)
            DF738dum=DF738[DF738.iloc[:,0].str.rstrip().isin(AClist)]
            
            airportdate=DF738dum.iloc[:,11].str.split(pat="         ", expand=True)
            FltNum=airportdate.iloc[:,1].str[:9]
            Airports=airportdate.iloc[:,1].str[9:17]
            DT=airportdate.iloc[:,1].str[17:]
            DT=pd.to_datetime(DT, format="%d%m%y%H%M%S")
            date=DT.dt.strftime('%Y-%m-%d')
            time=DT.dt.strftime('%H:%M:%S')
            
            Columns=["FltNumber", "Aircraft", "Date", "Time", "Airport", "Mach", "CAS", "TAT(C)", "Altitude", "GrossWeight", "FF1", "FF2", "N1-1", "N1-2", "QualityFactor", "GroundSpeed",
                     "LateralPos","LongitudePos", "MagHead", "TotalFuelQTY", "N2-1", "N2-2", "EGT1", "EGT2", "%ThrustDev", "N1Dev", "%FF Dev Eng1"   ,"%FF Dev Eng2"   ,
                     "%FF Dev Tot"    ,"FF Dev DueToN1 ","% FM Dev"]
            
            DF738dum[22]=(DF738dum[23]/100).astype(int).astype(str)
            DF738dum[15]=DF738dum[0].str.rstrip()
            DF738dum[25]=DF738dum[26]/1000
            tomerge1=DF738dum[[14,17,20,22,25, 29,30,33,34,37,38,55, 58, 61, 77, 122, 123, 130, 131, 2, 3,4, 5,8 ,10, 9]]
            
            MergedDF=pd.concat([pd.concat([pd.concat([pd.concat([pd.concat([FltNum, DF738dum[15]], axis=1), date], axis=1), time],  axis=1), Airports], axis=1), tomerge1], axis=1)
            MergedDF.columns=Columns
            MergedDF=MergedDF.reset_index()
            MergedDF["TotalFuelQTY"]=MergedDF["TotalFuelQTY"].astype(str)
            
            cur=conn36.cursor()
        
            for cntSQL in range(len(MergedDF)):
                SQLlist=MergedDF.iloc[cntSQL, :].tolist()
                for cntlist in range(4, len(SQLlist)):
                    if isinstance(SQLlist[cntlist], np.int64):
                        SQLlist[cntlist]=SQLlist[cntlist].astype(str)
                
                cur.execute("SELECT * from dbo.APM WHERE [Flt Num]=? ", int(SQLlist[1]))
                
                ftcur=cur.fetchone()
                if not ftcur:
                    try:
                        
                        
                        cur.execute("""INSERT INTO dbo.APM VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                                   SQLlist[1:])
                        conn36.commit()
                    except Exception as E:
                         print(E)
                 
                         pass  
        except:
            pass
  
APM(b738path, B738AC)
APM(b738SFPpath, B738SFPAC)
APM(MAXpath, MAXAC)

    