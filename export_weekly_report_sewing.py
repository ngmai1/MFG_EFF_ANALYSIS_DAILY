import sys
import os
import mysql.connector
import string
import pandas as pd
from sqlalchemy import create_engine
from datetime import date, timedelta
#import random
import datetime
import numpy as np
import time
import schedule
import xlwings as xw
import smtplib,email,email.encoders,email.mime.text
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
mydb=mysql.connector.connect(
#    host='localhost',
    host='pbvweb01v',
    user='LaDat',
    passwd='l4d4t5',
    database="linebalancing"
)
cut_db=mysql.connector.connect(
#    host='localhost',
    host='pbvweb01v',
    user='LaDat',
    passwd='l4d4t5',
    database="cutting_system"
)

engine_hbi_linebalancing = create_engine('mysql+mysqlconnector://LaDat:l4d4t5@pbvweb01v:3306/linebalancing', echo=False)
engine_hbi_cutting = create_engine('mysql+mysqlconnector://LaDat:l4d4t5@pbvweb01v:3306/cutting_system', echo=False)
engine_hbi_pr2k = create_engine('mysql+mysqlconnector://LaDat:l4d4t5@pbvweb01v:3306/pr2k', echo=False)
#hostname='127.0.0.1'
engine_local = create_engine('mysql+mysqlconnector://LaDat:l4d4t5@localhost:3306/linebalancing', echo=False)

def sunday_of_calenderweek(year, week):
    first = datetime.date(2024, 1, 1)
    base = 0
    return first + timedelta(days=base - first.isocalendar()[2] + 7 * (week))

def export_weekly_report_sewing():
    testdate=datetime.datetime.today()
    # w=testdate.isocalendar()[1]-1
    w=42
    d=sunday_of_calenderweek(2024,w)
    # d=datetime.date(2020,4,15)
    d1=d+datetime.timedelta(days=1)
    d2=d+datetime.timedelta(days=2)
    d3=d+datetime.timedelta(days=3)
    d4=d+datetime.timedelta(days=4)
    d5=d+datetime.timedelta(days=5)
    d6=d+datetime.timedelta(days=6)
    d7=d+datetime.timedelta(days=7)
    print('begin day',d1.strftime('%Y%m%d'))
    print('end day',d7.strftime('%Y%m%d'))
    sql=('SELECT temp4.employee,temp4.name,temp4.shift,temp4.line,temp4.NameGroup,temp4.sah_earn,if(mv.sahmover IS NULL,0,sahmover) AS sahmover,temp4.work_hours,temp4.code09, ROUND(100*(temp4.sah_earn+if(mv.sahmover IS NULL,0,sahmover))/(temp4.work_hours-(temp4.code09 + if(off_std.sum_span_time IS NULL,0,off_std.sum_span_time))),2) AS Efficiency,(temp4.sah_earn+if(mv.sahmover IS NULL,0,sahmover))*7500 AS INCENTIVE,sel4.operation,sel4.operation_sah, ROUND(sel4.operation_sah/temp4.sah_earn,2) AS Rate_OP,sel4.slcd,"'+d1.strftime('%Y%m%d')+'" AS BGDAY,"'+d7.strftime('%Y%m%d')+'" AS FINISHDAY '
    +'FROM ( '
    +'SELECT temp3.employee,temp3.name,temp3.line,loc.NameGroup,temp3.shift,temp3.sah_earn,temp3.work_hours,temp3.code09, ROUND(100*temp3.sah_earn/(temp3.work_hours-temp3.code09),2) AS Efficiency,temp3.sah_earn*7500 AS INCENTIVE '
    +'FROM ( '
    +'SELECT temp2.employee,erp.Name,erp.Line,erp.Shift, SUM(temp2.sah) AS sah_earn, ROUND(SUM(temp2.work_hrs),3) AS work_hours, ROUND(SUM(temp2.CD09),2) AS CODE09 '
    +'FROM ( '
    +'SELECT ts.id2 AS ID,ts.id5 AS employee, IF(temp.sah IS NULL, 0, temp.sah) AS sah ,DATE_FORMAT(ts.DATE, "%Y%m%d") AS date,ts.work_hrs,ts.CD09 '
    +'FROM ( '
    +'SELECT CONCAT(employee, DATE) AS ID, employee, ROUND(SUM(earned_hours)/60,3) AS SAH, COUNT(ticket) AS Bundle, DATE '
    +'FROM employee_scanticket scan '
    +'WHERE DATE>="'+d1.strftime('%Y%m%d')+'" AND DATE<="'+d7.strftime('%Y%m%d')+'" '
    +'GROUP BY ID) AS temp '
    +'right JOIN ( '
    +'SELECT *, CONCAT(ID5, DATE_FORMAT(DATE, "%Y%m%d")) AS id2 '
    +'FROM employee_timesheet where DATE>="'+d1.strftime('%Y%m%d')+'" AND DATE<="'+d7.strftime('%Y%m%d')+'" AND EMP_TYPE = "DR" ) ts '
    +'on temp.id=ts.id2) AS temp2 '
    +'LEFT JOIN erpsystem.setup_emplist erp ON temp2.employee=RIGHT(erp.ID,5) '
    +'GROUP BY employee) AS temp3 '
    +'LEFT JOIN erpsystem.setup_location loc ON temp3.line=loc.Location '
    +'GROUP BY temp3.employee '
    +'ORDER BY temp3.shift) AS temp4 '
    +'LEFT JOIN ( '
    +'SELECT sel2.employee,sel2.operation_sah,sel3.operation,sel2.slcd '
    +'FROM ( '
    +'SELECT sel1.employee, MAX(sel1.sah_earn) AS operation_sah, COUNT(operation) AS slcd '
    +'FROM ( '
    +'SELECT employee,operation, ROUND(SUM(earned_hours)/60,3) AS sah_earn '
    +'FROM employee_scanticket '
    +'WHERE DATE>="'+d1.strftime('%Y%m%d')+'" AND DATE<="'+d7.strftime('%Y%m%d')+'" '
    +'GROUP BY employee,operation) AS sel1 '
    +'GROUP BY sel1.employee) AS sel2 '
    +'LEFT JOIN ( '
    +'SELECT employee,operation, ROUND(SUM(earned_hours)/60,3) AS sah_earn '
    +'FROM employee_scanticket '
    +'WHERE DATE>="'+d1.strftime('%Y%m%d')+'" AND DATE<="'+d7.strftime('%Y%m%d')+'" '
    +'GROUP BY employee,operation) AS sel3 ON sel2.employee=sel3.employee AND sel2.operation_sah=sel3.sah_earn '
    +'GROUP BY sel2.employee) AS sel4 ON temp4.employee=sel4.employee '
    +'LEFT JOIN ( '
    +'SELECT RIGHT(employee,5) AS id5, ROUND(SUM(sahbyzone),2) AS sahmover '
    +'FROM ( SELECT employee,zone,ttdz*SAH AS sahbyzone '
    +'FROM ( SELECT fn.IDEmployees AS employee,fn.ZoneMover AS zone, SUM(fn.DzCase) AS ttdz,SA.SAH '
    +'FROM erpsystem.data_finishedgoodssewing fn '
    +'LEFT JOIN erpsystem.setup_sahmover SA ON SA.Area=fn.ZoneMover '
    +'WHERE fn.DATE>="'+d1.strftime('%Y-%m-%d')+'" AND fn.DATE<="'+d7.strftime('%Y-%m-%d')+'" '
    +'GROUP BY employee,zone) AS m1 '
    +'GROUP BY employee,zone) AS m2 '
    +'GROUP BY id5) AS mv ON temp4.employee=mv.id5 '
    +'LEFT JOIN (SELECT ID, round(SUM(b1.span_time),2) AS sum_span_time '
    +'FROM (SELECT *, if(SpanTime IS NULL, 0, SpanTime) AS span_time '
    +'FROM linebalancing.operation_offstandard_tracking o '
    +'WHERE o.DateUpdate >="'+d1.strftime('%Y-%m-%d')+' 00:00:00" AND o.DateUpdate <= "'+d7.strftime('%Y-%m-%d')+' 00:00:00" '
    +'AND left(o.Code,2) IN ("02","04","06","07","10","11","12","13")) b1 GROUP by ID) AS off_std '
    +'on temp4.employee= right(off_std.id,5) WHERE Efficiency != "0" '
    +'GROUP BY temp4.employee;'
    )
    print(sql)
    weekly_eff=pd.read_sql(sql,engine_hbi_pr2k)
    print(len(weekly_eff))
    weekly_eff.to_excel('\\\\pbvfps1\\Bundle_Scan\\Report\\Weekly_report\\Weekly_efficiency_Report\\weekly_efficiency_incentive_CD09_OP7000_MOVER_report_2024_W43x.xlsx')
    print(datetime.datetime.now())
    print('finish export weekly eff Sewing including mover ',w)


export_weekly_report_sewing()


print(datetime.datetime.now())
print('shedule start')
