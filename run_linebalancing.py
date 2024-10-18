import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
import datetime
import schedule
import time

engine_hbi_linebalancing = create_engine('mysql+mysqlconnector://LaDat:l4d4t5@pbvweb01v:3306/linebalancing', echo=False)
engine_hbi_cutting = create_engine('mysql+mysqlconnector://LaDat:l4d4t5@pbvweb01v:3306/cutting_system', echo=False)
engine_hbi_pr2k = create_engine('mysql+mysqlconnector://LaDat:l4d4t5@pbvweb01v:3306/pr2k', echo=False)
engine_local = create_engine('mysql+mysqlconnector://LaDat:l4d4t5@localhost:3306/linebalancing', echo=False)
engine_hbi_lbl = create_engine('mysql+mysqlconnector://IE_lbl_2024:12345678@pbvpweb01:3306/linebl_2024', echo=False)
engine_hbi_pr2k2 = create_engine('mysql+mysqlconnector://LaDat:l4d4t5@pbvweb01v:3306/pr2k', echo=False)
engine_pbvpweb01_lbl = create_engine('mysql+mysqlconnector://LaDat:l4d4t5@pbvpweb01:3306/linebl_2024', echo=False)


size_bundle_boy=['3','4','XS','S','3T','4T','2/3','MD','M','LG','L','XL']
size_range_boy=['R0','R0','R0','R0','R0','R0','R0','R1','R1','R1','R1','R1']

pd_size_boy=pd.DataFrame()
pd_size_boy["SIZE"]=size_bundle_boy
pd_size_boy["RANGE"]=size_range_boy

size_bundle_men=['S','M','L','XL','2X','2XL','3X','3XL','4X','4XL','SP','SS']
size_range_men=['R2','R2','R2','R3','R3','R3','R4','R4','R4','R4','R4','R4']

pd_size_men=pd.DataFrame()
pd_size_men["SIZE"]=size_bundle_men
pd_size_men["RANGE"]=size_range_men


def delete_scanticket():
    mydb = mysql.connector.connect(host='pbvpweb01', user='IE_lbl_2024', passwd='12345678', database="linebl_2024")
    myCursor = mydb.cursor()
    # get_date = date.strftime('%Y%m%d')
    query = ('delete from linebl_2024.employee_scanticket')
    myCursor.execute(query)
    mydb.commit()
    mydb.close()


def insert_scanticket(date):
    mydb = mysql.connector.connect(host='pbvpweb01', user='IE_lbl_2024', passwd='12345678', database="linebl_2024")
    mycursor = mydb.cursor()
    get_date = date.strftime('%Y%m%d')

    sql_insert = f"""
    insert into lineBl_2024.employee_scanticket
    select * from pr2k.employee_scanticket where date='{get_date}'
    """
    mycursor.execute(sql_insert)
    mydb.commit()

    return


def cal_sewing_eff():
    mydb = mysql.connector.connect(host='pbvpweb01', user='IE_lbl_2024', passwd='12345678', database="linebl_2024")
    date_current = datetime.date.today()
    d = 2
    while d <= 200:

        date_cal = date_current - datetime.timedelta(days=d)
        date_query = date_cal.strftime('%Y-%m-%d')
        print(date_cal)
        # delete_scanticket()
        # insert_scanticket(date_cal)


        # work_hours

        sql_timesheet = ('select ID5,WORK_HRS from pr2k.employee_timesheet where DATE="' + date_cal.strftime(
            "%Y-%m-%d") + '";')
        print(sql_timesheet)
        timesheet = pd.read_sql(sql_timesheet, engine_hbi_lbl)

        # off standard

        sql_offstd = f"""
            SELECT right(ID,5) ID5,left(CODE,2) OFFCODE,if(SpanTime IS NULL, 0, SpanTime) AS sp 
            FROM linebl_2024.operation_offstandard_tracking 
            WHERE  date(DateUpdate)="{date_query}" AND 
            left(CODE,2) IN ("02","04","06","07","09","10","11","13") 
            AND IEApprovedResult = 2
        """
        offstd = pd.read_sql(sql_offstd, engine_hbi_lbl)

        sql_data = """SELECT DATA1.*,aci.STYLE_DETAIL,TRIM(aci.BOY_MEN) BOY_MEN FROM 
                        (SELECT e.employee,e.operation,e.work_lot,TRIM(e.SIZE) SIZE,SUM(e.earned_hours) SAH_EARN,IF(a.SELLING_STYLE IS NULL,s.SellStyle,
                        a.SELLING_STYLE) SELL_STYLE FROM linebl_2024.employee_scanticket e 
                        LEFT JOIN pr2k.worklot_active a 
                        ON e.WORK_LOT=a.WORK_LOT 
                        LEFT JOIN erpsystem.setup_plansewing s 
                        ON e.work_lot=s.LotAnet 
                        WHERE e.OPERATION IS NOT NULL 
                        GROUP BY employee,operation,work_lot,SIZE ORDER BY employee) DATA1 
                        LEFT JOIN linebl_2024.aci_data aci 
                        ON DATA1.SELL_STYLE=aci.KEY_ACI
                    """
        print(sql_data)
        list_data = pd.read_sql(sql_data, engine_pbvpweb01_lbl)
        list_data = list_data.fillna('')

        list_data_employee = list_data.groupby(['employee'], as_index=False).agg({'SAH_EARN': 'sum'}).reset_index(
            drop=True)

        i = 0
        while i < len(list_data_employee):
            emp = str(list_data_employee.iloc[i][0])
            emp_sah = list_data_employee.iloc[i, 1]

            data_em = list_data.query('employee=="' + emp + '"').sort_values(by=['SAH_EARN']).reset_index(drop=True)
            print('data employee')
            print(data_em)
            # operation
            op_range_boy=data_em.query('BOY_MEN=="BOY"').reset_index(drop=True)
            print('data boy')
            print(op_range_boy)
            op_range_men=data_em.query('BOY_MEN=="MEN"').reset_index(drop=True)
            print('data men')
            print(op_range_men)
            em_size_range_boy=pd.merge(op_range_boy,pd_size_boy,on=['SIZE'])
            em_size_range_men=pd.merge(op_range_men,pd_size_men,on=['SIZE'])
            em_size_range_combine=pd.concat([em_size_range_boy,em_size_range_men]).reset_index(drop=True)
            print('data combine')
            print(em_size_range_combine)

            x=input()
            i=i+1
            continue


            em_operation = data_em.groupby(['operation'], as_index=False).agg({'SAH_EARN': 'sum'}).sort_values(
                by=['SAH_EARN'], ascending=[False]).reset_index(drop=True)
            print(em_operation)
            em_op = em_operation.iloc[0, 0]
            op_sah = em_operation.iloc[0, 1]
            em_op2=""
            op_sah2=0
            em_op3=""
            op_sah3=0
            rate_op = op_sah / emp_sah
            if len(em_operation)>1:
                em_op2 = em_operation.iloc[1, 0]
                op_sah2 = em_operation.iloc[1, 1]
            if len(em_operation)>2:
                em_op3 = em_operation.iloc[2, 0]
                op_sah3 = em_operation.iloc[2, 1]

            
            # xác định range size
            # op_size = data_em.query('operation=="'+em_op+'"').reset_index(drop=True)
            # op_range=op_size.groupby(['BOY_MEN'],as_index=False).agg({'SAH_EARN': 'sum'}).sort_values(by=['SAH_EARN'],ascending=[False]).reset_index(drop=True)




            boy_men=op_range.iloc[0,0]

            # size
            if boy_men=='BOY':
                em_size_range=pd.merge(op_size,pd_size_boy,on=['SIZE'],how='left')
                em_size_range['RANGE'].fillna('ZZZ',inplace=True)
                print(em_size_range)
                em_range_group=em_size_range.groupby(['RANGE'],as_index=False).agg({'SAH_EARN': 'sum'}).sort_values(by=['SAH_EARN'],ascending=[False]).reset_index(drop=True)
                print(em_range_group)
                x=input()
                range_size_0=em_range_group['RANGE'].tolist()[0]
                range_sah_0=em_range_group['SAH_EARN'].tolist()[0]
                R2=0
                R3=0
                R4=0
            else:
                em_size_range=pd.merge(op_size,pd_size_men,on=['SIZE'],how='left')
                em_size_range['RANGE'].fillna('ZZZ',inplace=True)
                print(em_size_range)
                em_range_group=em_size_range.groupby(['RANGE'],as_index=False).agg({'SAH_EARN': 'sum'}).sort_values(by=['SAH_EARN'],ascending=[False]).reset_index(drop=True)
                print(em_range_group)
                x=input()
                range_size_0=em_range_group['RANGE'].tolist()[0]
                range_sah_0=em_range_group['SAH_EARN'].tolist()[0]

            print('done size check')
            i=i+1
            continue
            x=input()
            # print(em_range_group)

            # em_sz = em_size.iloc[0, 0]
            # sz_sah = em_size.iloc[0, 1]
            # rate_sz = sz_sah / emp_sah

            # STYLE_DETAIL
            em_style_detail = data_em.groupby(['STYLE_DETAIL'], as_index=False).agg({'SAH_EARN': 'sum'}).sort_values(
                by=['SAH_EARN'], ascending=[False]).reset_index(drop=True)
            print(em_style_detail)
            em_style = em_style_detail.iloc[0, 0]
            style_sah = em_style_detail.iloc[0, 1]
            rate_style = style_sah / emp_sah
            wh = 7.5
            em_workhours = timesheet.query('ID5=="' + emp + '"').reset_index(drop=True)
            if len(em_workhours) > 0:
                wh = float(em_workhours.iloc[0, 1])
            off = 0
            # offstd
            em_offstd = offstd.query('ID5=="' + emp + '"')
            # chuyển từ string sang float
            em_offstd['sp'] = pd.to_numeric(em_offstd['sp'], errors='coerce')
            sum_offstd = em_offstd['sp'].sum()

            # eff
            eff = ((emp_sah / 60) / (wh - sum_offstd)) * 100

            print(emp, emp_sah, em_op, rate_op, em_sz, rate_sz, em_style, rate_style)


            try:
                sql_insert=('replace into linebl_2024.bundle_group_by_employee_detail (ind,employee,date,workcenter,ratiowc,operation,operation_name,ratioop,mnf,ratiomnf,size,ratiosize,style,ratiostyle,style_detail,garment,float_eff) '
                            +'values ("'+emp+date_cal.strftime("%Y%m%d")+'","'+emp+'","'+date_cal.strftime("%Y%m%d")+'",null,null,null,"'+str(em_op)+'","'+str(rate_op)+'",null,null,"'+str(em_sz)+'","'+str(rate_sz)+'","'+str(em_style)+'","'+str(rate_style)+'","'+str(em_style)+'",null,"'+str(round(eff, 3))+'");')

                print(sql_insert)
                myCursor=mydb.cursor()
                myCursor.execute(sql_insert)
                mydb.commit()
                myCursor.close()
            except:
                print('insert error')
            i = i + 1

        d = d + 1


cal_sewing_eff()
