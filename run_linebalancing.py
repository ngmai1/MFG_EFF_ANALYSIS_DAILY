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

size_bundle_boy = ['2', '3', '4', 'XS', 'S', '3T', '4T', '5T', '2/3', 'MD', 'M', 'LG', 'L', 'XL']
size_range_boy = ['R0', 'R0', 'R0', 'R0', 'R0', 'R0', 'R0', 'R0', 'R0', 'R1', 'R1', 'R1', 'R1', 'R1']

pd_size_boy = pd.DataFrame()
pd_size_boy["SIZE"] = size_bundle_boy
pd_size_boy["RANGE"] = size_range_boy

size_bundle_men = ['S', 'M', 'L', 'XL', '2X', '2XL', '3X', '3XL', '4X', '4XL', 'SP', 'SS']
size_range_men = ['R2', 'R2', 'R2', 'R3', 'R3', 'R3', 'R4', 'R4', 'R4', 'R4', 'R4', 'R4']

pd_size_men = pd.DataFrame()
pd_size_men["SIZE"] = size_bundle_men
pd_size_men["RANGE"] = size_range_men

def get_max_range(R0, R1, R2, R3, R4):
    values = {"R0": R0, "R1": R1, "R2": R2, "R3": R3, "R4": R4}
    max_var = max(values, key=values.get)
    return max_var

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
    mydb = mysql.connector.connect(host='pbvweb01v', user='LaDat', passwd='l4d4t5', database="linebalancing")
    date_current = datetime.date.today()
    d = 2
    while d <= 200:

        date_cal = date_current - datetime.timedelta(days=d)
        date_query = date_cal.strftime('%Y-%m-%d')
        date_cal_str = date_cal.strftime('%Y%m%d')
        print(date_cal)
        delete_scanticket()
        insert_scanticket(date_cal)

        # work_hours

        sql_timesheet = ('select ID5,WORK_HRS from pr2k.employee_timesheet where DATE="' + date_cal.strftime(
            "%Y-%m-%d") + '";')
        print(sql_timesheet)
        timesheet = pd.read_sql(sql_timesheet, engine_hbi_lbl)

        # off standard

        sql_offstd = f"""
            SELECT right(ID,5) ID5,left(CODE,2) OFFCODE,if(SpanTime IS NULL, 0, SpanTime) AS sp 
            FROM linebalancing.operation_offstandard_tracking 
            WHERE  date(DateUpdate)="{date_query}" AND 
            left(CODE,2) IN ("02","04","06","07","09","10","11","13") 
            AND IEApprovedResult = 2
        """
        offstd = pd.read_sql(sql_offstd, engine_hbi_linebalancing)

        sql_data = """SELECT DATA1.*,aci.STYLE_DETAIL,TRIM(aci.BOY_MEN) BOY_MEN FROM 
                        (SELECT e.employee,e.operation,e.work_lot,TRIM(e.SIZE) SIZE,SUM(e.earned_hours) SAH_EARN,IF(a.SELLING_STYLE IS NULL,s.SellStyle,
                        a.SELLING_STYLE) SELL_STYLE FROM linebl_2024.employee_scanticket e 
                        LEFT JOIN pr2k.worklot_active a 
                        ON e.WORK_LOT=a.WORK_LOT 
                        LEFT JOIN (SELECT * FROM erpsystem.setup_plansewing GROUP BY LotAnet) s 
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
            op_range_boy = data_em.query('BOY_MEN=="BOY"').reset_index(drop=True)
            print('data boy')
            print(op_range_boy)
            op_range_men = data_em.query('BOY_MEN=="MEN"').reset_index(drop=True)
            print('data men')
            print(op_range_men)
            em_size_range_boy = pd.merge(op_range_boy, pd_size_boy, on=['SIZE'])
            em_size_range_men = pd.merge(op_range_men, pd_size_men, on=['SIZE'])
            em_size_range_combine = pd.concat([em_size_range_boy, em_size_range_men]).reset_index(drop=True)
            print('data combine')
            print(em_size_range_combine)

            if len(em_size_range_combine) == 0:
                i = i + 1
                continue

            # lay size lon nhat
            size_earn_select = em_size_range_combine.groupby(['SIZE']).agg(
                {'SAH_EARN': 'sum'}).reset_index().sort_values(by=['SAH_EARN'], ascending=False)
            size = size_earn_select['SIZE'].iloc[0]

            em_size_range_select = em_size_range_combine.groupby(['RANGE']).agg(
                {'SAH_EARN': 'sum'}).reset_index().sort_values(by=['SAH_EARN'], ascending=False)

            range_size = em_size_range_select['RANGE'].iloc[0]
            emp_size_select_sah = em_size_range_select['SAH_EARN'].iloc[0]
            r0 = em_size_range_combine.query('RANGE=="R0"').reset_index(drop=True)['SAH_EARN'].sum()
            r1 = em_size_range_combine.query('RANGE=="R1"').reset_index(drop=True)['SAH_EARN'].sum()
            r2 = em_size_range_combine.query('RANGE=="R2"').reset_index(drop=True)['SAH_EARN'].sum()
            r3 = em_size_range_combine.query('RANGE=="R3"').reset_index(drop=True)['SAH_EARN'].sum()
            r4 = em_size_range_combine.query('RANGE=="R4"').reset_index(drop=True)['SAH_EARN'].sum()
            # range_size = get_max_range(r0, r1, r2, r3, r4)
            sah_range = r0 + r1 + r2 + r3 + r4
            ratio_range = round(emp_size_select_sah * 100 / sah_range, 2)

            # tinh style
            style_detail_gr = em_size_range_combine.groupby(['STYLE_DETAIL']).agg(
                {'SAH_EARN': 'sum'}).reset_index().sort_values(by=['SAH_EARN'], ascending=False)
            if len(style_detail_gr) == 1:
                style1 = style_detail_gr.iloc[0, 0]
                sah_style1 = style_detail_gr.iloc[0, 1]
                style2 = ''
                sah_style2 = 0
                style3 = ''
                sah_style3 = 0
            elif len(style_detail_gr) == 2:
                style1 = style_detail_gr.iloc[0, 0]
                sah_style1 = style_detail_gr.iloc[0, 1]
                style2 = style_detail_gr.iloc[1, 0]
                sah_style2 = style_detail_gr.iloc[1, 1]
                style3 = ''
                sah_style3 = 0
            elif len(style_detail_gr) >= 3:
                style1 = style_detail_gr.iloc[0, 0]
                sah_style1 = style_detail_gr.iloc[0, 1]
                style2 = style_detail_gr.iloc[1, 0]
                sah_style2 = style_detail_gr.iloc[1, 1]
                style3 = style_detail_gr.iloc[2, 0]
                sah_style3 = style_detail_gr.iloc[2, 1]
            else:
                style1 = ''
                sah_style1 = 0
                style2 = ''
                sah_style2 = 0
                style3 = ''
                sah_style3 = 0
            ratio_style1 = round(sah_style1 * 100 / emp_sah, 2)

            # tinh workcenter
            sql_wc = ('select workcenter from aci_data where workcenter is not null and style_detail="' + style1 + '";')
            dt_wc = pd.read_sql(sql_wc, engine_hbi_linebalancing)
            wc = ''
            if len(dt_wc) > 0:
                wc = dt_wc.iloc[0][0]

            # tinh operation
            operation_gr = em_size_range_combine.groupby(['operation']).agg(
                {'SAH_EARN': 'sum'}).reset_index().sort_values(by=['SAH_EARN'], ascending=False)
            if len(operation_gr) == 1:
                op1 = operation_gr.iloc[0, 0]
                sah_op1 = operation_gr.iloc[0, 1]
                op2 = ''
                sah_op2 = 0
                op3 = ''
                sah_op3 = 0
            elif len(operation_gr) == 2:
                op1 = operation_gr.iloc[0, 0]
                sah_op1 = operation_gr.iloc[0, 1]
                op2 = operation_gr.iloc[1, 0]
                sah_op2 = operation_gr.iloc[1, 1]
                op3 = ''
                sah_op3 = 0
            elif len(operation_gr) >= 3:
                op1 = operation_gr.iloc[0, 0]
                sah_op1 = operation_gr.iloc[0, 1]
                op2 = operation_gr.iloc[1, 0]
                sah_op2 = operation_gr.iloc[1, 1]
                op3 = operation_gr.iloc[2, 0]
                sah_op3 = operation_gr.iloc[2, 1]
            else:
                op1 = ''
                sah_op1 = 0
                op2 = ''
                sah_op2 = 0
                op3 = ''
                sah_op3 = 0
            ratio_op1 = sah_op1 / emp_sah

            # tinh selling
            selling_group = em_size_range_combine.groupby(['SELL_STYLE']).agg(
                {'SAH_EARN': 'sum'}).reset_index().sort_values(by=['SAH_EARN'], ascending=False)
            selling = selling_group.iloc[0, 0]
            selling_sah = selling_group['SAH_EARN'].iloc[0]

            # tinh construction
            selling_group = em_size_range_combine.groupby(['BOY_MEN']).agg(
                {'SAH_EARN': 'sum'}).reset_index().sort_values(by=['SAH_EARN'], ascending=False)
            boy_men = selling_group.iloc[0, 0]

            wh = 7.5
            em_workhours = timesheet.query('ID5=="' + emp + '"').reset_index(drop=True)
            if len(em_workhours) > 0:
                wh = float(em_workhours.iloc[0, 1])
            # off = 0
            # offstd
            em_offstd = offstd.query('ID5=="' + emp + '"')
            # chuyển từ string sang float
            em_offstd['sp'] = pd.to_numeric(em_offstd['sp'], errors='coerce')
            sum_offstd = em_offstd['sp'].sum()

            if wh - sum_offstd == 0:
                i = i + 1
                continue

            # eff
            if wh != 0:
                eff = ((emp_sah / 60) / (wh - sum_offstd)) * 100
                eff = round(eff, 2)
            else:
                eff = 0

            sql_insert = (
                    'replace into bundle_group_by_employee_detail (ind,employee,date,workcenter,planteff,operation_name,'
                    'ratioop,size,ratiosize,style_detail,ratiostyle,garment,garment_sah,float_eff,sah_earned,offstd,work_hrs,'
                    'r0,r1,r2,r3,r4,range_size,ratio_range, construction,'
                    'style_detail1,style_detail2,style_detail3,sah_style1,sah_style2,sah_style3,operation1,'
                    'operation2,operation3,sah_op1,sah_op2,sah_op3) '
                    + 'values ("' + emp + date_cal_str + '","' + emp + '","' + date_cal_str + '","' + wc + '","'
                    + str(eff) + '","' + str(op1) + '","' + str(ratio_op1) + '","' + str(size) + '","' + str(
                ratio_range) + '","'
                    + str(style1) + '","' + str(ratio_style1) + '","' + str(selling) + '","' + str(
                round(selling_sah / 60, 2))
                    + '","' + str(eff) + '","' + str(round(emp_sah / 60, 2)) + '","' + str(sum_offstd) + '","' + str(wh)
                    + '","' + str(round(r0 / 60, 2)) + '","' + str(round(r1 / 60, 2)) + '","' + str(
                round(r2 / 60, 2)) + '","'
                    + str(round(r3 / 60, 2)) + '","' + str(round(r4 / 60, 2)) + '","' + range_size + '","' + str(
                ratio_range)
                    + '","' + boy_men + '","' + style1 + '","' + style2 + '","' + style3
                    + '","' + str(sah_style1 / 60) + '","' + str(sah_style2 / 60) + '","' + str(sah_style3 / 60) + '","'
                    + op1 + '","' + op2 + '","' + op3 + '","' + str(sah_op1 / 60)
                    + '","' + str(sah_op2 / 60) + '","' + str(sah_op3 / 60) + '");'
            )

            print(sql_insert)
            myCursor = mydb.cursor()
            myCursor.execute(sql_insert)
            mydb.commit()
            myCursor.close()
            i = i + 1
        d = d + 1


# cal_sewing_eff()
schedule.every().day.at("10:00").do(cal_sewing_eff)
print(datetime.datetime.now())
print('shedule start')

# run pending
while True:
    schedule.run_pending()
    time.sleep(1)
