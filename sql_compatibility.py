
# -*- coding: utf-8 -*-

import xlrd,xlwt
from xlutils.copy import copy
import pymysql
import datetime

def getRefSQL(pSQL,pSheet):
    for i in range(1,pSheet.nrows):
        if '#'+pSheet.row_values(i,0)[0]==pSQL:
            return pSheet.row_values(i,1)[0]
    return ""

#执行SQL结果的比较
def compareSQL(filename):
    print('begin sql test === ')
    workbook= xlrd.open_workbook(filename)

    # init source db
    res_sheet=workbook.sheet_by_name('db_source')
    resDB_ip=res_sheet.row_values(3,1,2)[0]
    resDB_port=int(res_sheet.row_values(4,1,2)[0])
    resDB_username=res_sheet.row_values(5,1,2)[0]
    resDB_pwd=res_sheet.row_values(6,1,2)[0]
    resDB_db=res_sheet.row_values(7,1,2)[0]
    # init target db
    desDB_ip=res_sheet.row_values(3,2,3)[0]
    desDB_port=int(res_sheet.row_values(4,2,3)[0])
    desDB_username=res_sheet.row_values(5,2,3)[0]
    desDB_pwd=res_sheet.row_values(6,2,3)[0]
    desDB_db=res_sheet.row_values(7,2,3)[0]

    sql_sheet=workbook.sheet_by_name('sql_list')
    ref_sheet=workbook.sheet_by_name('ref_sql')

    wt_wb = copy(workbook)

    #需要执行的验证sql条数
    sql_num = sql_sheet.nrows

    for i in range(2,sql_num):
        if sql_sheet.row_values(i, 11, 12)[0]=='N':
            continue

        sql_type = sql_sheet.row_values(i,0,1)[0]
        sql_summary = sql_sheet.row_values(i,1,2)[0]

        # source sql
        s_pre_sql = sql_sheet.row_values(i,2,3)[0]
        if s_pre_sql.startswith("#"):
            s_pre_sql = getRefSQL(s_pre_sql,ref_sheet)
        s_test_sql = sql_sheet.row_values(i,3,4)[0]
        s_end_sql = sql_sheet.row_values(i,4,5)[0]
        if s_end_sql.startswith("#"):
            s_end_sql = getRefSQL(s_end_sql,ref_sheet)

        # target sql
        t_pre_sql = sql_sheet.row_values(i, 6, 7)[0]
        if t_pre_sql.startswith("#"):
            t_pre_sql = getRefSQL(t_pre_sql,ref_sheet)
        t_test_sql = sql_sheet.row_values(i, 7, 8)[0]
        t_end_sql = sql_sheet.row_values(i, 8, 9)[0]
        if t_end_sql.startswith("#"):
            t_end_sql = getRefSQL(t_end_sql,ref_sheet)

        res_result=execSQL(resDB_ip,resDB_port,resDB_username,resDB_pwd,resDB_db,s_pre_sql,s_test_sql,s_end_sql) #源数据库输出结果
        des_result=execSQL(desDB_ip,desDB_port,desDB_username,desDB_pwd,desDB_db,t_pre_sql,t_test_sql,t_end_sql) #目标库输出结果

        wt_sh1=wt_wb.get_sheet(1)
        wt_sh1.write(i, 5, str(res_result))
        wt_sh1.write(i, 9, str(des_result))
        res_stat = ('V','X')[res_result=='error']
        des_stat = ('V','X')[des_result=='error']
        cmp_stat = 'N'
        if res_stat=='V' and des_stat=='V' and res_result==des_result:
            cmp_stat='Y'
        wt_sh1.write(i, 10, ('N','Y')[res_result==des_result])
        print("...",i,'%-10s'%sql_type,'%-20s'%sql_summary,cmp_stat,'['+res_stat+']','['+des_stat+']',s_test_sql.replace('\n', ''))

    #保存文件
    outFile = 'sql_test_result_'+datetime.datetime.today().strftime("%Y%m%d%H%M%S")+'.xls'
    wt_wb.save(outFile)
    print("output file: "+outFile)
    print("end sql test ===")

#sql语句是否含有分号
def ifsemicolon(sql):
    if ";" in sql:
        return sql
    else:
        return sql+";"

#连接数据，并执行sql
def execSQL(ip,port,username,pwd,db,presql,sql,endsql):
    try:
        conn=pymysql.connect(host=ip,port=port,user=username,password=pwd,database=db)
        cursor=conn.cursor()
        # pre
        presql = ifsemicolon(presql).replace("\n","")

        test=presql.split(";")
        for l in range(len(test)-1):
            cursor.execute(test[l])

        # test
        sql = ifsemicolon(sql)
        test = sql.split(";")
        for l in range(len(test) - 1):
            cursor.execute(test[l])
        data1=cursor.fetchall()
        data=list(data1)

        # end
        endsql = ifsemicolon(endsql)

        test = endsql.split(";")
        for l in range(len(test) - 1):
            cursor.execute(test[l])
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as error:
        # print("Exception thrown: {0}".format(error))
        conn.rollback()
        endsql = ifsemicolon(endsql)
        test = endsql.split(";")
        for l in range(len(test) - 1):
            cursor.execute(test[l])
        conn.commit()
        # print("Rolled back")
        cursor.close()
        conn.close()
        data="error"
    return data

if __name__ == '__main__':
    sqlFile="sql_test.xlsx"
    compareSQL(sqlFile)
