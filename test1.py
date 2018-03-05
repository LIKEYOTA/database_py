# coding=utf-8

import time
import MySQLdb as mdb
import numpy as np
import sys
import os
import test6

# conn = mdb.connect(host='127.0.0.1', user='root', passwd='7ondr', db='testpy')


def storage_value(table_name):

    if 2 != len(sys.argv):
        exit(-1)

    dir_name = sys.argv[1]
    file_names = os.listdir(dir_name)
    for file in file_names:
        if file[-4:] == '.csv':
            s = file
            val1, val2, val3, val4, val5, val6, val7, val8 \
                = np.loadtxt("%s" % s, delimiter=',', usecols=(0, 1, 2, 3, 4, 5, 6, 7), unpack=True)
            # values = [val1, val2, val3, val4, val5, val6, val7, val8]
    for i in range(len(val1[:10])):
        values = [val1[i], val2[i], val3[i], val4[i], val5[i], val6[i], val7[i], val8[i]]
        insert_values(values, table_name)


def csv_to_database():
    '''
    if 2 != len(sys.argv):
        exit(-1)
    '''
    t = time.time()
    dir_name = 'H:\PYFILE\Tf\j9000'
    file_names = os.listdir(dir_name)
    t2 = 0
    for file in file_names:
        if file[-4:] == '.csv':
            s = dir_name + '\\' + file
            name = file[:-4]
            name1 = deal_name(name)
            print name1
            create_table(name1)
            storage(name1, s)
            storage_name(name1, 'names')
            t1 = time.time() - t
            print t2, t1
            t2 += 1
            # create_names_table()


def deal_name(name):
    le = len(name)
    file_name = name
    str1 = ''
    site = 0
    for i in range(le):
        if name[i] == '.':
            str1 = file_name[:i]
            site = i+1
            break

    # le1 = len(file_name)
    for i in range(site, le):
        if name[i] == "K":
            if name[i:i+4] == 'KBar':
                str1 += file_name[i:-4]
    return str1.replace('-', '_')


def storage_name(name, table_name):
    sql = "insert into %s(name) VALUES('%s') " % (table_name, name)
    print sql
    conn = mdb.connect(host='127.0.0.1', user='root', passwd='7ondr', db='test')
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    conn.close()


def get_name(table):
    sql = "select name from %s" % table
    conn = mdb.connect(host='127.0.0.1', user='root', passwd='7ondr', db='test')
    cursor = conn.cursor()
    cursor.execute(sql)
    val = []
    values = cursor.fetchall()
    # print values
    for i in range(len(values)):
        val.append(values[i][0])
    # val1 = list(values)
    # print val
    conn.close()
    return val


def storage(table_name, file_name):
    val1, val2, val3, val4, val5, val6, val7, val8 \
        = np.loadtxt(file_name, delimiter=',', usecols=(0, 1, 2, 3, 4, 5, 6, 7), unpack=True)
    # 2
    # conn = mdb.connect(host='127.0.0.1', user='root', passwd='7ondr', db='testpy')
    t1 = "insert into %s (a, b, c1, d, e, f, g, h)" % table_name
    print t1
    length = len(val1)
    print length
    number = 45000
    n = length/number
    print n
    # print val7[146177]
    '''
    if n > 0:
        return
    '''
    if n > 0:
        num = 0
        for j in range(n):
            params = []
            num += number
            conn = mdb.connect(host='127.0.0.1', user='root', passwd='7ondr', db='test')
            # print j
            for i in range(j * number, (j+1)*number):
                # values = [str(val1[i]), str(val2[i]), str(val3[i]), str(val4[i]), str(val5[i]), str(val6[i]), str(val7[i]), str(val8[i])]
                values = [val1[i], val2[i], val3[i], val4[i], val5[i], val6[i], val7[i], val8[i]]
                params.append(values)
            # sql = "insert into %s (a, b, c1, d, e, f, g, h) values(%s, %s, %s, %s, %s, %s, %s, %s)"  # ('you', 'ok', 'ok', 'ok', 'ok', 'ok', 'ok', 'ok', 'ok')
            # t1 = "insert into %s (a, b, c1, d, e, f, g, h)" % table_name
            sql = t1+" values(%s, %s, %s, %s, %s, %s, %s, %s)"
            # print params[0][7]
            # insert_values(values, table_name)
            cursor = conn.cursor()
            cursor.executemany(sql, params)
            conn.commit()
            cursor.close()
            conn.close()
            # time.sleep(0.5)
            # print 0.5

        l2 = len(val1[n*number:])
        # print l2
        # print num
        # print "over 1!"
        params = []
        conn = mdb.connect(host='127.0.0.1', user='root', passwd='7ondr', db='test')
        for i in range(n*number, len(val1)):
            # values = [table_name, str(val1[i]), str(val2[i]), str(val3[i]), str(val4[i]), str(val5[i]), str(val6[i]), str(val7[i]), str(val8[i])]
            values = [val1[i], val2[i], val3[i], val4[i], val5[i], val6[i], val7[i], val8[i]]
            params.append(values)
        # t1 = "insert into %s (a, b, c1, d, e, f, g, h)" % table_name
        sql = t1 + " values(%s, %s, %s, %s, %s, %s, %s, %s)"
        # sql = "insert into %s (a, b, c1, d, e, f, g, h) values(%s, %s, %s, %s, %s, %s, %s, %s)"  # ('you', 'ok', 'ok', 'ok', 'ok', 'ok', 'ok', 'ok', 'ok')
        # sql = "insert into csv_table(a, b, c1, d, e, f, g, h) values(%s, %s, %s, %s, %s, %s, %s, %s)"  # ('you', 'ok', 'ok', 'ok', 'ok', 'ok', 'ok', 'ok', 'ok')
        # print sql
        # insert_values(values, table_name)
        cursor = conn.cursor()
        cursor.executemany(sql, params)
        conn.commit()
        cursor.close()
        conn.close()
        # print "over 2!"
    else:
        params = []
        conn = mdb.connect(host='127.0.0.1', user='root', passwd='7ondr', db='test')
        for i in range(len(val1)):
            # values = [table_name, str(val1[i]), str(val2[i]), str(val3[i]), str(val4[i]), str(val5[i]), str(val6[i]), str(val7[i]), str(val8[i])]
            values = [val1[i], val2[i], val3[i], val4[i], val5[i], val6[i], val7[i], val8[i]]
            params.append(values)
        # t1 = "insert into %s (a, b, c1, d, e, f, g, h)" % table_name
        sql = t1 + " values(%s, %s, %s, %s, %s, %s, %s, %s)"
        # sql = "insert into %s(a, b, c1, d, e, f, g, h) values(%s, %s, %s, %s, %s, %s, %s, %s)"  # ('you', 'ok', 'ok', 'ok', 'ok', 'ok', 'ok', 'ok', 'ok')
        # sql = "insert into csv_table(a, b, c1, d, e, f, g, h) values(%s, %s, %s, %s, %s, %s, %s, %s)"  # ('you', 'ok', 'ok', 'ok', 'ok', 'ok', 'ok', 'ok', 'ok')
        # print sql
        # insert_values(values, table_name)
        cursor = conn.cursor()
        cursor.executemany(sql, params)
        conn.commit()
        cursor.close()
        conn.close()
    # conn.commit()
    # conn.close()
    # 1
    '''
    params = []
    for i in range(len(val1[20000:])):
        # values = [table_name, str(val1[i]), str(val2[i]), str(val3[i]), str(val4[i]), str(val5[i]), str(val6[i]), str(val7[i]), str(val8[i])]
        values = [val1[i], val2[i], val3[i], val4[i], val5[i], val6[i], val7[i], val8[i]]
        params.append(values)
    sql = "insert into csv_table(a, b, c1, d, e, f, g, h) values(%s, %s, %s, %s, %s, %s, %s, %s)"  # ('you', 'ok', 'ok', 'ok', 'ok', 'ok', 'ok', 'ok', 'ok')
    # print sql
    # insert_values(values, table_name)
    cursor = conn.cursor()
    cursor.executemany(sql, params)
    conn.commit()
    # print val1[:10]
    '''


def read_buck(params, table):
    try:
        print len(params)
        t1 = "insert into %s (a, b, c1, d, e, f, g, h)" % table
        sql = t1 + " values(%s, %s, %s, %s, %s, %s, %s, %s)"
        print sql
        conn = mdb.connect(host='127.0.0.1', user='root', passwd='7ondr', db='test')
        conn.cursor().executemany(sql, params)
        conn.cursor().close()
        conn.close()
    except Exception as e:
        print e


def test_read(table_name, file_name):
    val1, val2, val3, val4, val5, val6, val7, val8 \
        = np.loadtxt(file_name, delimiter=',', usecols=(0, 1, 2, 3, 4, 5, 6, 7), unpack=True)
    length = len(val1)
    print length
    number = 50000
    n = length / number
    print n
    if n > 0:
        num = 0
        for j in range(n):
            params = []
            num += number
            for i in range(len(val1[j * number:(j+1)*number])):
                values = [str(val1[i]), str(val2[i]), str(val3[i]), str(val4[i]), str(val5[i]), str(val6[i]), str(val7[i]), str(val8[i])]
                params.append(values)
            read_buck(params, table_name)

        l2 = len(val1[n * number:])
        # print l2
        # print num
        params = []
        for i in range(len(val1[n * number:])):
            values = [val1[i], val2[i], val3[i], val4[i], val5[i], val6[i], val7[i], val8[i]]
            params.append(values)
        read_buck(params, table_name)

    else:
        params = []
        for i in range(len(val1)):
            values = [val1[i], val2[i], val3[i], val4[i], val5[i], val6[i], val7[i], val8[i]]
            params.append(values)
        read_buck(params, table_name)


# 创建基础筛选csv的指标
def create_names_table(name):
    conn = mdb.connect(host='127.0.0.1', user='root', passwd='7ondr', db='test')
    sql = "create table %s(name_id int not null AUTO_INCREMENT," \
          "name varchar(200) not null," \
          "NP FLOAT DEFAULT 0.0," \
          "MDD  FLOAT DEFAULT 0.0," \
          "mDD_mC FLOAT DEFAULT 0.0," \
          "mDD_iC FLOAT DEFAULT 0.0," \
          "LVRG FLOAT DEFAULT 0.0," \
          "LVRG_Mon_NP FLOAT DEFAULT 0.0," \
          "PpT  FLOAT DEFAULT 0.0," \
          "TT FLOAT DEFAULT 0.0," \
          "ShR  FLOAT DEFAULT 0.0," \
          "Days FLOAT DEFAULT 0.0," \
          "D_WR FLOAT DEFAULT 0.0," \
          "D_STD FLOAT DEFAULT 0.0," \
          "D_MaxNP FLOAT DEFAULT 0.0," \
          "D_MinNP FLOAT DEFAULT 0.0," \
          "maxW_Days FLOAT DEFAULT 0.0," \
          "maxL_Days FLOAT DEFAULT 0.0," \
          "PRIMARY KEY (name_id))" % name

    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    conn.close()


def create_table(name):
    conn = mdb.connect(host='127.0.0.1', user='root', passwd='7ondr', db='test')
    sql = "CREATE TABLE "+name+"(csv_id int NOT NULL AUTO_INCREMENT,A  int NOT NULL," \
          "B  int NOT NULL," \
          "C1  int NOT NULL," \
          "D  int NOT NULL," \
          "E  int NOT NULL," \
          "F  int NOT NULL," \
          "G  int NOT NULL," \
          "H  int NOT NULL," \
          "PRIMARY KEY ( csv_id )" \
          ")"
    sql2 = "CREATE TABLE "+ name + \
           "(csv_id int NOT NULL AUTO_INCREMENT," \
           "A  int NOT NULL," \
           "B  int NOT NULL," \
           "C1  float NOT NULL," \
           "D  int NOT NULL," \
           "E  int NOT NULL," \
           "F  float NOT NULL," \
           "G  int NOT NULL," \
           "H  int NOT NULL," \
           "PRIMARY KEY ( csv_id ))"

    sql3 = "CREATE TABLE " + "test-112" + \
           "(csv_id int NOT NULL AUTO_INCREMENT," \
           "A  int NOT NULL," \
           "B  int NOT NULL," \
           "C1  float NOT NULL," \
           "D  int NOT NULL," \
           "E  int NOT NULL," \
           "F  float NOT NULL," \
           "G  int NOT NULL," \
           "H  int NOT NULL," \
           "PRIMARY KEY ( csv_id ))"

    # print sql
    cursor = conn.cursor()
    cursor.execute(sql2)
    conn.close()
    # cursor.commit()


def create_index_table():
    pass


def insert_index_names(table):
    names = get_name(table)
    '''
    sql = "insert into %s(NP, MDD, PpT, TT, ShR, Days, " \
          "D_WR, D_STD, D_MaxNP, D_MinNP, maxW_Days, maxL_Days, mDD_mC, mDD_iC)" \
          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    '''
    print names
    for name in names:
        val = test6.plot_result1(name)  # get the index of table name
        val = []
        sql = "insert into %s(NP, MDD, PpT, TT, ShR, Days, " \
              "D_WR, D_STD, D_MaxNP, D_MinNP, maxW_Days, maxL_Days, mDD_mC, mDD_iC)" \
              "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" % (name, val[0], val[0], val[0], val[0], val[0], \
                                                                            val[0], val[0], val[0], val[0], val[0], \
                                                                           val[0], val[0], val[0], val[0])
        print sql


def insert_values(values, table_name):
    conn = mdb.connect(host='127.0.0.1', user='root', passwd='7ondr', db='test')
    sql1="insert into "+table_name+"(a, b, c1, d, e, f, g, h) values("+str(values[0])+","+str(values[1])+","+str(values[2])+","+str(values[3])+","+str(values[4])+","\
    +str(values[5]) + ","+str(values[6])+","+str(values[7])+")"
    cursor = conn.cursor()
    cursor.execute(sql1)
    conn.commit()
    conn.close()


def get_values(table_name):
    conn = mdb.connect(host='127.0.0.1', user='root', passwd='7ondr', db='test')
    sql2 = 'select a, b, c1, d, e, f, g, h from '+table_name
    cursor = conn.cursor()
    cursor.execute(sql2)
    values = cursor.fetchall()
    val1 = []
    val2 = []
    val3 = []
    val4 = []
    val5 = []
    val6 = []
    val7 = []
    val8 = []

    # print values
    for row in values:
        val1.append(row[0])
        val2.append(row[1])
        val3.append(row[2])
        val4.append(row[3])
        val5.append(row[4])
        val6.append(row[5])
        val7.append(row[6])
        val8.append(row[7])
    conn.close()
    # print val1


def delete_csv_table(table):
    names = get_name(table)
    le = len(names)
    print le
    if le > 0:
        print names
        for i in range(le):
            delete_table(names[i])
        delete_value('names')
    else:
        print "Empty!"


def delete_value(table):
    conn = mdb.connect(host='127.0.0.1', user='root', passwd='7ondr', db='test')
    sql = 'TRUNCATE TABLE ' + table
    print sql
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    conn.close()


def delete_table(table_name):
    conn = mdb.connect(host='127.0.0.1', user='root', passwd='7ondr', db='test')
    sql = 'drop table '+table_name
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    conn.close()

'''
t1 = time.time()

filename = "test.csv"
s = "%s" % filename
val1, val2, val3, val4, val5, val6, val7, val8\
    = np.loadtxt("%s"% s, delimiter=',', usecols=(0, 1, 2, 3, 4, 5, 6, 7), unpack=True)


print val7[1:10]
print val8[:9]
t2 = time.time()

print(t2 - t1)
'''

'''
dir_name = 'H:\PYFILE\Tf'
file_names = os.listdir(dir_name)
for i in file_names:
    if i[-4:] == '.csv':
        print i
'''
# create_table('csv_table2')

# value_1 = [1, 2, 3, 4, 5, 6, 7, 8]
# table_name = 'csv_table'

# create_names_table('names')
# create_table(table_name)
# insert_values(value_1, table_name)
# get_values(table_name)
# delete_table('names')
# delete_table('test')

'''
delete_table('sj7znekbar_27m__20150105_20180102_22th')
delete_table('sj7znekbar_22m__20150105_20180102_12th')
delete_table('sj7znekbar_20m__20150105_20180102_9th')
delete_table('sj7znekbar_22m__20150105_20180102_11th')
delete_table('sj7znekbar_24m__20150105_20180102_17th')

delete_table('yfqmyskbar_21m__20150105_20180102_12th')
delete_table('yfqmyskbar_23m__20150105_20180102_15th')
delete_table('yfqmyskbar_26m__20150105_20180102_16th')
delete_table('yfqmyskbar_26m__20150105_20180102_18th')
delete_table('yfqmyskbar_27m__20150105_20180102_7th')
'''
# t = time.time()

# storage('csv_table', 'j9000\\YFQMYS.WOBV_DX_QG_GZ_ADC_DK_KBar_21m-_20150105-20180102_12th_bkt.csv')
# get_name('names')
# t2 = time.time() - t
# print t2
# storage('csv_table', 'test.csv')
# delete_csv_table('names')
# test_read(table_name, 'test.csv')
# csv_to_database()
# delete_value('names')
# insert_index_names('names')
# delete_value('csv_table')
# t1  ="insert into %s(a, b, c1, d, e, f, g, h)"  % ('show you',)
# t2 = t1+" values(%s, %s, %s, %s, %s, %s, %s, %s)"
# 'ok', 'ok', 'ok', 'ok', 'ok', 'ok', 'ok', 'ok')

# print t2
# deal_name('SJ7ZNE.WOBV_DX_QG_GZ_ADC_DK_KBar_20m-_20150105-20180102_9th_bkt.csv')
