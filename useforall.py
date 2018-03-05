import time
import MySQLdb as mdb
import numpy as np

a = "show me code (%s, %s)" % (1.2, 12)
print a


def read_data(file_name):
    val1, val2, val3, val4, val5, val6, val7, val8 \
        = np.loadtxt(file_name, delimiter=',', usecols=(0, 1, 2, 3, 4, 5, 6, 7), unpack=True)
    conn = mdb.connect(host='127.0.0.1', user='root', passwd='7ondr', db='testpy')
    length = len(val1)
    print length
    number = 20000
    n = length / number
    num = 0
    print n
    # print val7[146177]
    sql = "insert into csv_table(a, b, c1, d, e, f, g, h) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

    '''
    for j in range(n):
        params = []
        # print val7[146177]
        # print val7[30000]

        for i in range(len(val1[j * number:(j + 1) * number])):
            values = [val1[i], val2[i], val3[i], val4[i], val5[i], val6[i], val7[i], val8[i]]
            params.append(values)
            if val7[i] > 10 and num < 5:
                num += 1
                print val1[i], val2[i], val3[i], val4[i], val5[i], val6[i], val7[i], val8[i]
        cursor = conn.cursor()
        cursor.executemany(sql, params)
        conn.commit()
    '''
    params1 = []
    for i in range(10000):
        values = [val1[i], val2[i], val3[i], val4[i], val5[i], val6[i], val7[i], val8[i]]
        params1.append(values)

        # print params[10000: 10010]
    for i in range(10):
        print params1[i*99]
    cursor = conn.cursor()
    cursor.executemany(sql, params1)
    conn.commit()
    cursor.close()
    conn.close()

# read_data('j9000\\YFQMYS.WOBV_DX_QG_GZ_ADC_DK_KBar_21m-_20150105-20180102_12th_bkt.csv')

# read_data('test.csv')

db_name, host_id, user_name, password = [1, 2, 3, 4]

print db_name


