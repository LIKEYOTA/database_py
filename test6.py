# coding=utf-8
import pandas as pd
import MySQLdb as mdb
import numpy as np
import time

import matplotlib.pyplot as plt
from matplotlib.offsetbox import AnchoredOffsetbox, TextArea, HPacker


def connect_mysql(tablename):
    conn = mdb.connect(host='localhost', port=3306, user='root', passwd='7ondr', db='testpy')
    cur = conn.cursor()
    sql = 'SELECT * FROM ' + tablename
    cur.execute(sql)
    df = pd.read_sql(sql, conn)
    df.rename(columns={'A': 'date', 'B': 'time', 'C1': 'price', 'D': 'position', 'E': 'fnormpos', 'F': 'fnetreturn',
                       'G': 'normpos', 'H': 'netreturn'}, inplace=True)
    return df


def result_analysis(tablename):
    df = connect_mysql(tablename)

    result = {"NP": 0.0, "MDD": 0.0, "PF": 0.0, "PpT": 0.0, "WR": 0.0, "ShR": 0.0}
    df = df.dropna(how="any")
    df['netreturn'] = df['netreturn'] - df['netreturn'][0]
    numtrade = df.shape[0]  # the length
    datenum = df['date'].apply(lambda d: int(d))
    print(df["normpos"])
    numday = len(datenum.unique())
    hands = df["normpos"].max()
    if 0 == hands:
        return result
    profits = df["netreturn"] / hands
    drawdown = profits - profits.cummax()

    # trade
    pos = df["normpos"] - df["normpos"].shift(1)
    pos[0] = df["normpos"][0]
    tradetimes = np.sum(np.abs(pos.values))
    tradetimes = tradetimes / hands

    tradeprofit = profits.diff()
    tradeprofit.iloc[0] = profits.iloc[0]
    winrate = float((tradeprofit > 0).sum()) / numtrade
    pf = tradeprofit[tradeprofit > 0].sum() / -tradeprofit[tradeprofit < 0].sum()

    # day
    dailyprofitcum = profits.groupby(datenum).last()
    dailyprofit = dailyprofitcum.diff()
    dailyprofit.iloc[0] = dailyprofitcum.iloc[0]
    dailywinrate = float((dailyprofit > 0).sum()) / numday
    dailysharpe = dailyprofit.mean() / dailyprofit.std() * (242 ** 0.5)

    # year
    yearidx = (datenum / 10000).astype(int)
    yearlist = yearidx.unique()
    yearprofit = tradeprofit.groupby(yearidx).sum()

    netProfit = profits.iloc[-1]  # net profit
    maxDrawDown = drawdown.min()  # max drawdown
    numDay = numday  # days
    winRate = winrate * 100  # winrate trade
    dailyWr = dailywinrate * 100  # day winrate
    dailyShr = dailysharpe  # annualized daily sharpe
    PF = pf
    profitpertrade = netProfit / tradetimes

    myDecimalNumStr = ".3f"
    netProfit = float(format(netProfit, myDecimalNumStr))
    maxDrawDown = float(format(maxDrawDown, myDecimalNumStr))
    PF = float(format(PF, myDecimalNumStr))
    profitpertrade = float(format(profitpertrade, myDecimalNumStr))
    winRate = float(format(winrate, myDecimalNumStr))
    dailyShr = float(format(dailyShr, myDecimalNumStr))

    result = {"NP": netProfit, "MDD": maxDrawDown, "PF": PF, "PpT": profitpertrade, "WR": winRate, "ShR": dailyShr}
    print(df)
    print('ok')
    return result


def plot_result(tablename):
    df = connect_mysql(tablename)
    monBaseTen = 100
    yearBaseTen = 10000
    df = df.dropna(how="any")

    df['netreturn'] = df['netreturn'] - df['netreturn'][0]  # 减去第一个

    numtrade = df.shape[0]  # count of all
    # 1 get the unique day
    datenum = df['date'].apply(lambda d: int(d))  # select the only day
    numday = len(datenum.unique())

    # 2 get the unique month
    mon_df = datenum / 100
    mon_df = mon_df.apply(lambda d: int(d))
    num_mons = len(mon_df.unique())

    hands = df["normpos"].max()  # get the max normpos
    if 0 == hands:
        print("None of trade record")
        return -2
    profits = df["netreturn"] / hands  #
    drawdown = profits - profits.cummax()
    drawdown_range = np.array(profits < profits.cummax())

    close = np.array((df["price"]))
    drawdown_close = np.zeros(close.shape)
    drawdown_init_close = np.zeros(close.shape)
    min_close = close[0]
    init_close = min_close
    for ii in np.arange(1, drawdown_range.shape[0]):
        if drawdown_range[ii] and close[ii] < min_close:
            min_close = close[ii]

        elif not (drawdown_range[ii]):
            min_close = close[ii]
            init_close = min_close

        drawdown_close[ii] = min_close
        drawdown_init_close[ii] = init_close
    drawdown_d_close = -drawdown / drawdown_close
    drawdown_d_init_close = -drawdown / drawdown_init_close

    # trade
    pos = df["normpos"] - df["normpos"].shift(1)
    pos[0] = df["normpos"][0]
    tradetimes = np.sum(np.abs(pos.values))
    tradetimes = int(tradetimes / hands / 2)

    tradeprofit = profits.diff()
    tradeprofit.iloc[0] = profits.iloc[0]
    # winrate = float((tradeprofit > 0).sum()) / numtrade
    # pf = tradeprofit[tradeprofit > 0].sum() / -tradeprofit[tradeprofit < 0].sum()

    # day
    dailyprofitcum = profits.groupby(datenum).last()
    dailyprofit = dailyprofitcum.diff()
    dailyprofit.iloc[0] = dailyprofitcum.iloc[0]

    dailywinrate = float((dailyprofit > 0).sum()) / numday
    dailysharpe = dailyprofit.mean() / dailyprofit.std() * (242 ** 0.5)

    isnewhigh = dailyprofit > (dailyprofit.cummax() - 0.001)

    maxwindays = 0
    maxlossdays = 0
    nowdays = 0
    for x in dailyprofit.values:
        if x > 0:
            if nowdays > 0:
                nowdays += 1
            else:
                nowdays = 1
        if x < 0:
            if nowdays < 0:
                nowdays -= 1
            else:
                nowdays = -1
        maxwindays = max(maxwindays, nowdays)
        maxlossdays = min(maxlossdays, nowdays)
    daystonewhigh = 0
    nowdays = 0
    for x in isnewhigh.values:
        if x:
            nowdays = 0
        else:
            nowdays += 1
        daystonewhigh = max(daystonewhigh, nowdays + 1)

    # month
    monIdx = (datenum / monBaseTen).astype(int)

    # year
    yearIdx = (datenum / yearBaseTen).astype(int)

    netProfit = profits.iloc[-1]  # net profit
    maxDrawDown = drawdown.min()  # max drawdown
    tradeTimes = tradetimes
    numDay = numday  # days
    dailyWr = dailywinrate * 100  # day winrate
    dailyShr = dailysharpe  # annualized daily sharpe
    dailyStd = dailyprofit.std()
    PpT = netProfit / tradetimes
    dailyMaxProfit = dailyprofit.max()
    dailyMinProfit = dailyprofit.min()
    maxWinDays = int(maxwindays)
    maxLossDays = int(-maxlossdays)
    daysToNewHigh = int(daystonewhigh)

    ind1 = 'NP = {}'.format('%.1f' % (netProfit))
    ind2 = 'MDD = {}'.format('%.1f' % (maxDrawDown))
    ind3 = 'PpT = {}'.format('%.1f' % (PpT))
    ind4 = 'TT = {}'.format('%.0f' % (tradeTimes))
    ind5 = 'ShR = {}'.format('%.1f' % (dailyShr))
    ind6 = 'Days = {}'.format('%.0f' % (numDay))
    ind7 = 'D_WR = {}'.format('%.1f' % (dailyWr))
    ind8 = 'D_STD = {}'.format('%.1f' % (dailyStd))
    ind9 = 'D_MaxNP = {}'.format('%.1f' % (dailyMaxProfit))
    ind10 = 'D_MinNP = {}'.format('%.1f' % (dailyMinProfit))
    ind11 = 'maxW_Days = {}'.format('%.0f' % (maxWinDays))
    ind12 = 'maxL_Days = {}'.format('%.0f' % (maxLossDays))

    ind13 = 'mDD_mC = {}'.format('%.2f' % (drawdown_d_close.max()))
    ind14 = 'mDD_iC = {}'.format('%.2f' % (drawdown_d_init_close.max()))

    max_drawdown_d_close_id = drawdown_d_close[drawdown_d_close == drawdown_d_close.max()]
    max_drawdown_d_init_close_id = drawdown_d_init_close[drawdown_d_init_close == drawdown_d_init_close.max()]

    fig = plt.figure(figsize=(44, 20))
    # title = fileName.split('/')
    # plt.title(title[-1])
    plt.subplots_adjust(left=0.05, right=0.95, top=0.95, bottom=0.05)

    plt.fill_between(df.index, 0, drawdown, facecolor='red', color='red')
    plt.fill_between(df.index, 0, profits, facecolor='green', color='green')

    fsize = 20
    monIdxDiff = monIdx.diff()
    monIdxDiff[0] = 1
    monIdxDiff.iloc[df.index[-1]] = 1
    xind = monIdxDiff[monIdxDiff > 0].index
    nowsum = 0
    xT = []
    yT = []
    ymax = profits.values.max()
    ymin = profits.values.min()
    for x in xind.values:
        ptd = profits.values[x]
        y = ptd - nowsum

        if y > 0:
            bbox_props = dict(boxstyle='round', ec='none', fc='#F4F4F4', alpha=0.7)
            plt.text(x, profits.values[x] / 2, float(format(y, '.2f')), size=fsize, bbox=bbox_props, color='#000000')
        else:
            bbox_props = dict(boxstyle='round', ec='none', fc='#F4F4F4', alpha=0.7)
            plt.text(x, drawdown.values[x] / 2, float(format(y, '.2f')), size=fsize, bbox=bbox_props, color='#000000')

        xT.append(x)
        yT.append(str(datenum.values[x]))
        nowsum = ptd
    plt.xticks(xT, yT)
    plt.tick_params(labelsize=fsize)

    for ii in np.arange(max_drawdown_d_close_id.index.shape[0]):
        x = max_drawdown_d_close_id.index[ii]
        bbox_props = dict(boxstyle='round', ec='none', fc='r', alpha=0.5)
        plt.text(x, profits.max() / 2, "mDD_mC", size=fsize, bbox=bbox_props, color='#000000')

    for ii in np.arange(max_drawdown_d_init_close_id.index.shape[0]):
        x = max_drawdown_d_init_close_id.index[ii]
        bbox_props = dict(boxstyle='round', ec='none', fc='b', alpha=0.5)
        plt.text(x, profits.max() / 2, "mDD_iC", size=fsize, bbox=bbox_props, color='#000000')

    fsize = 20
    bbox_props = dict(boxstyle='round', ec='g', fc='none')
    box1 = TextArea(ind1, textprops=dict(size=fsize, bbox=bbox_props))
    bbox_props = dict(boxstyle='round', ec='r', fc='none')
    box2 = TextArea(ind2, textprops=dict(size=fsize, bbox=bbox_props))
    bbox_props = dict(boxstyle='round', ec='b', fc='none')
    box3 = TextArea(ind3, textprops=dict(size=fsize, bbox=bbox_props))
    bbox_props = dict(boxstyle='round', ec='c', fc='none')
    box4 = TextArea(ind4, textprops=dict(size=fsize, bbox=bbox_props))
    bbox_props = dict(boxstyle='round', ec='m', fc='none')
    box5 = TextArea(ind5, textprops=dict(size=fsize, bbox=bbox_props))
    bbox_props = dict(boxstyle='round', ec='y', fc='none')
    box6 = TextArea(ind6, textprops=dict(size=fsize, bbox=bbox_props))
    bbox_props = dict(boxstyle='round', ec='g', fc='none')
    box7 = TextArea(ind7, textprops=dict(size=fsize, bbox=bbox_props))
    bbox_props = dict(boxstyle='round', ec='r', fc='none')
    box8 = TextArea(ind8, textprops=dict(size=fsize, bbox=bbox_props))
    bbox_props = dict(boxstyle='round', ec='b', fc='none')
    box9 = TextArea(ind9, textprops=dict(size=fsize, bbox=bbox_props))
    bbox_props = dict(boxstyle='round', ec='c', fc='none')
    box10 = TextArea(ind10, textprops=dict(size=fsize, bbox=bbox_props))
    bbox_props = dict(boxstyle='round', ec='m', fc='none')
    box11 = TextArea(ind11, textprops=dict(size=fsize, bbox=bbox_props))
    bbox_props = dict(boxstyle='round', ec='y', fc='none')
    box12 = TextArea(ind12, textprops=dict(size=fsize, bbox=bbox_props))
    bbox_props = dict(boxstyle='round', ec='g', fc='none')
    box13 = TextArea(ind13, textprops=dict(size=fsize, bbox=bbox_props))
    bbox_props = dict(boxstyle='round', ec='r', fc='none')
    box14 = TextArea(ind14, textprops=dict(size=fsize, bbox=bbox_props))

    box = HPacker(children=[box1, box2, box13, box14, box3, box4, box5, box6,
                            box7, box8, box9, box10, box11, box12],
                  pad=0, sep=fsize - 5)

    ax = plt.gca()
    anchored_box = AnchoredOffsetbox(loc=2, child=box, pad=0.2, frameon=False)
    ax.add_artist(anchored_box)

    ax.grid(True)
    ax.autoscale_view()
    fig.autofmt_xdate()

    figname = tablename + '.png'
    plt.savefig(figname)
    plt.close()

    return 0


def plot_result1(tablename):
    t1 = time.time()
    df = connect_mysql(tablename)
    t2 = time.time() - t1
    print t2
    monBaseTen = 100
    yearBaseTen = 10000
    df = df.dropna(how="any")

    print len(df['netreturn'])
    df['netreturn'] = df['netreturn'] - df['netreturn'][0]  # 减去第一个

    numtrade = df.shape[0]  # count of all
    # 1 get the unique day
    datenum = df['date'].apply(lambda d: int(d))  # select the only day
    numday = len(datenum.unique())

    # 2 get the unique month
    mon_df = datenum / 100
    mon_df = mon_df.apply(lambda d: int(d))
    num_mons = len(mon_df.unique())

    hands = df["normpos"].max()  # get the max normpos
    if 0 == hands:
        print("None of trade record")
        return -2
    profits = df["netreturn"] / hands  #
    drawdown = profits - profits.cummax()
    drawdown_range = np.array(profits < profits.cummax())

    close = np.array((df["price"]))
    drawdown_close = np.zeros(close.shape)
    drawdown_init_close = np.zeros(close.shape)
    min_close = close[0]
    init_close = min_close
    for ii in np.arange(1, drawdown_range.shape[0]):
        if drawdown_range[ii] and close[ii] < min_close:
            min_close = close[ii]

        elif not (drawdown_range[ii]):
            min_close = close[ii]
            init_close = min_close

        drawdown_close[ii] = min_close
        drawdown_init_close[ii] = init_close
    drawdown_d_close = -drawdown / drawdown_close
    drawdown_d_init_close = -drawdown / drawdown_init_close

    # trade
    pos = df["normpos"] - df["normpos"].shift(1)
    pos[0] = df["normpos"][0]
    tradetimes = np.sum(np.abs(pos.values))
    tradetimes = int(tradetimes / hands / 2)

    tradeprofit = profits.diff()
    tradeprofit.iloc[0] = profits.iloc[0]
    # winrate = float((tradeprofit > 0).sum()) / numtrade
    # pf = tradeprofit[tradeprofit > 0].sum() / -tradeprofit[tradeprofit < 0].sum()

    # day
    dailyprofitcum = profits.groupby(datenum).last()
    dailyprofit = dailyprofitcum.diff()
    dailyprofit.iloc[0] = dailyprofitcum.iloc[0]

    dailywinrate = float((dailyprofit > 0).sum()) / numday
    dailysharpe = dailyprofit.mean() / dailyprofit.std() * (242 ** 0.5)

    isnewhigh = dailyprofit > (dailyprofit.cummax() - 0.001)

    maxwindays = 0
    maxlossdays = 0
    nowdays = 0
    for x in dailyprofit.values:
        if x > 0:
            if nowdays > 0:
                nowdays += 1
            else:
                nowdays = 1
        if x < 0:
            if nowdays < 0:
                nowdays -= 1
            else:
                nowdays = -1
        maxwindays = max(maxwindays, nowdays)
        maxlossdays = min(maxlossdays, nowdays)
    daystonewhigh = 0
    nowdays = 0
    for x in isnewhigh.values:
        if x:
            nowdays = 0
        else:
            nowdays += 1
        daystonewhigh = max(daystonewhigh, nowdays + 1)

    # month
    monIdx = (datenum / monBaseTen).astype(int)

    # year
    yearIdx = (datenum / yearBaseTen).astype(int)

    netProfit = profits.iloc[-1]  # net profit
    maxDrawDown = drawdown.min()  # max drawdown
    tradeTimes = tradetimes
    numDay = numday  # days
    dailyWr = dailywinrate * 100  # day winrate
    dailyShr = dailysharpe  # annualized daily sharpe
    dailyStd = dailyprofit.std()
    PpT = netProfit / tradetimes
    dailyMaxProfit = dailyprofit.max()
    dailyMinProfit = dailyprofit.min()
    maxWinDays = int(maxwindays)
    maxLossDays = int(-maxlossdays)
    daysToNewHigh = int(daystonewhigh)

    '''
    ind1 = 'NP = {}'.format('%.1f' % (netProfit))
    ind2 = 'MDD = {}'.format('%.1f' % (maxDrawDown))
    ind3 = 'PpT = {}'.format('%.1f' % (PpT))
    ind4 = 'TT = {}'.format('%.0f' % (tradeTimes))
    ind5 = 'ShR = {}'.format('%.1f' % (dailyShr))
    ind6 = 'Days = {}'.format('%.0f' % (numDay))
    ind7 = 'D_WR = {}'.format('%.1f' % (dailyWr))
    ind8 = 'D_STD = {}'.format('%.1f' % (dailyStd))
    ind9 = 'D_MaxNP = {}'.format('%.1f' % (dailyMaxProfit))
    ind10 = 'D_MinNP = {}'.format('%.1f' % (dailyMinProfit))
    ind11 = 'maxW_Days = {}'.format('%.0f' % (maxWinDays))
    ind12 = 'maxL_Days = {}'.format('%.0f' % (maxLossDays))

    ind13 = 'mDD_mC = {}'.format('%.2f' % (drawdown_d_close.max()))
    ind14 = 'mDD_iC = {}'.format('%.2f' % (drawdown_d_init_close.max()))
    '''
    sql = "insert into %s(NP, MDD, PpT, TT, ShR, Days, " \
          "D_WR, D_STD, D_MaxNP, D_MinNP, maxW_Days, maxL_Days, mDD_mC, mDD_iC)" \
          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    ind1 = netProfit
    ind2 = maxDrawDown
    ind3 = PpT
    ind4 = tradeTimes
    ind5 = dailyShr
    ind6 = numDay
    ind7 = dailyWr
    ind8 = dailyStd
    ind9 = dailyMaxProfit
    ind10 = dailyMinProfit
    ind11 = maxWinDays
    ind12 = maxLossDays

    ind13 = drawdown_d_close.max()
    ind14 = drawdown_d_init_close.max()

    print t2
    t3 = (time.time() - t2 - t1)
    print t3
    return ind1, ind2, ind3, ind4, ind5, ind6, ind7, ind8, ind9, ind10, ind11, ind12, ind13, ind14


def test_unit():
    table_name = 'yfqmyskbar_21m__20150105_20180102_12th'
    df = connect_mysql(table_name)
    df = df.dropna(how="any")
    l1 = len(df["normpos"])
    hands = df["normpos"].max()
    print df['normpos'][119135]
    print df['normpos'][146177]
    print hands, l1

'''
table_name = 'sj7znekbar_20m__20150105_20180102_9th'
table_name = 'yfqmyskbar_21m__20150105_20180102_12th'
# table_name1 = 'csv_table'
aa = plot_result1(table_name)
print(aa)
'''
# test_unit()
'''
1.09200000763
1.09200000763
0.141000032425
('NP = 602.0', 'MDD = -6214.0', 'PpT = 5.1', 'TT = 117', 'ShR = 1.3', 'Days = 59', 'D_WR = 42.4', 'D_STD = 508.7', 'D_MaxNP = 1210.0', 'D_MinNP = -1036.0', 'maxW_Days = 4', 'maxL_Days = 5', 'mDD_mC = 2.20', 'mDD_iC = 2.01')
'''
