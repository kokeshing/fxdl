import sys
import mysql.connector
from datetime import datetime, timedelta
import oandapy
import json

class DB_operation:
    def __init__(self):
        self.connect()

    def connect(self):

        self.cn = mysql.connector.connect(
            host = 'mysite-mysql.cbory55wxan7.ap-northeast-1.rds.amazonaws.com',
            port = 3306,
            user = 'kokeshi',
            password = 'Kokeshi1126',
            database = 'predict_fx'
        )

    def insert_db(self):
        now = datetime.now()
        
        self.cn.ping(reconnect=True)
        
        cur = self.cn.cursor(dictionary=True)
        cur.execute('SELECT * FROM t_ratelog WHERE YMD=(SELECT MAX(YMD) FROM t_ratelog)')

        result = cur.fetchone()
        latest = result["YMD"]
        if latest.weekday() == 5 and (latest.hour == 6 or latest.hour == 5) and latest.minute == 59:
            latest += timedelta(hours=48)
        elif latest == (now - timedelta(minutes=1)):
            print("DB already updated")
            sys.exit()


        oanda = oandapy.API(
                    environment  = "practice",
                    access_token = "86bad25a255583f7e658dafab488c852-b73991449cadc1c04dd7327de2b470ea"
                    )

        if latest > (now - timedelta(minutes=1) - timedelta(days=30)):
            if latest + timedelta(minutes=500) > now:
                start = (latest + timedelta(minutes=1) - timedelta(hours=9)).strftime('%Y-%m-%dT%H:%M:%S.000000Z')
                end   = (now - timedelta(minutes=1) - timedelta(hours=9)).strftime('%Y-%m-%dT%H:%M:%S.000000Z')
                response = oanda.get_history(
                             instrument   = "USD_JPY", # 銘柄
                             granularity  = "M1", # 1分足
                             start        = start,
                             end          = end,
                             candleFormat = "midpoint"
                             )

                self.cn.ping(reconnect=True)
                for candle in response["candles"]:
                    volume = '%d' % candle["volume"]
                    Open   = '%5.2f' % candle["openMid"]
                    High   = '%5.2f' % candle["highMid"]
                    Low    = '%5.2f' % candle["lowMid"]
                    ymd    = datetime.strptime(candle["time"], '%Y-%m-%dT%H:%M:%S.000000Z') + timedelta(hours=9)
                    ymd    = ymd.strftime('%Y-%m-%d %H:%M:%S')
                    Close  = '%5.2f' % candle["closeMid"]
                    strSQL = 'INSERT INTO t_ratelog (YMD, Open, High, Low, Close, volume) VALUES (\'%s\', %s, %s, %s, %s, %s)' % (ymd, Open, High, Low, Close, volume)

                    try:
                        cur = self.cn.cursor()
                        cur.execute(strSQL)
                        self.cn.commit()
                    except:
                        self.cn.rollback()
                        raise

            else:
                i = 0
                while latest + timedelta(minutes=500*(i+1)) < now:
                    start = (latest + timedelta(minutes=500*i) + timedelta(minutes=1) - timedelta(hours=9)).strftime('%Y-%m-%dT%H:%M:%S.000000Z')
                    end   = (latest + timedelta(minutes=500*(i+1)) - timedelta(hours=9)).strftime('%Y-%m-%dT%H:%M:%S.000000Z')
                    response = oanda.get_history(
                                instrument   = "USD_JPY", # 銘柄
                                granularity  = "M1", # 1分足
                                start        = start,
                                end          = end,
                                candleFormat = "midpoint"
                                )
                    i += 1

                    self.cn.ping(reconnect=True)
                    for candle in response["candles"]:
                        volume = '%d' % candle["volume"]
                        Open   = '%5.2f' % candle["openMid"]
                        High   = '%5.2f' % candle["highMid"]
                        Low    = '%5.2f' % candle["lowMid"]
                        ymd    = datetime.strptime(candle["time"], '%Y-%m-%dT%H:%M:%S.000000Z') + timedelta(hours=9)
                        ymd    = ymd.strftime('%Y-%m-%d %H:%M:%S')
                        Close  = '%5.2f' % candle["closeMid"]
                        strSQL = 'INSERT INTO t_ratelog (YMD, Open, High, Low, Close, volume) VALUES (\'%s\', %s, %s, %s, %s, %s)' % (ymd, Open, High, Low, Close, volume)

                        try:
                            cur = self.cn.cursor()
                            cur.execute(strSQL)
                            self.cn.commit()
                        except:
                            self.cn.rollback()
                            raise

                self.insert_db()

        else:
            i = 0
            latest = now - timedelta(days=30) - timedelta(minutes=1)
            while latest + timedelta(minutes=500*(i+1)) < now:
                start = (latest + timedelta(minutes=500*i) + timedelta(minutes=1) - timedelta(hours=9)).strftime('%Y-%m-%dT%H:%M:%S.000000Z')
                end   = (latest + timedelta(minutes=500*(i+1)) - timedelta(hours=9)).strftime('%Y-%m-%dT%H:%M:%S.000000Z')
                response = oanda.get_history(
                            instrument   = "USD_JPY", # 銘柄
                            granularity  = "M1", # 1分足
                            start        = start,
                            end          = end,
                            candleFormat = "midpoint"
                            )
                i += 1

                self.cn.ping(reconnect=True)
                for candle in response["candles"]:
                    volume = '%d' % candle["volume"]
                    Open   = '%5.2f' % candle["openMid"]
                    High   = '%5.2f' % candle["highMid"]
                    Low    = '%5.2f' % candle["lowMid"]
                    ymd    = datetime.strptime(candle["time"], '%Y-%m-%dT%H:%M:%S.000000Z') + timedelta(hours=9)
                    ymd    = ymd.strftime('%Y-%m-%d %H:%M:%S')
                    Close  = '%5.2f' % candle["closeMid"]
                    strSQL = 'INSERT INTO t_ratelog (YMD, Open, High, Low, Close, volume) VALUES (\'%s\', %s, %s, %s, %s, %s)' % (ymd, Open, High, Low, Close, volume)

                    try:
                        cur = self.cn.cursor()
                        cur.execute(strSQL)
                        self.cn.commit()
                    except:
                        self.cn.rollback()
                        raise

            self.insert_db()

    def update_predict(self, YMD, predict):
        strSQL = 'UPDATE t_ratelog SET predict = %d WHERE YMD = \'%s\'' % (predict, YMD)
        self.cn.ping(reconnect=True)
        cur = self.cn.cursor()

        try:
            cur.execute(strSQL)
            self.cn.commit()
        except:
            self.cn.rollback()
            raise

    def select_Query(self, strSQL):
        self.cn.ping(reconnect=True)
        cur = self.cn.cursor(dictionary=True)
        cur.execute(strSQL)
        return cur.fetchall()

    def update_result(self, YMD, result, prd_true_flg):
        strSQL = 'UPDATE t_ratelog SET result = %d, prd_true_flg = %d WHERE YMD = \'%s\'' % (result, prd_true_flg, YMD)
        self.cn.ping(reconnect=True)
        cur = self.cn.cursor()

        try:
            cur.execute(strSQL)
            self.cn.commit()
        except:
            self.cn.rollback()
            raise
    
    def check_result(self):
        now = datetime.now()
        start = (now - timedelta(days=2)).strftime('%Y-%m-%d 23:38:00')
        end   = (now - timedelta(days=1)).strftime('%Y-%m-%d 23:30:00')
        strSQL = 'SELECT * FROM t_ratelog WHERE (predict != -1) AND (YMD BETWEEN \'%s\' AND \'%s\') ORDER BY YMD ASC' % (start, end)
        
        resultSQL = self.select_Query(strSQL)

        for row in resultSQL:
            ymd = (row['YMD']).strftime('%Y-%m-%d %H:%M:00')
            ymd_af30 = (row['YMD'] + timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:00')
            selectSQL = 'SELECT * FROM t_ratelog WHERE YMD = \'%s\'' % ymd_af30

            result_af30 = select_Query(selectSQL)
            
            if result_af30[0]['Close'] > row['High']:
                result = 2
                prd_true_flg = 1 if row['predict'] == result else 0
            elif result_af30[0]['Close'] < row['Low']:
                result = 0
                prd_true_flg = 1 if row['predict'] == result else 0
            else:
                result = 1
                prd_true_flg = 1 if row['predict'] == result else 0

            update_result(ymd, result, prd_true_flg)

    def count_sql(self, strWHERE):
        strSQL = 'SELECT count(*) FROM ' + strWHERE
        result = select_Query(strSQL)

        return result[0]['count(*)']

    def check_hit_rate(self):
        update_ymd = (datetime.now()).strftime('%Y-%m-%d')
        strWHEREhit = 't_ratelog WHERE prd_true_flg = 1'
        result_hit = count_sql(strWHEREhit)
        
        strWHEREnohit = 't_ratelog WHERE prd_true_flg = 0'
        result_no_hit = count_sql(strWHEREnohit)

        strSQL = 'INSERT INTO t_ratelog (YMD, hit_predict, no_hit_predict) VALUES (\'%s\', %d, %d)' % (update_ymd, result_hit, result_no_hit)
        self.cn.ping(reconnect=True)
        cur = self.cn.cursor()

        try:
            cur.execute(strSQL)
            self.cn.commit()
        except:
            self.cn.rollback()
            raise





