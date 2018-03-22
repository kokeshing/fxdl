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




