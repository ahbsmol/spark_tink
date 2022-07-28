import tinkoff-investments
import json
from tinkoff.invest import CandleInterval, Client
from datetime import datetime
from os import system

if __name__ == '__main__':
    TOKEN = ''

    date_1 = '01/07/2022'
    date_2 = '05/07/2022'
    dt_object1 = datetime.strptime(date_1, "%d/%m/%Y")
    dt_object2 = datetime.strptime(date_2, "%d/%m/%Y")

    def main(figi):
        list_1 = []
        with Client(TOKEN) as client:
            for candle in client.get_all_candles(
                figi=figi,
                interval=CandleInterval.CANDLE_INTERVAL_HOUR,
                from_=dt_object1,
                to=dt_object2
            ):
                list_1.append(candle)
        return list_1

    figis = ['BBG00T22WKV5', 'BBG00R05JT04', 'BBG00KHGQP89', 'BBG00Q9K64Q5', 'BBG012NW2KW6', 'BBG00H2TYMQ2', 'BBG011MDDX15', 'BBG011PC1CT3', 'BBG012YS2TP2', 'TCS00A102LD1', 'BBG00YMT3ZH8']

    time_format = "%Y%m%d"
    now_date = datetime.strftime(datetime.now(), time_format)

    system(f'hdfs dfs -mkdir -p /data/quotations_src/load_dt={now_date}')
    for k in range(len(figis)):
        list1 = main(figis[k])
        list2 = []
        for i in list1[2:]:
            m = i.__dict__
            [m[j] = m[j].__dict__ for j in ('open', 'high', 'low', 'close')]
            m['time'] = datetime.strftime(m['time'], "%d/%m/%Y %H:%m")
            list2.append(json.dumps(m))
        with open(f'./historydata_{now_date}_' + str(k) + '.json', 'w+') as f:
            f.write("\n".join(list2))
        system(f'hdfs dfs -mkdir -p historydata_{now_date}_{str(k)}.json /data/quotations_src/load_dt={now_date}/figi={figis[k]}')
        system(f'hdfs dfs -put historydata_{now_date}_{str(k)}.json /data/quotations_src/load_dt={now_date}/figi={figis[k]}/data.json')
