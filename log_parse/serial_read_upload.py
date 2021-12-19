import argparse
import random
import re
import time
from datetime import datetime
from itertools import islice
from multiprocessing import Pool

from clickhouse_driver import Client

from log_analysis.log_parse.config import parse


pattern = re.compile(r"""(?P<ipaddress>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - \[(?P<dateandtime>\d{2}\/[a-z]{3}\/\d{4}:\d{2}:\d{2}:\d{2} (\+|\-)\d{4})\] (\"(?P<method>(GET|POST|OPTIONS|PUT|PATCH|DELETE) )(?P<url>.+)(http\/[1-2]\.[0-9]")) (?P<statuscode>\d{3}) (?P<bytes_sent>\d+) (?P<referer>-|"([^"]+)") (["](?P<useragent>[^"]+)["])""", re.IGNORECASE)


def worker(lines):
    _result = []
    for line in lines:
        _data = re.search(pattern, line)
        if _data:
            datadict = _data.groupdict()
            ip = datadict["ipaddress"]
            timestamp = int(datetime.strptime(datadict["dateandtime"], '%d/%b/%Y:%H:%M:%S %z').timestamp())
            dateandtime = timestamp - random.randrange(0, 10**9)  # решение для обеспечения уникальности записей
            url = datadict["url"]
            bytes_sent = int(datadict["bytes_sent"])
            referer = datadict["referer"]
            useragent = datadict["useragent"]
            status = int(datadict["statuscode"])
            method = datadict["method"]
            access_time = int((random.random() * 200)) * 10  # генерирую время от 0 до 2с
            _result.append((ip, dateandtime, url, bytes_sent, referer, useragent, status, method, access_time))
    return _result


def batch_file(log_file, lines_per_batch):
    with open(log_file, 'r') as f:
        while True:
            _batch = list(islice(f, lines_per_batch))
            if not _batch:
                return
            yield _batch


def send_db(_data, _client):
    _client.execute(
        'INSERT INTO access_log_test(ip, datetime, url, bytes_sent, referer, useragent, status, method, access_time) VALUES',
        _data)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="cfg file", required=True)
    parser.add_argument("-l", "--log_file", help="input log file", required=True)

    args = parser.parse_args()
    cfg = parse(args.config)

    client = Client(cfg.clickhouse.host,
                    port=cfg.clickhouse.port,
                    user=cfg.clickhouse.user,
                    password=cfg.clickhouse.password,
                    secure=False,
                    verify=False,
                    database=cfg.clickhouse.database,
                    compression=True)
    client.execute("CREATE TABLE IF NOT EXISTS access_log_test "
                   "(ip String, datetime DateTime('Europe/Moscow'), "
                   "url String, bytes_sent UInt32, referer String, "
                   "useragent String, status UInt16, method String, access_time UInt16)  "
                   "ENGINE = MergeTree() ORDER BY datetime")
    start = time.time()
    result = []
    with Pool(processes=cfg.num_workers) as pool:
        for batch in batch_file(args.log_file, cfg.lines_per_batch):
            result.append(pool.apply_async(worker, args=(batch, )))
        for r in result:
            data = r.get()
            send_db(data, client)
            print(f'batch send in {time.time() - start}s')
    client.disconnect()
    print(f'complete in {time.time() - start}s')


if __name__ == '__main__':
    main()
