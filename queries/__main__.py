import argparse
from pprint import pprint

from clickhouse_driver import Client

from log_analysis.log_parse.config import parse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="cfg file", required=True)

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

    reply = client.execute('SELECT ip, COUNT(*) as cnt FROM access_log GROUP BY ip ORDER BY cnt DESC')
    pprint('ip request frequency')
    pprint(reply)
    reply = client.execute('SELECT useragent, COUNT(*) as cnt FROM access_log GROUP BY useragent ORDER BY cnt DESC')
    pprint('browsers/user-agent')
    pprint(reply)
    reply = client.execute('SELECT status, COUNT(*) as cnt FROM access_log GROUP BY status ORDER BY cnt DESC')
    pprint('status code')
    pprint(reply)
    reply = client.execute('SELECT access_time, COUNT(*) as cnt FROM access_log GROUP BY access_time ORDER BY cnt DESC')
    pprint('access_time')
    pprint(reply)
    reply = client.execute('SELECT method, COUNT(*) as cnt FROM access_log GROUP BY method ORDER BY cnt DESC')
    pprint('methods')
    pprint(reply)


if __name__ == '__main__':
    main()
