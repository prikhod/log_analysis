import asyncio
import argparse
import random
import re
from datetime import datetime
from multiprocessing import Pool

import aiofiles
from aioch import Client

from log_analysis.log_parse.config import parse

pattern = re.compile(
    r"""(?P<ipaddress>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - \[(?P<dateandtime>\d{2}\/[a-z]{3}\/\d{4}:\d{2}:\d{2}:\d{2} (\+|\-)\d{4})\] (\"(?P<method>(GET|POST|OPTIONS|PUT|PATCH|DELETE) )(?P<url>.+)(http\/[1-2]\.[0-9]")) (?P<statuscode>\d{3}) (?P<bytes_sent>\d+) (?P<referer>-|"([^"]+)") (["](?P<useragent>[^"]+)["])""",
    re.IGNORECASE)

TASK_END = 'DONE'


async def file_reader(batch_queue, file, bytes_per_batch):
    """
    Asynchronous read file and put in queue
    :param batch_queue: queue for output lines of file
    :param file: path to log file
    :param bytes_per_batch: size of batch in bytes
    :return:
    """
    async with aiofiles.open(file, 'r') as afp:
        while True:
            lines = await afp.readlines(bytes_per_batch)
            if not lines:
                break
            await batch_queue.put(lines)
    await batch_queue.put(TASK_END)


async def send_db(queue_to_sent, clickhouse):
    """
    Write to db processed batch of file
    :param queue_to_sent: input queue with
    :param clickhouse: clickhouse config
    :return:
    """
    client = Client(clickhouse.host,
                    port=clickhouse.port,
                    user=clickhouse.user,
                    password=clickhouse.password,
                    secure=False,
                    verify=False,
                    database=clickhouse.database,
                    compression=True)
    await client.execute("CREATE TABLE IF NOT EXISTS access_log "
                         "(ip String, datetime DateTime('Europe/Moscow'), "
                         "url String, bytes_sent UInt32, referer String, "
                         "useragent String, status UInt16, method String, access_time UInt16)  "
                         "ENGINE = MergeTree() ORDER BY datetime")
    while True:
        data = await queue_to_sent.get()
        if data == TASK_END:
            queue_to_sent.task_done()
            break
        batch = data.get()
        await client.execute(
            'INSERT INTO access_log '
            '(ip, datetime, url, bytes_sent, referer, useragent, status, method, access_time) '
            'VALUES',
            batch)
        queue_to_sent.task_done()


def worker(lines):
    """
    Processing raw data from file to list of tuples with separate data
    :param lines: batch of file
    :return:
    """
    _result = []
    for line in lines:
        _data = re.search(pattern, line)
        if _data:
            datadict = _data.groupdict()
            ip = datadict["ipaddress"]
            timestamp = int(datetime.strptime(datadict["dateandtime"], '%d/%b/%Y:%H:%M:%S %z').timestamp())
            dateandtime = timestamp
            url = datadict["url"]
            bytes_sent = int(datadict["bytes_sent"])
            referer = datadict["referer"]
            useragent = datadict["useragent"]
            status = int(datadict["statuscode"])
            method = datadict["method"]
            access_time = int((random.random() * 200)) * 10  # генерируем время от 0 до 2с
            _result.append((ip, dateandtime, url, bytes_sent, referer, useragent, status, method, access_time))
    return _result


async def main():
    """
    Main process: create queue, tasks for read file and write to db, run process with workers
    :return:
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="cfg file", required=True)
    parser.add_argument("-l", "--log_file", help="input log file", required=True)
    args = parser.parse_args()
    cfg = parse(args.config)

    batch_queue = asyncio.Queue(maxsize=cfg.num_workers)
    queue_to_sent = asyncio.Queue(maxsize=cfg.num_workers)
    file_task = asyncio.create_task(file_reader(batch_queue, args.log_file, cfg.bytes_per_batch))
    db_task = asyncio.create_task(send_db(queue_to_sent, cfg.clickhouse))
    with Pool(processes=cfg.num_workers) as pool:
        while True:
            batch = await batch_queue.get()
            if batch == TASK_END:
                await queue_to_sent.put(TASK_END)
                batch_queue.task_done()
                break
            data = pool.apply_async(worker, args=(batch,))
            batch_queue.task_done()
            await queue_to_sent.put(data)
        await file_task
        await db_task


if __name__ == '__main__':
    asyncio.run(main())
