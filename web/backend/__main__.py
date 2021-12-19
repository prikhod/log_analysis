import argparse

import uvicorn
from clickhouse_driver import Client
from fastapi import FastAPI
from starlette import status
from starlette.responses import Response
from fastapi.middleware.cors import CORSMiddleware
from log_analysis.web.backend.config import parse

app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_clickhouse_client():
    return Client(cfg.clickhouse.host,
                  port=cfg.clickhouse.port,
                  user=cfg.clickhouse.user,
                  password=cfg.clickhouse.password,
                  secure=False,
                  verify=False,
                  database=cfg.clickhouse.database,
                  compression=True)


@app.get("/nginx/access/")
def _root(sort_by: str = None, sort: str = None, skip: int = 0, limit: int = 1000):
    client = get_clickhouse_client()
    table = client.execute('DESCRIBE TABLE access_log')
    columns = [name[0] for name in table]
    sort_query = ''
    if sort and sort_by in columns:
        if sort == 'asc':
            sort_query = f'ORDER BY {sort_by}'
        elif sort == 'desc':
            sort_query = f'ORDER BY {sort_by} DESC'

    data = client.execute(f'SELECT * FROM access_log {sort_query} limit {limit} OFFSET {skip}')
    client.disconnect()
    if data:
        return {"columns": columns, "data": data}
    return Response(status_code=status.HTTP_404_NOT_FOUND)


parser = argparse.ArgumentParser()
parser.add_argument("-c", "--config", help="cfg file")
args = parser.parse_args()
cfg = parse(args.config)

if __name__ == "__main__":
    uvicorn.run("log_analysis.web.backend.__main__:app",
                workers=1,
                host="0.0.0.0",
                port=cfg.port
                )
