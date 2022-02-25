### Parse nginx log and put in clickhouse database

pip3 install -r requirements.txt

Run log parser:

Settings in config.yaml

```shell 
python3 -m log_analysis.log_parse -c config.yaml -l access.log
```

### Simple Web view results


backend: 

```shell 
python3 -m log_analysis.backend -c config-backend.yaml
```

Frontend:

```shell 
cd web/frontend

npm install

npm start
```
