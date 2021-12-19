
pip3 install -r requirements.txt

Run log parser:

Settings in config.yaml

```python3 -m log_analysis.log_parse -c config.yaml -l generated-access.log```

Queries:

```python3 -m log_analysis.queries -c config.yaml```

Web:

backend: 

```python3 -m log_analysis.backend -c config-backend.yaml```

Frontend:

```cd web/frontend```

```npm install```

```npm start```