# llm-app


```
# run api server
poetry run uvicorn src.llm_app.api.main:app --host localhost --reload

# run dredd
poetry run dredd ./work/openapi.json http://127.0.0.1:8000 --server "uvicorn src.llm_app.api.main:app"
```
