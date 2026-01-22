### Run Locally

In one terminal

1. ```
   .\cloud-sql-proxy.exe <DB_URL> --port=<PORT_NUMBER> --debug
   ```

In 2nd terminal 
2. ```
python cdc_consumer.py

In 3rd terminal 
2. ```
python test_changes.py


To check the WAL size run this file `python check_wal.py`

### Steps to deploy on G-Cloud

```sh
cd python-cdc
```

Authenticate for Application

```sh
gcloud auth login
```

Build for Cloud Artifact Repository

```sh
gcloud builds submit --tag gcr.io/integral-iris-449816-g3/asint_python_cdc .
```

Run the app

```sh
gcloud beta run deploy  --image gcr.io/integral-iris-449816-g3/asint_python_cdc  asint-python-cdc
```
````
