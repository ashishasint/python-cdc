### Run Locally
```
python cdc_consumer.py
```

### Steps to deploy on G-Cloud

``` sh
cd python-cdc
```

Authenticate for Application
``` sh
gcloud auth login
```

Build for Cloud Artifact Repository
``` sh
gcloud builds submit --tag gcr.io/integral-iris-449816-g3/asint_python_cdc .
```

Run the app
``` sh
gcloud beta run deploy  --image gcr.io/integral-iris-449816-g3/asint_python_cdc  asint-python-cdc
```