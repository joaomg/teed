## Develop teed

```shell
cd teed
source venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## How to test

Install and launch the MinIO server.

https://min.io/docs/minio/linux/index.html

Start the minio server in Linux/WSL:

```bash
mkdir ~/minio
minio server ~/minio --console-address :9090
```

When using WSL2 it might be needed to run, in Powershell:

```
wsl --shutdown
```

In the application root directory run the tests:

```
python -m pytest tests
```

Calculate the code coverage:

```
coverage run --source=teed -m pytest tests
coverage report -m > coverage.report
```
