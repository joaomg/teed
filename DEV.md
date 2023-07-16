## Develop teed

```bash
cd teed
source venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
pip install -r requirements.dev.txt

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

```bash
wsl --shutdown
```

Alternatively it is possible to use Docker:

https://min.io/docs/minio/container/operations/install-deploy-manage/deploy-minio-single-node-single-drive.html

Pull docker image.

```bash
docker pull minio/minio
```

Copy config_minio.env file to /etc/default/minio

```bash
mkdir /etc/default/minio
cp config_minio.env /etc/default/minio/config_minio.env
```

The config_minio.env sets the env variables:

- MINIO_ROOT_USER myminioadmin
- MINIO_ROOT_PASSWORD bigSecret2023
- MINIO_VOLUMES "/mnt/data"

Create the /mnt/data directory which will hold the MinIO data.

```bash
mkdir /mnt/data
```

Launch the MinIO docker container.
Notice how the environment variable file is passed.

```bash
docker run -dt                                  \
  -p 9000:9000 -p 9090:9090                     \
  -v PATH:/mnt/data                             \
  -v /etc/default/minio/config_minio.env:/etc/config.env         \
  -e "MINIO_CONFIG_ENV_FILE=/etc/config.env"    \
  --name "minio_local"                          \
  minio/minio server --console-address ":9090"
```

Access the MinIO console and create a access and secret key:

http://localhost:9090/access-keys

Set them as environment variables.

An exem

```bash
export ACCESS_KEY=1atuJoRDF8iy2BR40Yv6
export SECRET_KEY=6EKNs22XJvMX7RiXWMwW84xxO1ppnStkA6C6kEDh
```

In the application root directory run the tests:

```bash
python -m pytest tests
```

Calculate the code coverage:

```bash
coverage run --source=teed -m pytest tests
coverage report -m > coverage.report
```
