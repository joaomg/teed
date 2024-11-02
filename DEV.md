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

- MINIO_VOLUMES "/mnt/data"

Solely for development in the local machine we aren't setting either

MINIO_ROOT_USER or MINIO_ROOT_PASSWORD.

We are using the default minioadmin.

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

An example:

```bash
export ACCESS_KEY=your_access_key
export SECRET_KEY=your_secret_key
```

In the application root directory run the tests:

```bash
pip install -r requirements.test.txt
python -m pytest tests
```

Calculate the code coverage:

```bash
coverage run --source=teed -m pytest tests
coverage report -m > coverage.report
```

## In Windows

The teed package doesn't work in Windows. 

However it is possible to develop teed in Windows.

To do so install WSL and configure Visual Studio Code to connect to the local WSL Ubuntu instance.

See this articly for details:

https://medium.com/@ishreyashkar06/a-quick-guide-to-setting-up-a-python-virtual-environment-through-wsl-in-vs-code-f23c233a313f

Create & activate the virtual enviroment: 

```shell
python3 -m venv .venv
source .venv/bin/activate
```

## Read / Write to filestore status

TODO