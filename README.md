# Spark on AWS EMR - Data lake project

This project exercises the use of:

- Spark SQL queries to parallelise data import from [AWS S3](https://aws.amazon.com/s3/)
- [AWS EMR](https://aws.amazon.com/emr/) using Spark in YARN mode

## Pre-requisites

1. AWS CLI

This will enable us to be authenticated in AWS. The installation guide can be found [here](https://aws.amazon.com/cli/).

2. EC2 Key pair

This key will be used to SSH into the Spark master to submit Spark scripts.

In the EC2 console > create a new key pair (PEM format).

/!\ Do not loose or share this key.

3. Provision the Spark cluster

```console
aws emr create-cluster \
    --name sparkcluster \
    --use-default-roles \
    --release-label emr-5.28.0 \
    --instance-count 2 \
    --applications Name=Spark \
    --ec2-attributes KeyName=[keypair-name] \
    --instance-type m5.xlarge \
    --instance-count 3 --auto-terminate`
```

4. Parquet-tools

This utility can be used to visualise the Parquet files saved by our ETL process.

On MacOS:

```console
brew install parquet-tools
```

Usage:

```console
parquet-tools cat part-00015-699b443b-1a3f-4144-804d-af0c537fcc4c.c000.snappy.parquet
song_id = SOHKNRJ12A6701D1F8
title = Drop of Rain
duration = 189.57016
```

## Usage

### Structure & configuration

The project includes one main file `etl.py`. The ETL process:

- uses Python functions to connect to AWS EMR and load staging tables from S3
- uses schema-on-read techniques to structure the data in fact/dimensions tables
- save these tables back into S3 in parquet files

To interact with AWS EMR and S3, you will need to create a configuration file `dl.cfg` and supply:

- the credentials to access AWS EMR
- S3 locations used for the datasets

Rename `dl.cfg-dist` into `dl.cfg` in the root folder and provide the following configurations:

```ini
AWS_ACCESS_KEY_ID=''
AWS_SECRET_ACCESS_KEY=''
S3_INPUT="s3a://[bucket-name]/"
S3_OUTPUT="s3a://[bucket-name]/"
```

### ETL-ing

Once configured, execute the following commands to run the ETL process in Spark:

```console
scp -i ~/[ec2-keypair-file].pem etl.py hadoop@[spark-master-IP]:~
ssh -i ~/[ec2-keypair-file].pem hadoop@[spark-master-IP]
spark-submit --master yarn etl.py
```

## Datasets

The project datasets are based on a subset from the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/).

A small subset of the data is available in the `data` folder.

### Songs data

You will need to store the data in a S3 bucket with the following structure:

- Song data: s3://mybucket/song_data
- Log data: s3://mybucket/log_data
- Log data json path: s3://mybucket/log_json_path.json

Each file is in JSON format and contains metadata about a song and the artist of that song.

The files are structured in a hierarchy of folders that follow the alphabetical order of the first three letters of each song's track ID.

For example:

```bash
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```

A single song file has the following form:

```json
{
    "artist_id": "ARJNIUY12298900C91",
    "artist_latitude": null,
    "artist_location": "",
    "artist_longitude": null,
    "artist_name": "Adelitas Way",
    "duration": 213.9424,
    "num_songs": 1,
    "song_id": "SOBLFFE12AF72AA5BA",
    "title": "Scream",
    "year": 2009
}
```

### Log Dataset

The logs dataset is generated from this [event simulator](https://github.com/Interana/eventsim) based on the songs in the song dataset. It will be used as our simulation of activity logs from users listening to songs.

The log files are partitioned by year and month. For example,

```bash
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

A single log file has the following form:

```json
{
...
}
{
    "artist": null,
    "auth": "Logged In",
    "firstName": "Walter",
    "gender": "M",
    "itemInSession": 0,
    "lastName": "Frye",
    "length": null,
    "level": "free",
    "location": "San Francisco-Oakland-Hayward, CA",
    "method": "GET",
    "page": "Home",
    "registration": 1540919166796.0,
    "sessionId": 38,
    "song": null,
    "status": 200,
    "ts": 1541105830796,
    "userAgent": "\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"",
    "userId": "39"
}
....
{
...
}
....
```

We will use the `log_json_path.json` JSONPath manifest to inform Redshift on how to import the JSON data files with a `COPY` from S3 command.

```json
{
    "jsonpaths": [
        "$['artist']",
        "$['auth']",
        "$['firstName']",
        "$['gender']",
        "$['itemInSession']",
        "$['lastName']",
        "$['length']",
        "$['level']",
        "$['location']",
        "$['method']",
        "$['page']",
        "$['registration']",
        "$['sessionId']",
        "$['song']",
        "$['status']",
        "$['ts']",
        "$['userAgent']",
        "$['userId']"
    ]
}
```

## Data model

### Star schema

This a visual represenation of the tables emulated with Spark SQL.

![Schema](https://udacity-redshiftsongs.s3-ap-southeast-2.amazonaws.com/schema.png "Songs schema")

### Spark specificities

You can learn more about Spark specificities here:

- [Spark SQL Extract hour/minute/second from timestamp](https://sparkbyexamples.com/spark/spark-extract-hour-minute-and-second-from-timestamp/)
- [Spark SQL date and time functions](https://sparkbyexamples.com/spark/spark-sql-date-and-time-functions/)
- [Spark SQL epoch to timestamp conversion](https://sparkbyexamples.com/spark/spark-epoch-time-to-timestamp-date/)
