# Transitdata-metro-ats-parser [![Test and create Docker image](https://github.com/HSLdevcom/transitdata-metro-ats-parser/actions/workflows/test-and-build.yml/badge.svg)](https://github.com/HSLdevcom/transitdata-metro-ats-parser/actions/workflows/test-and-build.yml)

## Description

Application for parsing metro ATS messages from raw JSON messages received from the metro ATS API over MQTT. The data from the raw JSON is combined with static data in Redis (see [transitdata-cache-bootstrapper](https://github.com/HSLdevcom/transitdata-cache-bootstrapper)) to add route IDs and other metadata to the message. Messages are read
from one Pulsar topic and the output is written to another Pulsar topic.

## Building

### Dependencies

This project depends on [transitdata-common](https://github.com/HSLdevcom/transitdata-common) project.

### Locally

- `mvn compile`
- `mvn package`

### Docker image

- Run [this script](build-image.sh) to build the Docker image

## Running

### Dependencies

* Pulsar
* Redis

### Environment variables

* `METRO_ATS_TIMEZONE`: timezone used in metro ATS API messages
* `PUBTRANS_TIMEZONE`: timezone used in PubTrans
* `ADDED_TRIPS_ENABLED`: whether to create messages for those metros that are not found from the static schedule (PubTrans)
