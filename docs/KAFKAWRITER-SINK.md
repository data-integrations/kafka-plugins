[![Build Status](https://travis-ci.org/hydrator/kafka-sink.svg?branch=master)](https://travis-ci.org/hydrator/kafka-sink) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Kafka Sink
==========

CDAP Plugin to write events to kafka in realtime. 

<img align="center" src="plugin-config.png"  width="400" alt="plugin configuration" />


Usage Notes
-----------

Kafka sink plugin that allows you to convert a Structured Record into CSV or JSON.
Plugin has the capability to push the data to one or more Kafka topics. It can
use one of the field values from input to partition the data on topic. The sink
can also be configured to operate in either sync or async mode. 

In case of failure the failed event will not be sent to kafka topic for which it failed. 

Plugin Configuration
---------------------

| Configuration | Required | Default | Description |
| :------------ | :------: | :----- | :---------- |
| **Kafka Brokers** | **Y** | N/A | This configuration specifies kafka brokers list seperated by comma |
| **Kafka Topics** | **Y** | N/A | This configuration specifies list of kafka topics separated by comma |
| **Is Async** | **Y** | N/A | This configuration specifies whether writing the events to broker is *Asynchronous* or *Synchronous*.  |
| **Message Format** | **Y** | N/A | This configuration specifies the format of the event published to Kafka. |
| **Partition Field** | **Y** | N/A | This configuration specifies the input fields that need to be used to determine the partition id; the field type should be int or long. |
| **Message Key Field** | **Y** | N/A | This configuration specifies the input field that should be used as the key for the event published into Kafka. |
