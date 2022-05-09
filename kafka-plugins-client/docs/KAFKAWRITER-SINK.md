[![Build Status](https://travis-ci.org/hydrator/kafka-plugins.svg?branch=master)](https://travis-ci.org/hydrator/kafka-plugins) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Kafka Sink
==========

Kafka sink that allows you to write events into CSV or JSON to kafka. Plugin has the capability to push the data to one or more Kafka topics. 
It can use one of the field values from input to partition the data on topic. The sink can also be configured to operate in either sync or async mode. 
The sink also allows you to write events into kerberos-enabled kafka.

<img align="center" src="kafka-sink-plugin-config.png"  width="400" alt="plugin configuration" />

Usage Notes
-----------

Kafka sink emits events in realtime to configured kafka topic and partition. It uses kafka producer [2.6 apis](https://kafka.apache.org/26/documentation.html) to write events into kafka.

This sink can be configured to operate in synchronous or asynchronous mode. In synchronous mode, each event will be sent to the broker synchronously on the thread that calls it. This is not sufficient on most of the high volume environments. 
In async mode, the kafka producer will batch together all the kafka events for greater throughput. But that makes it open for the possibility of dropping unsent events in case of client machine failure. Since kafka producer by default uses synchronous mode, this sink also uses Synchronous producer by default.

It uses String partitioner and String serializer for key and value to write events to kafka. Optionally if kafka key is provided, producer will use that key to partition events accross multiple partitions in a given topic. This sink also allows compression configuration. By default compression is none.

Kafka producer can be tuned using many properties as shown [here](https://kafka.apache.org/26/documentation.html#producerconfigs). This sink allows user to configure any property supported by kafka 2.6 Producer.


Plugin Configuration
---------------------

| Configuration | Required | Default | Description |
| :------------ | :------: | :----- | :---------- |
| **Kafka Brokers** | **Y** | N/A | List of Kafka brokers specified in host1:port1,host2:port2 form. |
| **Kafka Topic** | **Y** | N/A | The Kafka topic to write to. This should be a valid kafka topic string. Kafka topic should already exist. |
| **Is Async** | **Y** | False | Specifies whether writing the events to broker is *Asynchronous* or *Synchronous*.  |
| **Compression Type** | **Y** | none | This configuration specifies the format of the event published to Kafka. |
| **Kerberos Principal** | **N** | N/A | The kerberos principal used for the source when kerberos security is enabled for kafka. |
| **Keytab Location** | **N** | N/A | The keytab location for the kerberos principal when kerberos security is enabled for kafka. |
| **Additional Kafka Producer Properties** | **N** | N/A | Specifies additional kafka producer properties like acks, client.id as key and value pair. |
| **Message Format** | **Y** | CSV | This configuration specifies serialization format of the event published to Kafka. |
| **Message Key Field** | **N** | N/A | This configuration specifies the input field that should be used as the key for the event published into Kafka. This field will be used to partition kafka events across multiple partitions of a topic. Key field should be of type string. |


Build
-----
To build this plugin:

```
   mvn clean package
```    

The build will create a .jar and .json file under the ``target`` directory.
These files can be used to deploy your plugins.

Deployment
----------
You can deploy your plugins using the CDAP CLI:

    > load artifact <target/kafka-plugins-<version>.jar config-file <target/kafka-plugins<version>.json>

For example, if your artifact is named 'kafka-plugins-<version>':

    > load artifact target/kafka-plugins-<version>.jar config-file target/kafka-plugins-<version>.json
    
## Mailing Lists

CDAP User Group and Development Discussions:

* `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from 
users, release announcements, and any other discussions that we think will be helpful 
to the users.

## License and Trademarks

Copyright © 2017 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the 
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
either express or implied. See the License for the specific language governing permissions 
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.      
