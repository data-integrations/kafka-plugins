[![Build Status](https://travis-ci.org/hydrator/kafka-plugins.svg?branch=master)](https://travis-ci.org/hydrator/kafka-plugins) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Kafka Source
===========

Kafka streaming source that emits a records with user specified schema.

<img align="center" src="kafka-source-plugin-config.png"  width="400" alt="plugin configuration" />

Usage Notes
-----------

Kafka Streaming Source can be used to read events from a kafka topic. It uses kafka consumer [2.6 apis](https://kafka.apache.org/26/documentation.html) to read events from a kafka topic. Kafka Source converts incoming kafka events into cdap structured records which then can be used for further transformations.

The source provides capabilities to read from latest offset or from beginning or from the provided kafka offset. The plugin relies on Spark Streaming offset [storage capabilities](https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html) to manager offsets and checkpoints.

Plugin Configuration
---------------------

| Configuration | Required | Default | Description |
| :------------ | :------: | :----- | :---------- |
| **Kafka Brokers** | **Y** | N/A | List of Kafka brokers specified in host1:port1,host2:port2 form. |
| **Kafka Topic** | **Y** | N/A | The Kafka topic to read from. |
| **Topic Partition** | **N** | N/A | List of topic partitions to read from. If not specified, all partitions will be read.  |
| **Default Initial Offset** | **N** | N/A | The default initial offset for all topic partitions. An offset of -2 means the smallest offset. An offset of -1 means the latest offset. Defaults to -1. Offsets are inclusive. If an offset of 5 is used, the message at offset 5 will be read. If you wish to set different initial offsets for different partitions, use the initialPartitionOffsets property. |
| **Initial Partition Offsets** | **N** | N/A | The initial offset for each topic partition. If this is not specified, all partitions will use the same initial offset, which is determined by the defaultInitialOffset property. Any partitions specified in the partitions property, but not in this property will use the defaultInitialOffset. An offset of -2 means the smallest offset. An offset of -1 means the latest offset. Offsets are inclusive. If an offset of 5 is used, the message at offset 5 will be read. |
| **Time Field** | **N** | N/A | Optional name of the field containing the read time of the batch. If this is not set, no time field will be added to output records. If set, this field must be present in the schema property and must be a long. |
| **Key Field** | **N** | N/A | Optional name of the field containing the message key. If this is not set, no key field will be added to output records. If set, this field must be present in the schema property and must be bytes. |
| **Partition Field** | **N** | N/A | Optional name of the field containing the partition the message was read from. If this is not set, no partition field will be added to output records. If set, this field must be present in the schema property and must be an int. |
| **Offset Field** | **N** | N/A | Optional name of the field containing the partition offset the message was read from. If this is not set, no offset field will be added to output records. If set, this field must be present in the schema property and must be a long. |
| **Format** | **N** | N/A | Optional format of the Kafka event message. Any format supported by CDAP is supported. For example, a value of 'csv' will attempt to parse Kafka payloads as comma-separated values. If no format is given, Kafka message payloads will be treated as bytes. |
| **Kerberos Principal** | **N** | N/A | The kerberos principal used for the source when kerberos security is enabled for kafka. |
| **Keytab Location** | **N** | N/A | The keytab location for the kerberos principal when kerberos security is enabled for kafka. |


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

Copyright © 2018 Cask Data, Inc.

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
