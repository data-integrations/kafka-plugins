Kafka Plugins
=============

<a href="https://cdap-users.herokuapp.com/"><img alt="Join CDAP community" src="https://cdap-users.herokuapp.com/badge.svg?t=kafka-plugins"/></a> [![Build Status](https://travis-ci.org/hydrator/kafka-plugins.svg?branch=master)](https://travis-ci.org/hydrator/kafka-plugins) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) <img alt="CDAP Realtime Source" src="https://cdap-users.herokuapp.com/assets/cdap-realtime-source.svg"/> []() <img alt="CDAP Realtime Sink" src="https://cdap-users.herokuapp.com/assets/cdap-realtime-sink.svg"/> []() <img src="https://cdap-users.herokuapp.com/assets/cm-available.svg"/>

Kafka source and sink to read/write events to/from kafka. The plugins are broken down to two modules to support Kafka 0.8, 0.9 and 0.10.
The kafka batch source is based on Apache Gobblin.

* [Kafka Streaming Source for Kafka 10 and Onward](kafka-plugins-client/docs/KAFKASOURCE.md)
* [Kafka Batch Source for Kafka 10 and Onward](kafka-plugins-client/docs/KAFKABATCHSOURCE.md)
* [Kafka Batch Sink for Kafka 10 and Onward](kafka-plugins-client/docs/KAFKAWRITER-SINK.md)
* [Kafka Alert Publisher for Kafka 10 and Onward](kafka-plugins-client/docs/Kafka-alert-publisher.md)

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

    > load artifact <kafka-plugins-client/target/kafka-plugins-client-<version>.jar config-file kafka-plugins-client/target/kafka-plugins-client-<version>.json
    
## Mailing Lists

CDAP User Group and Development Discussions:

* `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from 
users, release announcements, and any other discussions that we think will be helpful 
to the users.

## IRC Channel

CDAP IRC Channel: #cdap on irc.freenode.net


## License and Trademarks

Copyright © 2017-2022 Cask Data, Inc.

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
