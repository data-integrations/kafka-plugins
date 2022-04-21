# Kafka Connection

Description
-----------
Use this connection to access data in Kafka.

Properties
----------
**Name:** Name of the connection. Connection names must be unique in a namespace.

**Description:** Description of the connection.

**kafkaBrokers:** List of Kafka brokers specified in host1:port1,host2:port2 form.

Path of the connection
----------------------
To browse, get a sample from, or get the specification for this connection through
[Pipeline Microservices](https://cdap.atlassian.net/wiki/spaces/DOCS/pages/975929350/Pipeline+Microservices), the `path`
property is required in the request body. It can be in the following form :

1. `/{topic}`
   This path indicates a topic. A topic is the only one that can be sampled. Browse on this path to return the specified topic.

2. `/`
   This path indicates the root. A root cannot be sampled. Browse on this path to get all the topics visible through this connection.
