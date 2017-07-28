# Kafka Batch Source


Description
-----------
Kafka batch source. Emits the record from kafka. It will emit a record based on the schema and format 
you use, or if no schema or format is specified, the message payload will be emitted. The source will 
remember the offset it read last run and continue from that offset for the next run.

Use Case
--------
This source is used whenever you want to read from Kafka. For example, you may want to read messages
from Kafka and write them to a Table.


Properties
----------
**referenceName:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**kafkaBrokers:** List of Kafka brokers specified in host1:port1,host2:port2 form. (Macro-enabled)

**topic:** The Kafka topic to read from. (Macro-enabled)

**tableName:** Optional table name to track the latest offsets read from Kafka. Stores offsets using a composite key 
of reference name, topic name and partition. Only change this table name if you would like to ignore the offsets read 
from a Kafka topic by a previous run using the Kafka Batch Source. (Macro-enabled)

**partitions:** List of topic partitions to read from. If not specified, all partitions will be read. (Macro-enabled)

**initialPartitionOffsets:** The initial offset for each topic partition. This offset will only be used for the 
first run of the pipeline. Any subsequent run will read from the latest offset from previous run. 
Offsets are inclusive. If an offset of 5 is used, the message at offset 5 will be read. (Macro-enabled)

**schema:** Output schema of the source. If you would like the output records to contain a field with the
Kafka message key, the schema must include a field of type bytes or nullable bytes, and you must set the
keyField property to that field's name. Similarly, if you would like the output records to contain a field with
the timestamp of when the record was read, the schema must include a field of type long or nullable long, and you
must set the timeField property to that field's name. Any field that is not the keyField, partitionField and keyField
 will be used in conjuction with the format to parse Kafka message payloads.

**format:** Optional format of the Kafka event message. Any format supported by CDAP is supported.
For example, a value of 'csv' will attempt to parse Kafka payloads as comma-separated values.
If no format is given, Kafka message payloads will be treated as bytes.

**keyField:** Optional name of the field containing the message key.
A default name of "key" is automatically added to the output schema.
If the user wishes the change the default name, the new name must be set and must replace the default name in the
output schema.

**partitionField:** Optional name of the field containing the partition the message was read from.
If this is not set, no partition field will be added to output records.
If set, this field must be present in the schema property and must be an int.

**offsetField:** Optional name of the field containing the partition offset the message was read from.
A default name of "offset" is automatically added to the output schema.
If the user wishes the change the default name, the new name must be set and must replace the default name in the
output schema.


Example
-------
This example reads from the 'purchases' topic of a Kafka instance running
on brokers host1.example.com:9092 and host2.example.com:9092. The source will add
a field named 'key' which will have the message key in it. It parses the Kafka messages 
using the 'csv' format with 'user', 'item', 'count', and 'price' as the message schema.

    {
        "name": "Kafka",
        "type": "streamingsource",
        "properties": {
            "topics": "purchases",
            "brokers": "host1.example.com:9092,host2.example.com:9092",
            "format": "csv",
            "keyField": "key",
            "schema": "{
                \"type\":\"record\",
                \"name\":\"purchase\",
                \"fields\":[
                    {\"name\":\"key\",\"type\":\"bytes\"},
                    {\"name\":\"user\",\"type\":\"string\"},
                    {\"name\":\"item\",\"type\":\"string\"},
                    {\"name\":\"count\",\"type\":\"int\"},
                    {\"name\":\"price\",\"type\":\"double\"}
                ]
            }"
        }
    }

For each Kafka message read, it will output a record with the schema:

    +================================+
    | field name  | type             |
    +================================+
    | key         | bytes            |
    | user        | string           |
    | item        | string           |
    | count       | int              |
    | price       | double           |
    +================================+
    