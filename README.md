# Kafka Gateway API

This API exposes simple Gateway objects from a running [Kafka](http://kafka.apache.org/)/[Zookeeper](https://zookeeper.apache.org/) instance. We easily get information about Brokers, Consumers, Topics, etc.

## Quick Examples

Get some information for the first broker:

``` Java
// Create a KafkaGatewayApi instance with the address of the zookeeper instance
KafkaGatewayApi kafkaGatewayApi = new KafkaGatewayApi("localhost:2181");

// Get every brokers
List<BrokerGateway> brokers = kafkaGatewayApi.getBrokers();

// Print information about the first one
BrokerGateway broker = brokers.get(0);
System.out.println("Broker 1 : host : " + broker.host + ", id: " + broker.id + ", port: " + broker.port);
```

Print every 5 seconds information about the first consumer first topic (Log size, offset, lag):

``` Java
while(true) {
    // Create a KafkaGatewayApi instance with the address of the zookeeper instance
    KafkaGatewayApi kafkaGatewayApi = new KafkaGatewayApi("localhost:2181");

    // Get the list of the consumers
    List<ConsumerGateway> consumers = kafkaGatewayApi.getConsumers();

    // Get the first topic of the first consumer (careful, must exisits!)
    ConsumerTopicGateway consumerTopic = consumers.get(0).consumerTopicsGateway.get(0);

    System.out.println("LogSize : " + consumerTopic.logSize);
    System.out.println("Lag : " + consumerTopic.lag);
    System.out.println("Offset : " + consumerTopic.offset);
    System.out.println("-----------------------------------------------");

    // Wait 5 seconds
    Thread.sleep(5000);
}
```

An easy way to launch a simple instance of Kafka and Zookeeper with Docker using [the spotify/kafka container](https://github.com/spotify/docker-kafka):

``` Shell
docker run -p 2181:2181 -p 9092:9092 spotify/kafka
```

It will map the zookeeper port (2181) and the kafka port (9092) on your machine.

## Available methods

Available methods with examples:

``` Java
// Create a KafkaGatewayApi instance with the address of the zookeeper instance
KafkaGatewayApi kafkaGatewayApi = new KafkaGatewayApi("localhost:2181");

// Get the list of brokers
List<BrokerGateway> brokers = kafkaGatewayApi.getBrokers();

// Get the list of topics
List<TopicGateway> topics = kafkaGatewayApi.getTopics();

// Get the list of consumers
List<ConsumerGateway> consumers = kafkaGatewayApi.getConsumers();

// Get the first topic of the first consumer (careful, must exisits!)
ConsumerTopicGateway consumerTopic = consumers.get(0).consumerTopicsGateway.get(0);
```

## Gateway Objects

As [Martin Fowler defines it](http://martinfowler.com/eaaCatalog/gateway.html), a Gateway object is an object that encapsulates access to an external system or resource. The API exposes the following objects:

### BrokerGateway

Information about a Kafka broker

* host
* id
* port

### ConsumerGateway

Information about a Kafka consumer

* name
* consumerTopicsGateway: The List of topics that the consumer consumes

### ConsumerTopicGateway

Information about a Topic consumed by a Consumer

* name
* offset: Current cursor of the consumer
* logSize: Total number of messages of the topic
* lag: Number of messages that must be consumed (logSize - offset)

### TopicGateway

Information about a Topic (regardless of a certain consumer)

* name
* partitionIds
* leaderId: Id of the leader broker

## Contributing

If you think more Gateway objects and properties could be useful, don't hesitate to contact me. Pull requests are welcome. This repository uses Gradle for building. Tests are written in Groovy/[Spock](https://github.com/spockframework/spock).

Launch the tests:

``` Shell
gradle test
```

## TODO

* The code here only gets the first partition
* Publish to Maven

## License

The MIT License (MIT)

Copyright (c) 2015 Emmanuel Payet

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
