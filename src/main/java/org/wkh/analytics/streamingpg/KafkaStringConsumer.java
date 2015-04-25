package org.wkh.analytics.streamingpg;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaStringConsumer {
    public static void main(String[] args) {
        final String topic = "vanilla-kafka-test";

        Properties props = new Properties();
        props.put("zookeeper.connect", "localhost:2181");
        props.put("zookeeper.connectiontimeout.ms", "1000000");
        props.put("group.id", "kafka-avro-console-consumer");
        props.put("auto.offset.reset", "smallest"); // start from beginning

        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        System.out.println("About to consume messages");

        for(final KafkaStream<byte[], byte[]> stream: streams) {
            for(MessageAndMetadata msgAndMetadata: stream) {
                byte[] message = (byte[])msgAndMetadata.message();
                String messageStr = new String(message);
                System.out.println(messageStr);
            }
        }

        System.out.println("Shutting down");
        consumerConnector.shutdown();
    }
}
