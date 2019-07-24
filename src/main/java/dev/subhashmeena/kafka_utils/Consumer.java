package dev.subhashmeena.kafka_utils;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import java.util.Iterator;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;



public class Consumer {
	
	public static void main(String args[]) {
		
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,LongDeserializer.class);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.GROUP_ID_CONFIG,"dev.subhashmeena.kafka_utils.consumer");
		
		KafkaConsumer<String,Long> consumer = new KafkaConsumer<String,Long>(props);
		
		//consumer.assign(Arrays.asList("topic-one"));
		//consumer.subscribe(Arrays.asList("topic-one"));
		consumer.assign(Arrays.asList(new TopicPartition("topic-four",0)));
		consumer.seekToBeginning(Arrays.asList(new TopicPartition("topic-four",0)));
		ConsumerRecords<String,Long> records = consumer.poll(Duration.ofMillis(1000));
		//System.out.println(records.count());
		for(ConsumerRecord<String,Long> record: records) {
			//System.out.println("Reaching in the for loop");
			System.out.println(record.key()+" -> "+record.value());
		}
	}

}
