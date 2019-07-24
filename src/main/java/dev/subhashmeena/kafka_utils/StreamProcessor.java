package dev.subhashmeena.kafka_utils;

import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;

public class StreamProcessor {
	public static void main(String... args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG,"dev.subhashmeena.kafka_utils.streamprocessor");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "two");
		//props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
		//props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,LongSerializer.class);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
		
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String,String> lines = builder.stream("topic-one",Consumed.with(Serdes.String(),Serdes.String()));
		lines.groupByKey().count().toStream().to("topic-four",Produced.with(Serdes.String(), Serdes.Long()));
//		lines.print(Printed.toSysOut());;
		
		KafkaStreams streams = new KafkaStreams(builder.build(),props);
		streams.start();
		
	}
}
