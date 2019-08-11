package dev.subhashmeena.kafka_utils;



import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class App 
{
    public static void main( String[] args )
    {
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "dev.subhashmeena.kafka_util.producer");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        
        
        KafkaProducer producer = new KafkaProducer(props);
        
        producer.send(new ProducerRecord<String,String>("topic-one","key2","world"));
        producer.close();
    }
}
