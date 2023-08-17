import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());





    public static void main(String[] args) {
       log.info("I am kafka producer!");

       //create producer Properties
        Properties properties = new Properties();

        //connect to localhost
        properties.setProperty("bootstrap.servers" , "127.0.0.1:9092");

       // set producer properties
        properties.setProperty("key.serializer" , StringSerializer.class.getName());
        properties.setProperty("value.serializer" , StringSerializer.class.getName());

       //create the producer
        //KakfaProducer<key , value> producerName
        KafkaProducer<String , String> producer = new KafkaProducer<>(properties);

        //create a Producer Record - Record that we are gonna send to kafka
        ProducerRecord<String , String> producerRecord = new ProducerRecord<>("demo_java" , "hello world");

        //send data
        producer.send(producerRecord);

        //flush & close the producer
        producer.flush();
        producer.close();

    }




}
