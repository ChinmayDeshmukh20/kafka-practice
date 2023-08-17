import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
       log.info("I am kafka producer!");

       //create producer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers" , "127.0.0.1:9092");

       // set producer properties
        properties.setProperty("key.serializer" , StringSerializer.class.getName());
        properties.setProperty("value.serializer" , StringSerializer.class.getName());
        properties.setProperty("batch.size","400");
        //properties.setProperty("partitioner.class" , RoundRobinPartitioner.class.getName());


       //create the producer
        KafkaProducer<String , String> producer = new KafkaProducer<>(properties);


        for(int j=0 ; j<10 ; j++)
        {

            for(int i=0 ; i<30 ; i++)
            {

                //create a Producer Record
                ProducerRecord<String , String> producerRecord = new ProducerRecord<>("demo_java" , "hello world" + i);

                //send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        //executes everytime a record is sent successfully or exception occurs
                        if(e==null)
                        {
                            //record was successfully sent
                            log.info("Received New metadata \n" +
                                    "Topic: " + metadata.topic() + "\n" +
                                    "Partition: " + metadata.partition() + "\n" +
                                    "Offset: " + metadata.offset() + "\n" +
                                    "Timestamp: " + metadata.timestamp() + "\n"
                            );
                        }else{
                            log.error("Error while producing" , e);
                        }

                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }


        //flush & close the producer
        producer.flush();
        producer.close();

    }

}
