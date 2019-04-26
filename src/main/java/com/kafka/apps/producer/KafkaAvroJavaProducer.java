package com.kafka.apps.producer;

import java.util.Properties;

import com.kafka.avro.Customer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class KafkaAvroJavaProducer {

   public static void main(String[] args) {
      Properties properties = new Properties();
      // normal producer
      properties.setProperty("bootstrap.servers","127.0.0.1:9092");
      properties.setProperty("acks", "all");
      properties.setProperty("retries", "10");
      // avro part
      properties.setProperty("key.serializer", StringSerializer.class.getName());
      properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
      properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");

      Producer<String, Customer> producer = new KafkaProducer<String, Customer>(properties);
      String topic = "customer-avro";

      long startTime= System.currentTimeMillis();

      for (long i = 0; i < 10; i++) {

         Customer customer = Customer.newBuilder()
                  .setFirstName("John" + i)
                  .setLastName("Doe")
                  .setAge(24)
                  .build();
         System.out.println(customer);
         ProducerRecord<String, Customer> producerRecord = new ProducerRecord<String, Customer>(
                  topic, customer.getFirstName(), customer
         );
         producer.send(producerRecord);
      }

      long endTime= System.currentTimeMillis();
      System.out.println("Total time taken " + (endTime-startTime)/1000d);

      producer.flush();
      producer.close();
   }
}
