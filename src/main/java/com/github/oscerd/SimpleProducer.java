package com.github.oscerd;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SimpleProducer {

    public static void main(String[] args) throws JsonProcessingException {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> prod = new KafkaProducer<String, String>(props);
        
        Author author1 = new Author();
        author1.setName("Andrea Cosentino");
        Author author2 = new Author();
        author2.setName("Jonathan Anstey");
        Author author3 = new Author();
        author3.setName("Claus Ibsen");
        Author author4 = new Author();
        author4.setName("Normam Maurer");
        Author author5 = new Author();
        author5.setName("Philip Roth");  
        
        ObjectMapper mapper = new ObjectMapper();

        prod.send(new ProducerRecord<String, String>("test", "1", mapper.writeValueAsString(author1)));
        prod.send(new ProducerRecord<String, String>("test", "2", mapper.writeValueAsString(author2)));
        prod.send(new ProducerRecord<String, String>("test", "3", mapper.writeValueAsString(author3)));
        prod.send(new ProducerRecord<String, String>("test", "4", mapper.writeValueAsString(author4)));
        prod.send(new ProducerRecord<String, String>("test", "5", mapper.writeValueAsString(author5)));

        prod.close();
    }
}
