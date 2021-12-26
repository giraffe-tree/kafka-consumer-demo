package me.giraffetree.kafka.consumer.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.concurrent.Future;

/**
 * @author GiraffeTree
 * @date 2021/12/26
 */
public class ProducerTest {

    public static void main(String[] args) {

        HashMap<String, Object> configMap = new HashMap<>();
        configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.31.242:9092");
        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(
                configMap
        );
        int num = 1;
        while (true) {
            final String key = "" + num++;
            final String value = "test:" + key;

            producer.send(new ProducerRecord<>("test", key, value), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.printf("%s send success - key:%s value:%s\n", LocalDateTime.now(), key, value);
                }
            });
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }


}
