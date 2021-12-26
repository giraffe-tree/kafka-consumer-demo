package me.giraffetree.kafka.consumer.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.HashMap;

/**
 * @author GiraffeTree
 * @date 2021/12/26
 */
public class ClientTest {

    public static void main(String[] args) {
        KafkaMultiConsumerClient<String, String> client = getClient();
        client.start();

        try {
            System.out.println("main thread sleep 30s");
            Thread.sleep(30000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    private static KafkaMultiConsumerClient<String, String> getClient() {
        HashMap<String, Object> map = new HashMap<>();
        map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.31.242:9092");
        map.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        map.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        map.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "2000");
        map.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "8000");
        map.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");
        map.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10000");
        map.put(ConsumerConfig.GROUP_ID_CONFIG, "test.group.1");

        return new KafkaMultiConsumerClient<>(
                map,
                2,
                true, Collections.singletonList("test"), null,
                2,
                OffsetCommitMode.DEFAULT,
                (record) -> {
                    System.out.printf("consume - key:%s value:%s\n", record.key(), record.value());
                }
        );
    }
}
