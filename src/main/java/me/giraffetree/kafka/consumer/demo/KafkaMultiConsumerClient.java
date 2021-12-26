package me.giraffetree.kafka.consumer.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * 多消费者多线程消费客户端
 *
 * @author GiraffeTree
 * @date 2021-12-26
 */
@Slf4j
public class KafkaMultiConsumerClient<K, V> {

    /**
     * 消费者配置
     */
    private Map<String, Object> consumerConfig;
    /**
     * 反序列化器
     */
    private Deserializer<K> keyDeserializer;
    /**
     * 反序列化器
     */
    private Deserializer<V> valueDeserializer;
    /**
     * 需要启动几个消费者
     */
    private int consumerSize;
    /**
     * 是否使用订阅模式
     * useSubscribe = true -> 使用订阅模式 topicList
     * useSubscribe = false -> 指定分区消费
     */
    private boolean useSubscribe;
    /**
     * 需要订阅的 topic 列表
     */
    private List<String> topicList;
    /**
     * 手动指定消费的分区
     */
    private List<TopicPartition> assignPartitions;
    /**
     * 每个消费者对应几个线程进行消费
     */
    private int threadCountPerConsumer;
    /**
     * 唯一提交模式
     */
    private OffsetCommitMode offsetCommitMode;

    private Map<String, KafkaConsumer<K, V>> consumers;

    /**
     * 拉取线程
     */
    private ExecutorService mainPool;

    /**
     * 处理线程池
     */
    private Map<String, ExecutorService> processPoolMap;

    private Consumer<ConsumerRecord<K, V>> processor;

    private Map<String, Boolean > deadMap ;

    private final AtomicInteger counter = new AtomicInteger(0);

    public KafkaMultiConsumerClient(Map<String, Object> consumerConfig, int consumerSize, boolean useSubscribe, List<String> topicList, List<TopicPartition> assignPartitions, int threadCountPerConsumer, OffsetCommitMode offsetCommitMode, Consumer<ConsumerRecord<K, V>> processor) {
        this.consumerConfig = consumerConfig;
        this.consumerSize = consumerSize;
        this.useSubscribe = useSubscribe;
        this.topicList = topicList;
        this.assignPartitions = assignPartitions;
        this.threadCountPerConsumer = threadCountPerConsumer;
        this.offsetCommitMode = offsetCommitMode;
        this.consumers = new ConcurrentHashMap<>(consumerSize);
        this.mainPool = Executors.newFixedThreadPool(consumerSize);
        this.processPoolMap = new ConcurrentHashMap<>(consumerSize);
        this.processor = processor;
        this.deadMap = new HashMap<>(consumerSize);
    }

    /**
     * 启动消费
     */
    public void start() {
        for (int i = 0; i < consumerSize; i++) {
            final KafkaConsumer<K, V> kafkaConsumer = new KafkaConsumer<>(consumerConfig, keyDeserializer, valueDeserializer);
            if (useSubscribe) {
                kafkaConsumer.subscribe(topicList);
            }else {
                kafkaConsumer.assign(assignPartitions);
            }
            String consumerKey = getConsumerKey();
            if (threadCountPerConsumer == 1) {
                // 单消费者单线程消费
                consumers.put(consumerKey, kafkaConsumer);
                mainPool.execute(() -> process(kafkaConsumer, null));
            }else {
                // 多线程消费
                consumers.put(consumerKey, kafkaConsumer);
                ExecutorService processPool = Executors.newFixedThreadPool(threadCountPerConsumer);
                processPoolMap.put(consumerKey, processPool);
                mainPool.execute(() -> process(kafkaConsumer,processPool));
            }
        }
    }

    private void process(KafkaConsumer<K, V> kafkaConsumer, ExecutorService processPool) {

        try {
            while (true) {
                ConsumerRecords<K, V> records = kafkaConsumer.poll(Duration.ofSeconds(10));
                for (ConsumerRecord<K, V> record : records) {
                    if (processPool == null) {
                        processor.accept(record);
                    }else {
                        processPool.submit(() -> processor.accept(record));
                    }
                }
            }
        } catch (WakeupException wakeupException) {
            // todo: log and metric
//            deadMap.put()
        } catch (Exception e) {
            // todo:
            log.error("process error", e);
        } finally {
            kafkaConsumer.close();
        }
    }

    private String getConsumerKey() {
        int cur = counter.incrementAndGet();
        return "consumer-" + cur;
    }

    /**
     * 停止消费
     */
    public void stopConsume() {
        for (KafkaConsumer<K, V> consumer : consumers.values()) {
            consumer.wakeup();
        }
    }

    /**
     * 等待关闭
     *
     * @param mills 等待时间 单位: 毫秒
     */
    private void awaitTermination(long mills) {
        for (Map.Entry<String, ExecutorService> poolEntry : processPoolMap.entrySet()) {
            String consumerKey = poolEntry.getKey();
            KafkaConsumer<K, V> kafkaConsumer = consumers.get(consumerKey);

        }
    }


}
