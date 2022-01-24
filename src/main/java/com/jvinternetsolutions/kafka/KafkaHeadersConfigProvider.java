package com.jvinternetsolutions.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigData;
import org.apache.kafka.common.config.provider.ConfigProvider;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class KafkaHeadersConfigProvider implements ConfigProvider {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaHeadersConfigProvider.class);

    private KafkaConsumer<Object, Object> consumer;

    @Override
    public void close() throws IOException {
        LOG.info("Closing Kafka config provider");

        consumer.close();
    }

    @Override
    public void configure(Map<String, ?> map) {
        LOG.info("Configuring Kafka config provider for topic: {}", map);

        final Properties props = System.getProperties();

        if (!map.isEmpty()) {
            props.putAll(map);
        }

        consumer = new KafkaConsumer<>(props);
    }

    @Override
    public ConfigData get(String path) {
        return getValues(path, null);
    }

    @Override
    public ConfigData get(String path, Set<String> keys) {
        return getValues(path, keys);
    }

    /**
     * Gets the values from the Kubernetes resource.
     *
     * @param path Path to the Kubernetes resource
     * @param keys Keys which should be extracted from the resource
     * @return Kafka ConfigData with the configuration
     */
    private ConfigData getValues(String path, Set<String> keys) {
        final Headers headers = getLatestHeaders(path);

        Map<String, String> configs = new HashMap<>(0);

        if (keys == null) {
            headers.forEach(header -> {
                configs.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
            });
        } else {
            headers.forEach(header -> {
                if (keys.contains(header.key())) {
                    configs.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
                }
            });
        }

        return new ConfigData(configs);
    }

    private Headers getLatestHeaders(String topicName) {
        if (consumer.subscription().contains(topicName)) {
            consumer.paused().forEach(topicPartition -> {
                if (topicPartition.topic().equals(topicName)) {
                    consumer.resume(Collections.singletonList(topicPartition));
                }
            });
        } else {
            consumer.subscribe(Collections.singletonList(topicName));
        }

        AtomicLong maxTimestamp = new AtomicLong();
        AtomicReference<ConsumerRecord<Object, Object>> latestRecord = new AtomicReference<>();

        // get the last offsets for each partition
        final Collection<TopicPartition> assignment = consumer.assignment()
                .stream()
                .filter(topicPartition -> topicPartition.topic().equals(topicName))
                .collect(Collectors.toList());

        consumer.endOffsets(assignment).forEach((topicPartition, offset) -> {
            // seek to the last offset of each partition
            consumer.seek(topicPartition, (offset == 0) ? offset : offset - 1);

            // poll to get the last record in each partition
            consumer.poll(Duration.ofSeconds(5)).forEach(record -> {
                // the latest record in the 'topic' is the one with the highest timestamp
                if (record.timestamp() > maxTimestamp.get()) {
                    maxTimestamp.set(record.timestamp());
                    latestRecord.set(record);
                }
            });
        });

        // pause the consumer
        consumer.pause(assignment);

        final ConsumerRecord<Object, Object> record = latestRecord.get();

        if (record == null) {
            return new RecordHeaders();
        }

        return record.headers();
    }
}
