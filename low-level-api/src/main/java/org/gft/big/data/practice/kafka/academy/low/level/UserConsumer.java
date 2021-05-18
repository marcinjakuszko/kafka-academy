package org.gft.big.data.practice.kafka.academy.low.level;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.gft.big.data.practice.kafka.academy.model.User;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Go to the org.gft.big.data.practice.kafka.academy.low.level.UserConsumer class and implement
 * the consumeUsers method so that it continuously reads the records from a given Kafka topic
 * until the maxRecords record count or the timeout is reached and then deserializes each value
 * to a User class from JSON (use the ObjectMapper) to eventually return the retrieved users.
 *
 * One implemented, run the UserConsumerTest to check the correctness of your implementation
 */
public class UserConsumer {

    private ObjectMapper mapper;

    private Duration pollingTimeout;

    private Clock clock;

    public UserConsumer(ObjectMapper mapper, Duration pollingTimeout) {
        this.mapper = mapper;
        this.pollingTimeout = pollingTimeout;
        this.clock = Clock.systemUTC();
    }

    public List<User> consumeUsers(String bootstrapServers, String topic, String groupId, Duration timeout, long maxRecords) {
        List<ConsumerRecord<Long, String>> resultRecords = new ArrayList<>();
        Map<String, Object> configMap = Map.of(
                "bootstrap.servers", bootstrapServers,
                "group.id", groupId,
                "auto.offset.reset", "earliest" // start reading from the beginng
        );

        KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(configMap, new LongDeserializer(), new StringDeserializer());
        consumer.subscribe(List.of(topic));

        Boolean keepConsuming = true;
        Instant startTime = Instant.now();
        while (keepConsuming) {
            for(ConsumerRecord<Long, String> record : consumer.poll(pollingTimeout)) {
                resultRecords.add(record);
            }
            if (resultRecords.size() >= maxRecords || Instant.now().minus(timeout).isAfter(startTime)) {
                keepConsuming = false;
            }
        }

        return resultRecords.stream()
                .map(this::unpackRecord)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private User unpackRecord(ConsumerRecord<Long, String> record) {
        try {
            return mapper.readValue(record.value(), User.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
