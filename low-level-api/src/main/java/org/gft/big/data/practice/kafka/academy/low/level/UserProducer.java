package org.gft.big.data.practice.kafka.academy.low.level;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spikhalskiy.futurity.Futurity;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.gft.big.data.practice.kafka.academy.model.User;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Go to the org.gft.big.data.practice.kafka.academy.low.level.UserProducer class and
 * implement the produceUsers method.
 *
 * To serialize each User to JSON (use the ObjectMapper to achieve this),
 * Send it to Kafka under the user's id and return a single CompletableFuture which finishes
 * if / when all the user records are sent to Kafka,
 *
 * Use the Futurity.shift method to transform a simple Java Future to a CompletableFuture
 * Once implemented, run the UserProducerTest to check the correctness of your implementation.
 */
public class UserProducer {

    private ObjectMapper objectMapper;

    public UserProducer(ObjectMapper objectMapper){
        this.objectMapper = objectMapper;
    }

    public CompletableFuture<?> produceUsers(String bootstrapServers, String topic, Collection<User> users){
        KafkaProducer<Long, String> producer = getKafkaProducer(bootstrapServers);
        CompletableFuture<?>[] futures = users.stream()
                .map(user -> packUserRecord(user, topic))
                .filter(Objects::nonNull)
                .map(producer::send)
                .map(Futurity::shift)
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures);
    }


    private ProducerRecord<Long, String> packUserRecord(User user, String topic) {
        try {
            String value = objectMapper.writeValueAsString(user);
            return new ProducerRecord<>(topic, user.getId(), value);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    private KafkaProducer<Long, String> getKafkaProducer(String bootStrapServers) {
        return new KafkaProducer<>(
                Map.of("bootstrap.servers", bootStrapServers),
                new LongSerializer(),
                new StringSerializer()
        );
    }
}
