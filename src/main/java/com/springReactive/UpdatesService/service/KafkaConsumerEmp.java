package com.springReactive.UpdatesService.service;

import com.springReactive.UpdatesService.model.EmployeeRequest;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerEmp {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    String groupId = "sample-group";
    String sourceTopic = "appUpdates";
    private final Logger log = LoggerFactory.getLogger(KafkaConsumerEmp.class);
    KafkaSender<Integer, EmployeeRequest> sender;
    @Bean
    public ReceiverOptions<Integer, EmployeeRequest> receiverOptions() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-consumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "groupId");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, EmployeeSerDes.class);
        return ReceiverOptions.<Integer, EmployeeRequest>create(props);
    }

    @Bean
    public ReceiverOptions<Integer, EmployeeRequest> receiverOptions(Collection<String> topics) {
        return receiverOptions()
                .addAssignListener(p -> log.info("Group {} partitions assigned {}", groupId, p))
                .addRevokeListener(p -> log.info("Group {} partitions revoked {}", groupId, p))
                .subscription(topics);
    }

    //    @Bean
//    Flux<ReceiverRecord<Integer, EmployeeRequest>> reactiveKafkaReceiver(ReceiverOptions<Integer, EmployeeRequest> kafkaReceiverOptions) {
//        return KafkaReceiver.create(receiverOptions().subscription(Collections.singleton(sourceTopic))).receive();
//    }
    @Bean
    public SenderOptions<Integer, EmployeeRequest> senderOptions() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "sample-producer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EmployeeSerDes.class);
        return SenderOptions.create(props);
    }

    @Bean
    public KafkaSender<Integer, EmployeeRequest> sender(SenderOptions<Integer, EmployeeRequest> senderOptions) {
        sender = KafkaSender.create(senderOptions);
        return sender;
    }
}
