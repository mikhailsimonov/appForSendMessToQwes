package com.simonov.utils;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

import static com.simonov.frames.MainFrame.group1;
import static com.simonov.frames.MainFrame.serv1;


/**
 * Created by Simonov-MS on 26.02.2018.
 * Этот инструмент забирает сообщения из очереди
 */
public class KConsumer{

    private static final Logger LOGGER = LoggerFactory.getLogger(KConsumer.class.getName()); //объявляем логер для логирования

    public static String topic = null;

    private final static String BOOTSTRAP_SERVERS
            = serv1;
    public static void setTopic(String topicName) {

        if (topicName.equals("toAI")) {

            topic = "toAI";
        }else if (topicName.equals("fromAI")) {

            topic = "fromAI";
        }
        else if (topicName.equals("AiTopic")) {

            topic = "AiTopic";
        }
        else if (topicName.equals("PaTopic")) {

            topic = "PaTopic";
        } else if (topicName.equals("sbermessTopic")) {

            topic = "sbermessTopic";
        } else {

            LOGGER.warn("topic not found!");

        }
    }
    private static Consumer<Long, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                group1);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        // Настройки консюмера
        final Consumer<Long, String> consumer
                = new org.apache.kafka.clients.consumer.KafkaConsumer<Long, String>(props);

        // Подсписаться на топик
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }
    public static String runConsumer() {
        final Consumer<Long, String> consumer = createConsumer();
        final int giveUp = 100;
        int noRecordsCount = 0;
        String resp = null;
        /**
         * надо будет реализовать если сообщение будет не одно List<String> qwe
         * = new ArrayList<>();
         */
        while (true) {
            final ConsumerRecords<Long, String> consumerRecords
                    = consumer.poll(5);
            if (consumerRecords.count() == 0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) {
                    break;
                } else {
                    continue;
                }
            }
            for (ConsumerRecord<Long, String> record : consumerRecords) {
                resp = record.value();
                //qwe.add(record.value());
            }
            consumer.commitAsync();
        }
        consumer.close();
        LOGGER.info("get message from queue of topic :" + topic + "\nmessage :" + resp);

        return resp;
    }
}
