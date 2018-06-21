package com.simonov.utils;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.logging.Logger;

/**
 * Created by Simonov-MS on 26.02.2018.
 * Этот инструмент отправляет сообщения в очередь
 */

public class KProducer {


    private static final Logger LOGGER = Logger.getLogger(KProducer.class.getName()); //объявляем логер для логирования

    public static Future<RecordMetadata> kafkaSend(String url, String topic, String message) {
        //settings
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, url);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //producer
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        //send message
        Future<RecordMetadata> response = producer.send(new ProducerRecord<String, String>(topic, message), new Callback() {

            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                LOGGER.info("check callback after send message in qwe");

                if (exception != null) { //проверяю что запрос успешно отправился в очередь
                    exception.printStackTrace();
                    LOGGER.info("Exception :");
                } else {
                    LOGGER.info("message flew into qwe successfully");
                }
            }
        });
        //end
        producer.close();
        return response;
    }
}
