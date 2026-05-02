package tn.utm.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import java.time.Duration;
import java.util.*;

public class DetecteurAnomalies {
    public static void main(String[] args) {

        Properties cprops = new Properties();
        cprops.put("bootstrap.servers", "localhost:9092");
        cprops.put("group.id", "alerte-1");
        cprops.put("key.deserializer", StringDeserializer.class.getName());
        cprops.put("value.deserializer", StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cprops);
        consumer.subscribe(Arrays.asList("pos-events"));

        Properties pprops = new Properties();
        pprops.put("bootstrap.servers", "localhost:9092");
        pprops.put("key.serializer", StringSerializer.class.getName());
        pprops.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(pprops);

        while (true) {
            ConsumerRecords<String, String> records =
                consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> rec : records) {
                String value = rec.value();

                if (value.contains("RETOUR")) {
                    double montant = Double.parseDouble(
                        value.split("montant\":")[1].replace("}", "")
                    );

                    if (montant > 200) {
                        producer.send(new ProducerRecord<>("alertes-retours", value));
                        System.out.println("ALERTE: " + value);
                    }
                }
            }
        }
    }
}