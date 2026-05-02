package tn.utm.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.*;

public class ChiffreAffairesParVille {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "ca-1");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("pos-events"));

        Map<String, Double> ca = new HashMap<>();

        long lastPrint = System.currentTimeMillis();

        while (true) {
            ConsumerRecords<String, String> records =
                consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> rec : records) {
                String ville = rec.key();
                String value = rec.value();

                double montant = 0;
                if (value.contains("VENTE")) montant = extract(value);
                if (value.contains("RETOUR")) montant = -extract(value);

                ca.put(ville, ca.getOrDefault(ville, 0.0) + montant);
            }

            consumer.commitSync();

            if (System.currentTimeMillis() - lastPrint > 5000) {
                System.out.println("CA par ville: " + ca);
                lastPrint = System.currentTimeMillis();
            }
        }
    }

    static double extract(String json) {
        return Double.parseDouble(json.split("montant\":")[1].replace("}", ""));
    }
}