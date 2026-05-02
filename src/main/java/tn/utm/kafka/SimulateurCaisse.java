package tn.utm.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.*;

public class SimulateurCaisse {
    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "all");
        props.put("enable.idempotence", "true");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        String[] villes = {"Tunis", "Sousse", "Sfax", "Bizerte", "Gabes"};
        Random rand = new Random();

        while (true) {
            String ville = villes[rand.nextInt(villes.length)];

            double p = rand.nextDouble();
            String type = (p < 0.7) ? "VENTE" : (p < 0.8) ? "RETOUR" : "OUVERTURE";

            double montant = (type.equals("OUVERTURE")) ? 0 : 5 + rand.nextDouble() * 495;

            String json = String.format(
                "{\"type\":\"%s\",\"ville\":\"%s\",\"montant\":%.2f}",
                type, ville, montant
            );

            ProducerRecord<String, String> record =
                new ProducerRecord<>("pos-events", ville, json);

            producer.send(record);

            Thread.sleep(100 + rand.nextInt(400));
        }
    }
}