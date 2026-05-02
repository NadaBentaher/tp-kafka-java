# Mini-projet Kafka - Pipeline de logs temps réel

## Description
Ce projet simule plusieurs caisses POS qui envoient des événements en temps réel vers Apache Kafka.

## Architecture
- Producer : SimulateurCaisse
- Consumer 1 : ChiffreAffairesParVille
- Consumer 2 : DetecteurAnomalies

## Topics
- pos-events
- alertes-retours

## Lancement

### Compiler
mvn clean compile

### Lancer le simulateur
mvn exec:java "-Dexec.mainClass=tn.utm.kafka.SimulateurCaisse"

### Lancer le calcul CA
mvn exec:java "-Dexec.mainClass=tn.utm.kafka.ChiffreAffairesParVille"

### Lancer le détecteur
mvn exec:java "-Dexec.mainClass=tn.utm.kafka.DetecteurAnomalies"
