# Mini-projet Kafka - Pipeline de logs temps réel

#Lien rapport (Google Drive) :
https://drive.google.com/drive/folders/15fvWdc1XcAo9cvOJn95Hi7qP0lWOVrCI?usp=sharing

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


