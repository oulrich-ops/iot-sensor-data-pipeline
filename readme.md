Pour la construction des services veuiller lire les notes :

- La specification de kafka   https://developer.confluent.io/confluent-tutorials/kafka-on-docker/ 

## Exemple données à envoyer par nos capteurs 

- **sensor_id** *(string)*  
  Identifiant unique du capteur.

- **sensor_type** *(string)*  
  Type de capteur (ex : temperature).

- **location** *(object)*  
  - **building** *(string)* : Nom du bâtiment.  
  - **floor** *(number)* : Numéro d’étage.  
  - **room** *(string)* : Identifiant de la salle.

- **timestamp** *(string, ISO 8601)*  
  Date et heure de la mesure.

- **value** *(number)*  
  Valeur mesurée par le capteur.

- **unit** *(string)*  
  Unité de mesure (ex : celsius).

- **metadata** *(object)*  
  - **battery_level** *(number)* : Niveau de batterie (%).  
  - **signal_strength** *(number)* : Force du signal (dBm).
