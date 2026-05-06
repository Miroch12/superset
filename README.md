📌 Objectif

Ce projet utilise Apache Airflow.
L’environnement virtuel airflow_venv n’est pas versionné (normal), donc il doit être recréé localement.

🚀 1. Créer l’environnement virtuel

Dans le dossier du projet :

python3 -m venv airflow_venv
▶️ 2. Activer l’environnement
Linux / WSL / Mac :
source airflow_venv/bin/activate
📦 3. Installer les dépendances

Si tu as un fichier requirements.txt :

pip install -r requirements.txt
⚠️ Si requirements.txt n’existe pas

Installe Airflow manuellement :

pip install apache-airflow

Ou version spécifique (recommandé) :

pip install "apache-airflow==2.8.1"
🛠️ 4. Initialiser Airflow
airflow db init
👤 5. Créer un utilisateur admin
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
▶️ 6. Lancer Airflow

Terminal 1 (webserver) :

airflow webserver --port 8080

Terminal 2 (scheduler) :

airflow scheduler
🌐 7. Accéder à l’interface

Ouvre :

http://localhost:8080
