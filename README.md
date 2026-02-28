# Weather Data Streaming — Real-Time Pipeline

Projet de data streaming en temps réel : collecte de données météo, traitement avec Spark, détection d'anomalies ML, et visualisation dynamique.

## Architecture

```
OpenWeatherMap API (10 villes européennes)
        │
        ▼ (toutes les 60s)
  Kafka Producer (Python)
        │
        ▼
  Apache Kafka (Topic: weather-data)
        │
        ▼
  Spark Structured Streaming
  ├── Data Cleaning
  ├── Windowed Aggregations (fenêtres 5 min)
  └── ML: Isolation Forest (anomaly detection)
        │
        ▼
  Fichiers JSON (volume partagé)
        │
        ▼
  Streamlit Dashboard (auto-refresh 30s)
```

## Stack Technique

| Composant | Technologie |
|-----------|-------------|
| Message Broker | Apache Kafka 7.4 |
| Stream Processing | Apache Spark 3.4 (Structured Streaming) |
| ML | scikit-learn (Isolation Forest) |
| Visualisation | Streamlit + Plotly |
| Orchestration | Docker Compose |
| Source de données | OpenWeatherMap API |

## Données collectées

Pour **10 villes européennes** (Paris, London, Berlin, Madrid, Rome, Amsterdam, Brussels, Vienna, Zurich, Stockholm) :
- Température (°C), ressenti, min/max
- Humidité (%), Pression (hPa)
- Vitesse du vent (m/s), direction
- Couverture nuageuse (%), visibilité
- Description météo

## Machine Learning

**Isolation Forest** pour la détection d'anomalies météorologiques :
- Features : température, humidité, pression, vitesse du vent, nuages
- Contamination : 5% (seuil d'anomalie)
- Modèle mis à jour à chaque batch de données
- Résultats affichés en temps réel dans le dashboard

## Prérequis

- Docker & Docker Compose
- Clé API OpenWeatherMap (gratuite sur [openweathermap.org](https://openweathermap.org/api))

## Installation & Exécution

### 1. Cloner le repo

```bash
git clone https://github.com/keita223/projet_data_streaming.git
cd projet_data_streaming
```

### 2. Configurer l'API Key

```bash
cp .env.example .env
# Éditer .env et remplacer 'your_openweathermap_api_key_here' par ta clé OWM
```

### 3. Lancer l'application

```bash
docker-compose up --build -d
```

### 4. Accéder aux interfaces

| Service | URL |
|---------|-----|
| Streamlit Dashboard | http://localhost:8501 |
| Kafka UI | http://localhost:8080 |

### 5. Arrêter

```bash
docker-compose down
```

## Structure du projet

```
projet_data_streaming/
├── docker-compose.yml        # Orchestration des services
├── .env.example              # Template de configuration
├── README.md
├── producer/
│   ├── Dockerfile
│   ├── producer.py           # Collecte OWM → Kafka
│   └── requirements.txt
├── consumer/
│   ├── Dockerfile
│   ├── spark_consumer.py     # Spark Structured Streaming
│   ├── anomaly_detector.py   # Isolation Forest ML
│   └── requirements.txt
├── dashboard/
│   ├── Dockerfile
│   ├── app.py                # Streamlit dashboard
│   └── requirements.txt
└── data/                     # Données générées (gitignored)
```

## Visualisations

- **Météo actuelle** : cartes par ville avec température et conditions
- **Séries temporelles** : évolution de la température par ville
- **Heatmap** : comparaison multi-villes multi-métriques
- **Agrégations fenêtrées** : moyennes sur fenêtres de 5 minutes
- **Anomalies** : alertes en temps réel avec score d'anomalie
- **Vents** : rose des vents interactive

## Auteur

Projet réalisé dans le cadre du M2 IASD — Data Streaming (2025-2026).
