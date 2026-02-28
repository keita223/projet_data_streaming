# Weather Data Streaming — Real-Time Pipeline

> Projet M2 IASD — Data Streaming 2025-2026
> Collecte, traitement, ML et visualisation de données météo en temps réel.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                      OpenWeatherMap API                          │
│         10 villes européennes, polling toutes les 60 secondes    │
└──────────────────────────┬───────────────────────────────────────┘
                           │ HTTP REST
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│                    Kafka Producer (Python)                        │
│   • Sérialisation JSON                                           │
│   • Clé = nom de ville (partitionnement)                         │
│   • Retry / reconnexion automatique                              │
└──────────────────────────┬───────────────────────────────────────┘
                           │ kafka-python
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│          Apache Kafka   (topic: weather-data)                    │
│   • Confluent 7.4 · 1 broker · Zookeeper                        │
│   • Kafka UI disponible sur http://localhost:8080                │
└──────────────────────────┬───────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│          Spark Structured Streaming  (PySpark 3.4)               │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  Data Cleaning                                          │    │
│  │  • Filtre valeurs physiquement impossibles              │    │
│  │  • temp ∈ [-60, 60]°C  · humidity ∈ [0, 100]%          │    │
│  │  • pressure ∈ [870, 1085] hPa  · wind_speed ≥ 0        │    │
│  └──────────────┬──────────────────────────────────────────┘    │
│                 │                                                │
│      ┌──────────┴──────────┐                                    │
│      ▼                     ▼                                    │
│  Windowed Aggregations  foreachBatch                            │
│  (5 min tumbling)       (raw records + ML)                      │
│  avg/min/max temp,          │                                   │
│  humidity, pressure,        ▼                                   │
│  wind, stddev          Isolation Forest                         │
│      │                 anomaly detection                        │
│      ▼                      │                                   │
│  data/aggregations/    data/anomalies.json                      │
│  data/weather_records.json                                      │
└──────────────────────────┬───────────────────────────────────────┘
                           │ Shared Docker volume
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│                    Streamlit Dashboard                           │
│                 http://localhost:8501                            │
│                                                                  │
│  • KPI cards — conditions actuelles par ville                   │
│  • Série temporelle — température (10 villes)                   │
│  • Dual-axis — humidité + pression                              │
│  • Bar chart — vitesse du vent                                  │
│  • Heatmap — comparaison multi-villes                           │
│  • Agrégations fenêtrées — avg/min/max par fenêtre 5 min        │
│  • Panel ML — anomalies, scores, taux d'anomalie                │
│  • Auto-refresh toutes les 30 secondes                          │
└──────────────────────────────────────────────────────────────────┘
```

## Stack Technique

| Composant | Technologie | Version |
|-----------|-------------|---------|
| Message Broker | Apache Kafka (Confluent) | 7.4.0 |
| Coordination | Zookeeper | 7.4.0 |
| Stream Processing | Apache Spark Structured Streaming | 3.4.1 |
| Machine Learning | scikit-learn — Isolation Forest | 1.3.2 |
| Visualisation | Streamlit + Plotly | 1.29 / 5.18 |
| Orchestration | Docker Compose | v3.8 |
| Source de données | OpenWeatherMap API (Current Weather) | v2.5 |
| Langage | Python | 3.11 |

## Données collectées

Pour **10 villes européennes** (Paris, London, Berlin, Madrid, Rome, Amsterdam, Brussels, Vienna, Zurich, Stockholm) :

| Champ | Description |
|-------|-------------|
| `temperature` | Température actuelle (°C) |
| `feels_like` | Ressenti thermique (°C) |
| `temp_min/max` | Min/Max de la journée (°C) |
| `humidity` | Humidité relative (%) |
| `pressure` | Pression atmosphérique (hPa) |
| `wind_speed` | Vitesse du vent (m/s) |
| `wind_deg` | Direction du vent (degrés) |
| `wind_gust` | Rafales (m/s) |
| `clouds` | Couverture nuageuse (%) |
| `visibility` | Visibilité (mètres) |
| `weather_main` | Condition principale (Clear, Rain, …) |
| `weather_description` | Description détaillée |

## Machine Learning — Isolation Forest

**Algorithme** : `sklearn.ensemble.IsolationForest`

**Principe** : L'Isolation Forest isole les anomalies en construisant des arbres aléatoires. Les points faciles à isoler (peu de coupures nécessaires) sont des anomalies.

**Features** : `temperature`, `humidity`, `pressure`, `wind_speed`, `clouds`

**Paramètres** :
- `n_estimators = 100` — nombre d'arbres
- `contamination = 0.05` — seuil d'anomalie (5%)
- `StandardScaler` appliqué avant l'entraînement

**Stratégie online** : Le modèle est sauvegardé sur disque (`iso_forest.pkl`) et réentraîné à chaque micro-batch. Il s'adapte ainsi aux variations saisonnières au fil du temps.

**Output par enregistrement anomalique** :
- `anomaly_label` : -1 (anomalie) / 1 (normal)
- `anomaly_score` : score brut IF (plus bas = plus anormal)
- `is_anomaly` : booléen

## Prérequis

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) ≥ 24.0
- Clé API OpenWeatherMap gratuite → [openweathermap.org/api](https://openweathermap.org/api)

## Installation & Exécution

### 1. Cloner le repo

```bash
git clone https://github.com/keita223/projet_data_streaming.git
cd projet_data_streaming
```

### 2. Configurer la clé API

```bash
cp .env.example .env
# Ouvrir .env et remplacer 'your_openweathermap_api_key_here' par ta clé OWM
```

### 3. Lancer le pipeline complet

```bash
docker-compose up --build -d
```

Le démarrage prend ~2 minutes (téléchargement des images, démarrage Kafka).

### 4. Vérifier que tout tourne

```bash
docker-compose ps
# Tous les services doivent être "Up"
```

### 5. Accéder aux interfaces

| Interface | URL | Description |
|-----------|-----|-------------|
| **Dashboard** | http://localhost:8501 | Visualisation Streamlit |
| **Kafka UI** | http://localhost:8080 | Monitoring Kafka |

> Les premières données apparaissent après ~60 secondes (premier polling OWM).
> Les agrégations Spark sont disponibles après ~5 minutes.

### 6. Arrêter

```bash
docker-compose down
# Pour supprimer aussi les volumes :
docker-compose down -v
```

## Structure du projet

```
projet_data_streaming/
├── docker-compose.yml          # Orchestration (Zookeeper, Kafka, Producer, Consumer, Dashboard)
├── .env.example                # Template de configuration
├── .gitignore
├── README.md
│
├── producer/
│   ├── Dockerfile
│   ├── producer.py             # Collecte OWM → publication Kafka
│   └── requirements.txt
│
├── consumer/
│   ├── Dockerfile
│   ├── spark_consumer.py       # Spark Structured Streaming (cleaning + windowing)
│   ├── anomaly_detector.py     # Isolation Forest ML
│   └── requirements.txt
│
├── dashboard/
│   ├── Dockerfile
│   ├── app.py                  # Streamlit dashboard (6 types de graphiques)
│   └── requirements.txt
│
└── data/                       # Généré au runtime (gitignored)
    ├── weather_records.json    # Données brutes nettoyées
    ├── anomalies.json          # Résultats ML
    ├── aggregations/           # Agrégations Spark (JSON)
    ├── checkpoints/            # Checkpoints Spark
    ├── iso_forest.pkl          # Modèle persisté
    └── scaler.pkl              # Scaler persisté
```

## Logs en temps réel

```bash
# Voir le producer (données collectées)
docker logs -f weather_producer

# Voir le consumer Spark
docker logs -f spark_consumer

# Voir le dashboard
docker logs -f streamlit_dashboard
```
