# 🚀 Mini projet Spark — Présentation

Démonstration d'**Apache Spark** avec **Scala** et **Docker** : un cluster distribué (1 master + 2 workers) qui exécute des traitements de données avancés.

---

## 📖 Qu'est-ce que Spark ?

**Apache Spark** est un moteur de traitement distribué, rapide et polyvalent pour le Big Data.

### Architecture

```
┌─────────────────────────────────────────────┐
│              Driver Program                 │
│         (SparkSession / SparkContext)        │
│                     │                       │
│              Cluster Manager                │
│         (Spark Master - port 7077)          │
│              ┌──────┴──────┐                │
│         Worker 1       Worker 2             │
│        (Executor)     (Executor)            │
│       ┌────┴────┐   ┌────┴────┐            │
│    Task  Task  Task  Task  Task             │
└─────────────────────────────────────────────┘
```

### Concepts clés

| Concept | Description |
|---------|-------------|
| **RDD** | Collection distribuée et immuable d'objets, API de bas niveau |
| **DataFrame** | Tableau structuré avec colonnes nommées et types (comme SQL) |
| **Transformation** | Opération paresseuse (`map`, `filter`, `groupBy`...) |
| **Action** | Déclenche le calcul (`collect`, `count`, `show`...) |
| **SparkSession** | Point d'entrée unique pour les APIs Spark |

---

## 🏗️ Structure du projet

```
presentation spark/
├── build.sbt                     # Dépendances Scala + Spark
├── project/
│   ├── build.properties          # Version SBT
│   └── plugins.sbt               # Plugin sbt-assembly
├── docker-compose.yml            # Cluster : 1 master + 2 workers
├── Dockerfile                    # Build multi-étape (compilation + image Spark)
├── data/
│   ├── sales.csv                 # Données de démo (Ventes)
│   └── customers.csv             # Données de démo (Clients)
├── src/main/scala/
│   └── SparkDemo.scala           # Application principale avec 6 démos
└── README.md
```

---

## 🚀 Lancement

### 1. Construire et démarrer le cluster

```bash
docker compose build    # Compile le projet Scala et crée les images
docker compose up -d    # Démarre le cluster (master + 2 workers)
```

### 2. Vérifier le cluster

Ouvrir le **Spark Web UI** : [http://localhost:8080](http://localhost:8080)

→ Vous devriez voir **2 workers** connectés au master.

### 3. Lancer l'application

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --class SparkDemo \
  /opt/spark-app/spark-demo.jar
```

### 4. Arrêter le cluster

```bash
docker compose down
```

---

## 🎯 Contenu des 6 Démos

Le fichier `SparkDemo.scala` enchaîne 6 démonstrations pour illustrer toute la puissance de Spark :

### Demo 1 — Word Count (API RDD)
- **Objectif :** Montrer l'API de base (`flatMap`, `map`, `reduceByKey`).
- **Intérêt :** Spark distribue ces opérations sur les workers automatiquement.

### Demo 2 — Analyse de ventes (API DataFrame)
- **Objectif :** Lire un fichier CSV et calculer le chiffre d'affaires par produit.
- **Intérêt :** API de haut niveau, optimisée par le *Catalyst Optimizer* de Spark.

### Demo 3 — Requête SQL directe
- **Objectif :** Utiliser Spark SQL pour interroger une vue temporaire.
- **Intérêt :** Spark permet d'écrire du SQL classique sur des données distribuées.

### Demo 4 — Cache / Persist (Avantage #1 de Spark) ⚡
- **Objectif :** Démontrer le traitement **en mémoire** (In-Memory Computing).
- **Intérêt :** On exécute de lourds calculs sans cache, puis avec `.cache()`. La deuxième exécution est quasiment instantanée, montrant pourquoi Spark est beaucoup plus rapide qu'Hadoop MapReduce.

### Demo 5 — Jointure entre Datasets (Ventes + Clients)
- **Objectif :** Combiner `sales.csv` et `customers.csv` sur `customer_id`.
- **Intérêt :** Montrer comment Spark fusionne des données issues de plusieurs sources de manière distribuée pour faire de l'analyse croisée (ex: CA par ville ou catégorie de client).

### Demo 6 — Spark Structured Streaming (Temps Réel) 📡
- **Objectif :** Intercepter des données en temps réel. Spark écoute un dossier et traite les fichiers au fur et à mesure de leur arrivée.
- **Intérêt :** Le script écrit de nouveaux fichiers toutes les 5 secondes et Spark met à jour le chiffre d'affaires dynamiquement à l'écran.

---

## 🔧 Technologies utilisées

| Outil | Version | Rôle |
|-------|---------|------|
| Apache Spark | 3.5.0 | Moteur de traitement distribué |
| Scala | 2.12.18 | Langage de programmation |
| SBT | 1.9.9 | Build tool Scala |
| Docker + Compose | — | Orchestration du cluster |
