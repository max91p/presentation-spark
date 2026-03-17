# ============================================================
# Étape 1 : Compilation du projet Scala avec SBT
# ============================================================
FROM sbtscala/scala-sbt:eclipse-temurin-jammy-17.0.10_7_1.9.9_2.12.18 AS builder

WORKDIR /app

# Copie des fichiers de build en premier (cache Docker)
COPY project/build.properties project/build.properties
COPY project/plugins.sbt project/plugins.sbt
COPY build.sbt build.sbt

# Télécharge les dépendances (cette couche est mise en cache)
RUN sbt update

# Copie le code source et compile le fat JAR
COPY src/ src/
RUN sbt assembly

# ============================================================
# Étape 2 : Image finale avec Spark
# ============================================================
FROM apache/spark:3.5.0

USER root

# Copie le fat JAR depuis l'étape de build
COPY --from=builder /app/target/scala-2.12/spark-demo-assembly-1.0.jar /opt/spark-app/spark-demo.jar

# Copie les données
COPY data/ /opt/spark-data/

USER spark
