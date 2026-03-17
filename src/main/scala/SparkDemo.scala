import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.storage.StorageLevel

object SparkDemo {

  def main(args: Array[String]): Unit = {

    // ============================================================
    // Création de la SparkSession (point d'entrée unique de Spark)
    // ============================================================
    val spark = SparkSession.builder()
      .appName("Spark Demo - Présentation")
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    println("\n" + "=" * 60)
    println("   DEMO SPARK - Présentation de l'outil")
    println("=" * 60)

    // ============================================================
    // DEMO 1 : Word Count avec l'API RDD
    // ============================================================
    // L'API RDD (Resilient Distributed Dataset) est l'API de base
    // de Spark. Elle permet de manipuler des collections distribuées
    // sur le cluster avec des transformations fonctionnelles.
    // ============================================================
    println("\n" + "-" * 60)
    println("   DEMO 1 : Word Count (API RDD)")
    println("-" * 60)

    val texte = Seq(
      "Apache Spark est un moteur de traitement distribué",
      "Spark permet de traiter des données à grande échelle",
      "Scala est le langage natif de Spark",
      "Spark utilise le concept de RDD pour distribuer les données",
      "Les RDD sont résilients et distribués sur le cluster",
      "Spark est rapide grâce au traitement en mémoire",
      "Le traitement distribué permet de paralléliser les calculs",
      "Spark supporte Scala Java Python et R"
    )

    // Création d'un RDD à partir d'une collection locale
    // Le RDD est distribué sur les workers du cluster
    val rdd = sc.parallelize(texte)

    // Pipeline de transformations fonctionnelles :
    // flatMap → Découpe chaque ligne en mots
    // map     → Crée une paire (mot, 1)
    // reduceByKey → Agrège les compteurs par mot
    val wordCounts = rdd
      .flatMap(ligne => ligne.toLowerCase.split("\\s+"))  // flatMap : 1 ligne → N mots
      .map(mot => (mot, 1))                                // map : mot → (mot, 1)
      .reduceByKey(_ + _)                                  // reduceByKey : somme par clé
      .sortBy(_._2, ascending = false)                     // Tri par nombre décroissant

    println("\n📊 Top 10 des mots les plus fréquents :")
    println("-" * 35)
    wordCounts.take(10).foreach { case (mot, count) =>
      println(f"   $mot%-20s → $count%d fois")
    }

    println(s"\n📈 Nombre total de mots uniques : ${wordCounts.count()}")

    // ============================================================
    // DEMO 2 : Analyse de ventes avec l'API DataFrame / SQL
    // ============================================================
    // L'API DataFrame est une abstraction de plus haut niveau
    // qui permet de manipuler des données structurées comme
    // dans une base de données SQL. Spark optimise
    // automatiquement les requêtes grâce au Catalyst Optimizer.
    // ============================================================
    println("\n" + "-" * 60)
    println("   DEMO 2 : Analyse de ventes (API DataFrame / SQL)")
    println("-" * 60)

    // Lecture du fichier CSV avec inférence de schéma
    val sales = spark.read
      .option("header", "true")       // La première ligne contient les noms de colonnes
      .option("inferSchema", "true")  // Spark détecte automatiquement les types
      .csv("/opt/spark-data/sales.csv")

    println("\n📋 Schéma des données :")
    sales.printSchema()

    println("📋 Aperçu des données (5 premières lignes) :")
    sales.show(5)

    // Calcul du chiffre d'affaires par produit
    val revenueByProduct = sales
      .withColumn("revenue", col("quantity") * col("price"))
      .groupBy("product")
      .agg(
        sum("quantity").alias("total_quantite"),
        sum("revenue").alias("chiffre_affaires"),
        count("*").alias("nb_transactions")
      )
      .orderBy(desc("chiffre_affaires"))

    println("💰 Chiffre d'affaires par produit :")
    revenueByProduct.show(false)

    // Statistiques globales
    val totalRevenue = sales
      .withColumn("revenue", col("quantity") * col("price"))
      .agg(
        sum("revenue").alias("total"),
        count("*").alias("nb_transactions"),
        avg("revenue").alias("panier_moyen")
      )

    println("📊 Statistiques globales :")
    totalRevenue.show(false)

    // ============================================================
    // DEMO 3 : Requête SQL directe
    // ============================================================
    // Spark permet aussi d'écrire directement des requêtes SQL
    // sur les DataFrames enregistrés comme vues temporaires.
    // ============================================================
    println("\n" + "-" * 60)
    println("   DEMO 3 : Requête SQL directe")
    println("-" * 60)

    sales.createOrReplaceTempView("ventes")

    val topProducts = spark.sql("""
      SELECT
        product AS produit,
        SUM(quantity) AS total_vendu,
        ROUND(SUM(quantity * price), 2) AS chiffre_affaires
      FROM ventes
      GROUP BY product
      HAVING SUM(quantity * price) > 1000
      ORDER BY chiffre_affaires DESC
    """)

    println("\n🏆 Produits avec un CA supérieur à 1000€ :")
    topProducts.show(false)

    // ============================================================
    // DEMO 4 : Cache / Persist — Traitement en mémoire
    // ============================================================
    // C'est L'AVANTAGE N°1 de Spark sur Hadoop MapReduce :
    // Spark garde les données en mémoire entre les opérations,
    // ce qui évite de relire depuis le disque à chaque étape.
    //
    // Sans cache : chaque action relit le fichier depuis le début
    // Avec cache : les données sont gardées en RAM après la 1ère lecture
    // ============================================================
    println("\n" + "-" * 60)
    println("   DEMO 4 : Cache / Persist (traitement en mémoire)")
    println("-" * 60)

    // Créer un grand dataset en dupliquant les ventes
    val bigSales = (1 to 100).foldLeft(sales) { (acc, _) =>
      acc.union(sales)
    }

    // --- SANS CACHE ---
    println("\n⏱️  SANS cache :")
    val startWithout = System.nanoTime()

    // Action 1 : compter les lignes (force la lecture complète)
    val count1 = bigSales.count()
    // Action 2 : recalculer (relecture depuis le début !)
    val avg1 = bigSales.agg(avg("price")).collect()(0).getDouble(0)
    // Action 3 : encore un calcul (encore une relecture !)
    val max1 = bigSales.agg(max("quantity")).collect()(0).getInt(0)

    val timeWithout = (System.nanoTime() - startWithout) / 1e6
    println(f"   3 opérations terminées en $timeWithout%.0f ms")
    println(f"   (count=$count1, avg_price=$avg1%.2f, max_qty=$max1)")

    // --- AVEC CACHE ---
    println("\n⏱️  AVEC cache (données gardées en mémoire) :")

    // .cache() = .persist(StorageLevel.MEMORY_ONLY)
    // Indique à Spark de garder ce DataFrame en RAM
    val cachedSales = bigSales.cache()

    // Première action : charge les données en mémoire
    cachedSales.count()  // "Warm-up" : met les données en cache

    val startWith = System.nanoTime()

    // Maintenant les 3 mêmes opérations, mais les données sont déjà en RAM !
    val count2 = cachedSales.count()
    val avg2 = cachedSales.agg(avg("price")).collect()(0).getDouble(0)
    val max2 = cachedSales.agg(max("quantity")).collect()(0).getInt(0)

    val timeWith = (System.nanoTime() - startWith) / 1e6
    println(f"   3 opérations terminées en $timeWith%.0f ms")
    println(f"   (count=$count2, avg_price=$avg2%.2f, max_qty=$max2)")

    val speedup = timeWithout / timeWith
    println(f"\n🚀 Accélération grâce au cache : ${speedup}%.1fx plus rapide !")
    println("   → Spark évite de relire les données depuis le disque")

    // Libérer le cache
    cachedSales.unpersist()

    // ============================================================
    // DEMO 5 : Jointure entre deux datasets
    // ============================================================
    // En pratique, les données viennent de sources multiples.
    // Spark permet de joindre des datasets distribués
    // efficacement, comme un JOIN SQL classique.
    // ============================================================
    println("\n" + "-" * 60)
    println("   DEMO 5 : Jointure entre datasets")
    println("-" * 60)

    // Lecture du fichier clients
    val customers = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/opt/spark-data/customers.csv")

    println("\n👥 Table des clients :")
    customers.show(false)

    // Jointure entre ventes et clients sur customer_id
    // Comme un JOIN SQL : ON sales.customer_id = customers.customer_id
    val salesWithCustomers = sales
      .join(customers, Seq("customer_id"), "inner")

    println("🔗 Ventes enrichies avec les infos clients (JOIN) :")
    salesWithCustomers.show(10, false)

    // Analyse : CA par ville
    val revenueByCity = salesWithCustomers
      .withColumn("revenue", col("quantity") * col("price"))
      .groupBy("city")
      .agg(
        sum("revenue").alias("chiffre_affaires"),
        countDistinct("customer_id").alias("nb_clients"),
        sum("quantity").alias("total_articles")
      )
      .orderBy(desc("chiffre_affaires"))

    println("🏙️  Chiffre d'affaires par ville :")
    revenueByCity.show(false)

    // Analyse : CA par catégorie client
    val revenueByCategory = salesWithCustomers
      .withColumn("revenue", col("quantity") * col("price"))
      .groupBy("category")
      .agg(
        sum("revenue").alias("chiffre_affaires"),
        countDistinct("customer_id").alias("nb_clients"),
        round(avg(col("quantity") * col("price")), 2).alias("panier_moyen")
      )
      .orderBy(desc("chiffre_affaires"))

    println("👑 Analyse par catégorie de client (Premium vs Standard) :")
    revenueByCategory.show(false)

    // ============================================================
    // DEMO 6 : Spark Structured Streaming
    // ============================================================
    // Spark Streaming permet de traiter des données en temps réel.
    // Ici on simule un flux de données : Spark surveille un
    // dossier et traite automatiquement chaque nouveau fichier
    // qui y apparaît, comme s'il venait d'un flux Kafka ou IoT.
    // ============================================================
    println("\n" + "-" * 60)
    println("   DEMO 6 : Spark Structured Streaming")
    println("-" * 60)
    println("   Spark surveille un dossier et traite les nouveaux")
    println("   fichiers automatiquement en temps réel !")

    // Créer le dossier d'entrée pour le streaming
    import java.nio.file.{Files, Paths, StandardOpenOption}

    val streamInputDir = "/opt/spark-data/stream-input"
    val streamCheckpointDir = s"/opt/spark-data/stream-input/.checkpoint-${System.currentTimeMillis()}"
    Files.createDirectories(Paths.get(streamInputDir))

    // Définir le schéma du flux (même format que sales.csv)
    val streamSchema = sales.schema

    // Créer le flux de lecture : Spark surveille le dossier
    val streamDF = spark.readStream
      .schema(streamSchema)
      .option("header", "true")
      .option("maxFilesPerTrigger", 1)  // Traite 1 fichier à la fois
      .csv(streamInputDir)

    // Pipeline de traitement en temps réel :
    // Calcule le CA par produit sur les nouvelles données
    val streamAgg = streamDF
      .withColumn("revenue", col("quantity") * col("price"))
      .groupBy("product")
      .agg(
        sum("revenue").alias("chiffre_affaires"),
        sum("quantity").alias("total_vendu")
      )

    // Démarrer le flux en mode console (affiche les résultats)
    val query = streamAgg.writeStream
      .outputMode("complete")         // Résultats complets à chaque micro-batch
      .format("console")              // Affiche dans la console
      .option("truncate", "false")
      .option("checkpointLocation", streamCheckpointDir)
      .trigger(Trigger.ProcessingTime("2 seconds"))  // Toutes les 2 secondes
      .start()

    println("\n📡 Streaming démarré ! Écriture de données simulées...")

    // Simuler l'arrivée de données en temps réel
    // Batch 1
    val batch1 =
      """product,quantity,price,customer_id
        |Laptop,4,999.99,1
        |Smartphone,8,699.99,3
        |Tablet,2,449.99,5""".stripMargin
    Files.write(
      Paths.get(s"$streamInputDir/batch1.csv"),
      batch1.getBytes,
      StandardOpenOption.CREATE
    )
    println("   → Batch 1 envoyé (Laptop, Smartphone, Tablet)")
    Thread.sleep(5000)

    // Batch 2
    val batch2 =
      """product,quantity,price,customer_id
        |Ecran,5,349.99,2
        |Laptop,2,999.99,4
        |Casque Audio,12,79.99,6""".stripMargin
    Files.write(
      Paths.get(s"$streamInputDir/batch2.csv"),
      batch2.getBytes,
      StandardOpenOption.CREATE
    )
    println("   → Batch 2 envoyé (Ecran, Laptop, Casque Audio)")
    Thread.sleep(5000)

    // Batch 3
    val batch3 =
      """product,quantity,price,customer_id
        |Smartphone,3,699.99,7
        |Webcam,10,89.99,8
        |Souris,20,29.99,9""".stripMargin
    Files.write(
      Paths.get(s"$streamInputDir/batch3.csv"),
      batch3.getBytes,
      StandardOpenOption.CREATE
    )
    println("   → Batch 3 envoyé (Smartphone, Webcam, Souris)")
    Thread.sleep(5000)

    // Arrêter le streaming après la démo
    query.stop()
    println("\n✅ Streaming arrêté après traitement de 3 batches.")
    println("   → Spark a traité chaque batch en quasi temps réel !")

    // ============================================================
    println("\n" + "=" * 60)
    println("   FIN DE LA DEMO")
    println("=" * 60 + "\n")

    spark.stop()
  }
}
