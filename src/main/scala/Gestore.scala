
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.functions.{col, rand, when}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import java.io.File


object Gestore {

  var graph: Graph[_, _] = _ //creo il grafo come variabile globale

  def main(args: Array[String]): Unit = {

    //Creo spark Seesion e successivamente lo spark context
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("bigdataProject")
      .config("spark.driver.memory", "8g")
      .config("spark.executor.memory", "8g")
      .getOrCreate()


    val sc: SparkContext = spark.sparkContext

    //definisco i path dei vari dataset
    val datasetAll = "/Users/remokr00/Desktop/Friendster Project/Dataset/com-friendster.ungraph.txt"
    val datasetGruppi = "/Users/remokr00/Desktop/Friendster Project/Dataset/com-friendster.all.cmty.txt"
    val datasetTop500 = "/Users/remokr00/Desktop/Friendster Project/Dataset/com-friendster.top5000.cmty.txt"
    val reducedDataSet = "/Users/remokr00/Desktop/Friendster Project/Dataset/reduced" //file all'interno del quale verrà salvato il dataset ridotto

    /*
    Path Ilaria
     */

    //val datasetGruppi = "/Volumes/Extreme SSD/Friendster Project/Dataset/com-friendster.all.cmty.txt"
    //val datasetAll =  "/Volumes/Extreme SSD/Friendster Project/Dataset/com-friendster.ungraph.txt"
    //val datasetTop500 =  "/Volumes/Extreme SSD/Friendster Project/Dataset/com-friendster.top5000.cmty.txt"
    //val reducedDataSet = "/Volumes/Extreme SSD/Friendster Project/Dataset/reduced.txt" //file all'interno del quale verrà salvato il dataset ridotto

    //trasformo i file dei gruppi in RDD

    val all = sc.textFile(datasetAll).filter(!_.startsWith("#")) //escludo i commenti
    // val allDF = spark.read.text(datasetAll).filter(row => !row.toString().contains('#'))

    // converte ogni riga in una tupla di due elementi
    val tuples = all.map(line => {
      val fields = line.split("\t")
      (fields(0), fields(1))
    })

    //creo degli oggetti Row in modo da creare il DF
    val rowRDD = tuples.map(p => Row(p._1, p._2))

    //Definisco lo schema del DF in modo tale che abbia due colonne Source e Destination
    val schema = StructType(Seq(
      StructField("Src", StringType, true),
      StructField("Dst", StringType, true)
    ))

    //creo il DF
    val allDF = spark.createDataFrame(rowRDD, schema)

    //verifico che il DF sia corretto
    allDF.printSchema()
    allDF.show(false)

    val allGroups = sc.textFile(datasetGruppi)
    val top5000 = sc.textFile(datasetTop500)


    //------------------------------------------------------------ Riduzione con DF

    val reducedFolder = new File(reducedDataSet)

    if (!reducedFolder.exists() || !reducedFolder.isDirectory || reducedFolder.listFiles().length == 0) {
      // Calcolo del numero di archi per ogni nodo
      val my_degree = allDF.groupBy("Src").count().withColumnRenamed("count", "degree_count")

      // Definizione delle fasce di numero di archi
      val lessThan1k = my_degree.filter("degree_count < 1000").withColumnRenamed("degree_count", "lessThan1k_degree_count")
      val between1kAnd5k = my_degree.filter("degree_count >= 1000 AND degree_count <= 5000").withColumnRenamed("degree_count", "between1kAnd5k_degree_count")
      val greaterThan5k = my_degree.filter("degree_count > 5000").withColumnRenamed("degree_count", "greaterThan5k_degree_count")

      // Definizione della percentuale di campionamento per ciascuna fascia di archi
      val sampleFractionLessThan1k = 0.1
      val sampleFractionBetween1kAnd5k = 0.05
      val sampleFractionGreaterThan5k = 0.01

      // Creazione del DataFrame di struttura stratificata
      val strata = allDF.select("Src").distinct()
        .join(lessThan1k, Seq("Src"), "left_outer")
        .join(between1kAnd5k, Seq("Src"), "left_outer")
        .join(greaterThan5k, Seq("Src"), "left_outer")
        .withColumn("sampleFraction", when(col("lessThan1k_degree_count").isNull, null)
          .when(col("lessThan1k_degree_count") < 1000, sampleFractionLessThan1k)
          .when(col("between1kAnd5k_degree_count") >= 1000 && col("between1kAnd5k_degree_count") <= 5000, sampleFractionBetween1kAnd5k)
          .otherwise(sampleFractionGreaterThan5k))
        .withColumn("rand", rand())

      // Campionamento stratificato del DataFrame originale
      val sampled = allDF.join(strata, Seq("Src"))
        .where("rand <= sampleFraction")
        .select(allDF.columns.map(col): _*)

      sampled.show()
      println(sampled.count())

      //salvo il df su un file di testo
      sampled.write
        .option("header", "false")
        .option("sep", ";")
        .option("coalesced", "true")
        .csv(reducedDataSet)
    }

    //leggo il dataset ridotto
    val reduced = sc.textFile(reducedDataSet + "/*") //leggo tutti i file

    //Converto ogni riga del file in una coppia di nodi

    val edgesRDD = reduced.map(line => {
      val Array(src, dst) = line.split(";") //creo un array di coppie from e to
      (src.toLong, dst.toLong) //converto i numeri in long
    }).cache()

    edgesRDD.take(100).foreach(println)

    /*
    Creo il grafo a partire dagli rdd di archi
     */
    graph = Graph.fromEdgeTuples(edgesRDD, defaultValue = 1).cache()

  }


}












