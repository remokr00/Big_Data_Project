

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession

import scala.io.Source
import java.io.{FileOutputStream, PrintWriter}
import scala.util.Random

object Gestore {
  def main(args: Array[String]): Unit = {

    //Creo spark Seesion e successivamente lo spark context
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("bigdataProject").getOrCreate()

    val sc: SparkContext = spark.sparkContext

    //definisco i path dei vari dataset
    val datasetAll = "/Users/remokr00/Desktop/Friendster Project/Dataset/com-friendster.ungraph.txt"
    val datasetGruppi = "/Users/remokr00/Desktop/Friendster Project/Dataset/com-friendster.all.cmty.txt"
    val datasetTop500 = "/Users/remokr00/Desktop/Friendster Project/Dataset/com-friendster.top5000.cmty.txt"
    val reducedDataSet = "/Users/remokr00/Desktop/Friendster Project/Dataset/reduced.txt" //file all'interno del quale verrà salvato il dataset ridotto

    //trasformo i file dei gruppi in RDD
    val allGroups = sc.textFile(datasetGruppi)
    val top5000 = sc.textFile(datasetTop500)

    //Apro il file dove mettere gli archi del dataset ridotto
    //val filter = new PrintWriter(new FileOutputStream(reducedDataSet,true))

    /*
   Riduco il dataset prendendo solamente i primi 180606713 per ottenere un campione del grafo
   Non vengono perse informazioni importanti ma, al più, alcune relazioni di amicizia, in quanto il datadet è della forma

   #ID1      #ID2
   101       102
   101       103
   ...       ...

    */

    /*
    Salvo i primi 180606714 archi nel file reduced che verrà usato poi per effettuare le analisi

    var cont = 0
    for(line <- Source.fromFile(datasetAll).getLines()){
      if(! line.contains("#") && cont < 180606714 ){ //la riga non deve contenere commenti e non devo aver raggiunto il limite di archi
        filter.write(line + "\n")
        cont += 1
      }
    }
    */
    //trasformo il file col grafo ridotto in RDD

    val reducedDataSetRDD = sc.textFile(reducedDataSet)


    /*
    Uso sc.parallelize perché altrimenti, anziché creare un RDD[STRING] il datasetRidotto
    risulterebbe un Array[String]
     */


   //Converto ogni riga del file in una coppia di nodi

    val edgesRDD = reducedDataSetRDD.map(line => {
      val Array(src, dst) = line.split("\t") //creo un array di coppie from e to
      (src.toLong, dst.toLong) //converto i numeri in long
    }).cache()

    /*
    Creo il grafo a partire dagli rdd di archi
     */
    val graph = Graph.fromEdgeTuples(edgesRDD, defaultValue = 1).cache()
    graph.edges.take(10).foreach(println)



  }

}
