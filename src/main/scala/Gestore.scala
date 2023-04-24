

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession

import java.io.{FileOutputStream, PrintWriter}
import scala.io.Source
import scala.util.control.Breaks.break

object Gestore {

  var graph: Graph[_, _] = _ //creo il grafo come variabile globale

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
    val most_popular = "/Users/remokr00/Desktop/Friendster Project/Dataset/most_popular.txt" //file all'interno del quale verrà salvato il dataset ridotto

    /*
    Path Ilaria
     */

    //val datasetGruppi = "/Volumes/Extreme SSD/Friendster Project/Dataset/com-friendster.all.cmty.txt"
    //val datasetAll =  "/Volumes/Extreme SSD/Friendster Project/Dataset/com-friendster.ungraph.txt"
    //val datasetTop500 =  "/Volumes/Extreme SSD/Friendster Project/Dataset/com-friendster.top5000.cmty.txt"
    //val reducedDataSet = "/Volumes/Extreme SSD/Friendster Project/Dataset/reduced.txt" //file all'interno del quale verrà salvato il dataset ridotto

    //trasformo i file dei gruppi in RDD
    val allGroups = sc.textFile(datasetGruppi)
    val top5000 = sc.textFile(datasetTop500)


    /*
    Riduco il dataset prendendo solamente i primi 180606713 per ottenere un campione del grafo
    Non vengono perse informazioni importanti ma, al più, alcune relazioni di amicizia, in quanto il datadet è della forma

    #ID1      #ID2
    101       102
    101       103
    ...       ...

    Salvo i primi 180606714 archi nel file reduced che verrà usato poi per effettuare le analisi

    var cont = 0
    for(line <- Source.fromFile(datasetAll).getLines()){
      if(! line.contains("#") && cont < 180606714 ){ //la riga non deve contenere commenti e non devo aver raggiunto il limite di archi
        filter.write(line + "\n")
        cont += 1
      }
    }
    */

    /*
    Metodo alternativo, sperabilmente più efficiente per ridurre il dataset
     */

    //per evitare di rifare le stesse operazioni più volte controllo che il file non esista
    //if(reducedDataSet.isEmpty) {

    val filter = new PrintWriter(new FileOutputStream(reducedDataSet, true))
    var idCount = Map.empty[String, Int] //mappa che tiene conto di quante volte appare l'ID nel file ridotto per definire poi la penalità
    val uniqueIDs = collection.mutable.Set[String]()
    var numEdges = 0
    for (line <- Source.fromFile(datasetAll).getLines().drop(4)) { //escludo le righe coi commenti con drop
      val fstElem = line.split("\t")(0)
      if (!uniqueIDs.contains(fstElem)) { //id non è mai stato inserito
        uniqueIDs += fstElem //aggiungo l'id
        filter.write(line + "\n")
        // Inizializzo il conteggio per l'id corrente a 1
        idCount += (fstElem -> 1)
        numEdges+=1
      }
      else {
        val currentCount = idCount(fstElem)
        // Genero un numero casuale tra 0 e il conteggio corrente elevato a un parametro di scala
        val rnd = scala.util.Random.nextDouble()
        val lambda = 1/(1*Math.sqrt(currentCount))
        // Se il numero casuale è uguale al conteggio corrente, allora sostituisco la riga corrente
          if (rnd < lambda) {
            filter.write(line + "\n")
            // Aggiorno il conteggio per l'id corrente
            idCount += (fstElem -> (currentCount + 1))
            numEdges+=1
          }
      }
      if(numEdges > 180606714){
        break
      }

    }
    //chiudo il printwriter
    filter.close()




    /*

    var idCount = Map.empty[String, Int] //mappa che tiene conto di quante volte appare l'ID nel file ridotto per definire poi la penalità
    for (line <- Source.fromFile(datasetAll).getLines().drop(4)) { //escludo le righe coi commenti con drop
      val fstElem = line.split("\t")(0)
      if(!idCount.contains(fstElem)){
        idCount += (fstElem -> 1)
      }
      else{
        idCount += (fstElem -> (idCount(fstElem) + 1))
      }

    }
    //val printriter = new PrintWriter(new FileOutputStream(most_popular,true))
    val orderedIdCount = idCount.toList.sortBy(-_._2).toMap
    /*
    var cont = 0
    for(u <- orderedIdCount){
      if(cont < 180606714){
        //voglio aggiungere a most pop la chiave contenuta in orderedIdCOunt
        printriter.write(u._1+"\n")
        cont += orderedIdCount(u._1)
      }
    }


 */


    println(orderedIdCount)

    /*
    Metodo milo e mario
     */




  }



}

/*

    //trasformo il file col grafo ridotto in RDD

    val reducedDataSetRDD = sc.textFile(reducedDataSet).cache()

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
    graph = Graph.fromEdgeTuples(edgesRDD, defaultValue = 1).cache()



     //val most_popular = sc.parallelize(Queries.most_popular(graph)).cache()
     println("Il path è: "+Queries.distance(graph, 101, 181))



 */





 */
  }
}




