import org.apache.spark.graphx
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD

object Queries {

  /*
  1. Troviamo gli utenti più popolari
   */

  def most_popular(graph: Graph[_,_]): Array[(graphx.VertexId, Int)] ={
    //essendo il grafo diretto verifichiamo per ogni nodo gli archi in uscita
    val in_degrees = graph.inDegrees.cache()
    //effettuo un ordinamento rispetto al numero di archi in entrata
    val ris = in_degrees.sortBy(utente => utente._2, ascending = false).take(100)
    ris
  }


}
