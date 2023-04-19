import org.apache.spark.graphx
import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

object Queries {

  /*
  1. Troviamo gli utenti più popolari
   */

  def most_popular(graph: Graph[_,_]): Array[(graphx.VertexId, Int)] ={
    //essendo il grafo diretto verifichiamo per ogni nodo gli archi in entrata
    val in_degrees = graph.inDegrees.cache()
    //effettuo un ordinamento rispetto al numero di archi in entrata
    val ris = in_degrees.top(100)(Ordering.by(_._2))
    ris.foreach(println)
    in_degrees.unpersist()
    ris
  }

  def pageRank(graph: Graph[_, _], numIter: Int, resetProb: Double): RDD[(VertexId, Double)] = {
    graph.pageRank(numIter, resetProb).vertices
  }
  def mostPopular(graph: Graph[_,_]): Array[(VertexId, Double)] ={
    val pageRanks = pageRank(graph, 10, 0.15)
    val mostPopular = pageRanks.top(100)(Ordering.by(_._2))
    mostPopular.foreach(println)
    mostPopular
  }

   /*
   2.  Selezionato un utente, restituire una tupla contenente il numero e la lista dei suoi amici(seguiti)
    */
  def friends_list(graph: Graph[_,_], id: Long): (Long, List[Int])= {
    val list = graph.outDegrees.filter {case (user, _) => user == id} //filter su archi
    val count  =list.values.sum().toLong
    val list_id = list.values.collect().toList
    (count, list_id)
  }

  /*
    3.  Selezionato un utente, restituire una tupla contenente il numero e la lista dei suoi follower
     */
  def follower_list(graph: Graph[_,_], id: Long): (Long, List[Int])= {
    val list = graph.inDegrees.filter {case (user, _) => user == id}
    val count  =list.values.sum().toLong
    val list_id = list.values.collect().toList
    (count, list_id)
  }






}
