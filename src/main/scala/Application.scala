import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object Application{

  def main(args: Array[String]): Unit ={

    //creo la spark session

    val sparkSession = SparkSession.builder().master("local[8]").appName("Friendster Project").getOrCreate()

    //Creo lo spark context

    val sparkContext = sparkSession.sparkContext

    // definisco i path per i vari data set

    val usersDataSetPath = "/Users/remokr00/Desktop/Friendster Project/Dataset/com-friendster.ungraph.txt"
    val groupDataSetPath = "/Users/remokr00/Desktop/Friendster Project/Dataset/com-friendster.all.cmty.txt"
    val top5000DataSetPath = "/Users/remokr00/Desktop/Friendster Project/Dataset/com-friendster.top5000.cmty.txt"

    // importo il data set

    val userDataSet = sparkSession.read.text(usersDataSetPath)
    userDataSet.show()

    val groupDataSet = sparkSession.read.text(groupDataSetPath)
    groupDataSet.show()

    val top5000DataSet = sparkSession.read.text(top5000DataSetPath)
    top5000DataSet.show()


  }

}
