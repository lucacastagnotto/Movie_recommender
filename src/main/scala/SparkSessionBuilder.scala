import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionBuilder {
  def create_SparkSession(): SparkSession = {
    // Spark configuration
    val spark_conf = new SparkConf().setMaster("local[*]").setAppName("movie_reccomender")
    // Spark session
    val spark_session = SparkSession.builder()
      .config(spark_conf)
      .getOrCreate()

    spark_session
  }
}
