import org.apache.spark.sql.SparkSession

object SparkWordCount {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("myscalaapp").config("spark.master", "local").getOrCreate()
    import spark.implicits._
    val df = spark.read.json("src/main/resources/example.json")
    // Creates a temporary view using the DataFrame
    df.createOrReplaceTempView("example")

    val catdf = spark.read.json("src/main/resources/category.json")
    // Creates a temporary view using the DataFrame
    catdf.createOrReplaceTempView("category")
    val resultDF = spark.sql("SELECT * FROM example e join category c on e.catId=c.catId ")
    resultDF.show()
  }
}