import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{row_number,collect_list}
import org.apache.spark.sql.expressions.Window

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
    val resultDF = spark.sql("SELECT e.itemId, e.catId, e.transDate  FROM example e join category c on e.catId=c.catId ")
    resultDF.show()
    val w = Window.partitionBy($"itemId").orderBy($"transDate".desc)
    val dfTop = resultDF.withColumn("rn", row_number.over(w)).where($"rn" <=2).drop("rn")
    dfTop.show;

    dfTop.groupBy("itemId")
      .agg(collect_list("catId").alias("catIds"))
      .write.json("src/main/resources/output")

  }
}