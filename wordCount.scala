import org.apache.spark.sql.functions._
import spark.implicits._

val lines = spark.read.text("/Users/niranjan/Documents/demo.txt").as[String]

val words = lines.flatMap(line => line.split("\\s+")).filter(word => word != "")

val wordCounts = words.groupBy("value").count()

wordCounts.orderBy(desc("count")).show(false)
