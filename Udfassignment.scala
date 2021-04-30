package demo
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatterBuilder
import java.time._
import org.apache.spark.sql.functions.{col, column, date_format, degrees, row_number, struct, udf, unix_timestamp}
import org.apache.spark.sql.SparkSession

import java.sql.Timestamp
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.joda.time.PeriodType.time
import org.joda.time.format.DateTimeFormat
import java.text.SimpleDateFormat
import org.apache.spark.sql.DataFrame

object Udfassignment {
 def main(args:Array[String]):Unit = {

   val spark = SparkSession.builder.master("local").appName("Employee Assignment").getOrCreate()
   import spark.implicits._
   spark.sparkContext.setLogLevel("WARN")
   val df = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
     .csv("src\\main\\resources\\emp_data.txt")
   df.show()

   val w3 = Window.partitionBy("designation").orderBy(col("salary").desc)
            df.withColumn("row",row_number.over(w3))
            .show()

    // write data into file in csv format
    //df.write.option("header","true").csv("src\\main\\resources\\empData")

    // Find first record of the file
    val emp_header = df.first()
    println(emp_header)

   def tooLong(s:Int): Long = {
     s.toLong
   }

   def tooTimeStamp(s:Int) = {
     //val T:Timestamp = Timestamp.valueOf(s)
     val ss:String = LocalDateTime.now().toString
     val res = ss.replace("T","_").replace("-","").replace(":","")
     res //return result
   }
     //println(tooTimeStamp("2018-09-01 09:01:15"))
     val toLongUDF = udf[Long,Int](tooLong)
     val toTsUDF = udf[String,Int](tooTimeStamp)

     val dataset = Seq((100, "Smith",29042021,"India"), (101, "Jones",29042021,"UK"),(102, "Jack",29042021,"Sweden"),
       (103, "Jame",29042021,"Russia"))
       .toDF("id","name","Date","countries")


     dataset.printSchema()
     dataset.withColumn("newcol",toLongUDF($"id")).show()
     dataset.withColumn("newTS",toTsUDF($"Date")).show()



     // 2. Assignment
     // List of Strings
      val country_names: List[String] = List("India", "USA", "Sweden","Africa")

      def udf_list(country_names:List[String]) = {
        udf((name:String) => {
          country_names.contains(name)
       })
   }
   println(udf_list(country_names))
   dataset.withColumn("is_name_in_list", udf_list(country_names)($"countries")).show()
 }
}