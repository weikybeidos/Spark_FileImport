/**
  * Created by bastian on 04.07.2017.
  */

import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


object FileImport {


  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName("FileImport")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    hiveContext.setConf("hive.exec.dynamic.partition", "true")
    hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    if ((args(0) != null) && (args(1) != null) && (args(2) != null)) {
      println("import file" + args(1) + "with rowtag" + args(0))
      val xml = hiveContext.read.format("com.databricks.spark.xml").option("rowTag", args(0)).load(args(1))
      val xml_day = xml.withColumn("day", substring(xml("EREIGNIS_ZEITPUNKT"), 0, 10))
      xml_day.write.format("parquet").mode("append").partitionBy("day").saveAsTable(args(2))

    }
    else {
      println("DEFAULT USED PARAMETER MISSING")
      val xml = hiveContext.read.format("com.databricks.spark.xml").option("rowTag", "V_AG_EXPORT_XML").load("/tmp/V_AG_EXPORT_XML.xml")
      val xml_day = xml.withColumn("day", substring(xml("EREIGNIS_ZEITPUNKT"), 0, 10))
      xml_day.write.format("parquet").mode("append").partitionBy("day").saveAsTable("spark_test_table_partfin")

    }


    // terminate spark context
    sc.stop()

  }


}
