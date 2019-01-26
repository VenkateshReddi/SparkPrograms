package com.sparkprograms.examples

import org.apache.spark.sql.SparkSession

object TestSpark {
def main(args: Array[String]): Unit ={
  val spark = SparkSession.builder().appName("TestSpark").master("local[*]").getOrCreate()

  import spark.implicits._

  val ds = spark.createDataset(Seq(1,2,3,4,5))
  ds.show()
  //create dataset from RDD
  val rdd = spark.sparkContext.parallelize(Seq(1,2,3,4,5))
  val rdd_ds = rdd.toDS()
  val rdd_df = rdd.toDF()
  rdd_ds.show()
  rdd_df.show()
}
}
