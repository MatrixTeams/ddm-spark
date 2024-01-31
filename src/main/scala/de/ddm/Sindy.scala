//package de.ddm
//
//import org.apache.spark.sql.{Dataset, Row, SparkSession}
//
//object Sindy {
//
//  private def readData(input: String, spark: SparkSession): Dataset[Row] = {
//    spark
//      .read
//      .option("inferSchema", "false")
//      .option("header", "true")
//      .option("quote", "\"")
//      .option("delimiter", ";")
//      .csv(input)
//  }
//
//  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
//    import spark.implicits._
//
//    inputs.map(input => readData(input, spark)) //read input data
//      .map(df => mapRowsWithColumn(df)) //pair all values with column names
//      .reduce(_ union _) //combine dataframes
//      .groupByKey(k => k._1) //group by value -> resulting in a set of columns that contain value
//      .mapGroups((key, value) => (key, value.flatMap(elem => elem._2).toSet)) //get value key combinations
//      .flatMap(set => findCandidates(set)) //find all candidates
//      .groupByKey(candidate => candidate._1) //group by candidates
//      .mapGroups((value, set) => checkINDs(value, set)) //check for INDs
//      .filter(elem => elem._2.nonEmpty) //all non empties are the final INDs
//      .collect() //start processing
//      .map(row => (row._1, row._2.toList.sorted)) //sort the dependencies alphabetically
//      .sortBy(x => x._1) //sort the reference alphabetically (row wise)
//      .foreach(row => prettyPrint(row)) //print final results
//
//
//    def mapRowsWithColumn(df: Dataset[Row]) = {
//      val column = df.columns
//      df.flatMap(row => {
//        for (i <- column.indices) yield {
//          (row.get(i).toString, Set(column(i)))
//        }
//      })
//    }
//
//    def findCandidates(set: (String, Set[String])) = {
//      set._2.map(identifier => (identifier, set._2 - identifier))
//    }
//
//    def checkINDs(value: String, set: Iterator[(String, Set[String])]) = {
//      (value, set
//        .map(x => x._2)
//        .reduce((reference, dependent) => reference.intersect(dependent))
//      )
//    }
//
//    def prettyPrint(row: (String, List[String])): Unit = {
//      val result = row._1 + " < " + row._2.mkString(", ")
//      println(result)
//    }
//
//  }
//
//}
//
//
package de.ddm
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Sindy {

  // Function to read data, similar to the original Sindy implementation
  private def readData(input: String, spark: SparkSession): Dataset[Row] = {
    spark.read
      .option("inferSchema", "false")
      .option("header", "true")
      .option("quote", "\"")
      .option("delimiter", ";")
      .csv(input)
  }

  // Main function to discover INDs
  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    import spark.implicits._

    val columnsData = inputs.flatMap(input => {
      val df = readData(input, spark)
      df.columns.map(column => (column, df.select(column).distinct.as[String].collect.toSet))
    }).toMap

    val potentialINDs = columnsData.keys.flatMap { dependentCol =>
      columnsData.keys.collect {
        case referenceCol if dependentCol != referenceCol && columnsData(dependentCol).subsetOf(columnsData(referenceCol)) =>
          (dependentCol, referenceCol)
      }
    }
    val aggregatedINDs = potentialINDs
      .groupBy(_._1)
      .mapValues(_.map(_._2).toList.sorted)
      .toSeq
      .sortBy(_._1)
    aggregatedINDs.foreach {
      case (dependent, references) =>
        println(s"$dependent < ${references.mkString(", ")}")
    }
  }
}
