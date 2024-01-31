
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
