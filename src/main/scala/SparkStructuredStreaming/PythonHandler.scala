package SparkStructuredStreaming

import UtilityPackage.Utility
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class PythonHandler(sparkSessionObj: SparkSession) {

  /**
    * The objective of the function is to pipe the python machine learning algorithm for the
    * given input dataframe and predict the Close Price
    * @param inputDataFrame [DataFrame]
    * @return predictedStockPriceDataFrame [DataFrame]
    */
  //Configuring log4j
  lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def loadingLinearRegressionModelPython(
      inputDataFrame: DataFrame,
      pythonFilePath: String
  ): DataFrame = {

    logger.info("Predicting Close Price Using Python Model")
    try {
      var predictedStockPriceDataFrame = sparkSessionObj.emptyDataFrame
      if (!inputDataFrame.isEmpty) {
        val predictedPriceRDD =
          Utility.runPythonCommand(
            pythonFilePath,
            inputDataFrame.drop("Close", "Date")
          )
        //Collecting the result from the output RDD and converting it to Double
        val predictedPrice =
          predictedPriceRDD.collect().toList.map(elements => elements.toDouble)
        //Creating a new dataframe with new predicted value Column
        predictedStockPriceDataFrame = sparkSessionObj.createDataFrame(
          // Adding New Column
          inputDataFrame.rdd.zipWithIndex.map {
            case (row, columnIndex) =>
              Row.fromSeq(row.toSeq :+ predictedPrice(columnIndex.toInt))
          },
          // Create schema
          StructType(
            inputDataFrame.schema.fields :+ StructField(
              "Predicted Close Price",
              DoubleType,
              false
            )
          )
        )
      }
      predictedStockPriceDataFrame
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        logger.info("Difficulty in Predicting Close Price Using Python Model")
        throw new Exception(
          "Difficulty in Predicting Close Price Using Python Model"
        )
    }
  }

}
