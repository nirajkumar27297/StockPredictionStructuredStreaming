package AWSUtilites
import org.apache.spark.SparkContext

object S3Configurations {

  /***
    * AWS S3 Connect Configuration
    * @param sparkContextObj SparkContext
    * @return Boolean
    */
  def connectToS3(
      sparkContextObj: SparkContext
  ): Boolean = {
    System.setProperty("com.amazonaws.services.s3.enableV4", "true")
    sparkContextObj.hadoopConfiguration
      .set("fs.s3a.awsAccessKeyId", System.getenv("AWS_ACCESS_KEY_ID"))
    sparkContextObj.hadoopConfiguration
      .set("fs.s3a.awsSecretAccessKey", System.getenv("AWS_SECRET_ACCESS_KEY"))
    sparkContextObj.hadoopConfiguration.set(
      "fs.s3a.impl",
      "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    sparkContextObj.hadoopConfiguration
      .set("fs.s3a.endpoint", "s3.amazonaws.com")
    true
  }
}
