package lab200_ingestion_schema_manipulation;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class IngestionSchemaManipulationApp {

  public static void main(String[] args) {
    IngestionSchemaManipulationApp app = new IngestionSchemaManipulationApp();
    app.start();
  }

  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Restaurants in Wake County, NC")
        .master("local")
        .getOrCreate();

    // Reads a CSV file with header
    String csvPath = "/home/carl/Code/Hadoop/spark/spark-in-action/code" + 
        "/ch03/data/Restaurants_in_Wake_County_NC.csv";
    Dataset<Row> df = spark.read().format("csv")
        .option("header", "true")
        .load(csvPath);

    System.out.println("*** Right after ingestion");
    df.show(5);
    df.printSchema();
    System.out.println("We have " + df.count() + " records.");

    // Let's transform our dataframe
    df = df.withColumn("county", lit("Wake"))
        .withColumnRenamed("HSISID", "datasetId")
        .withColumnRenamed("NAME", "name")
        .withColumnRenamed("ADDRESS1", "address1")
        .withColumnRenamed("ADDRESS2", "address2")
        .withColumnRenamed("CITY", "city")
        .withColumnRenamed("STATE", "state")
        .withColumnRenamed("POSTALCODE", "zip")
        .withColumnRenamed("PHONENUMBER", "tel")
        .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
        .withColumnRenamed("FACILITYTYPE", "type")
        .withColumnRenamed("X", "geoX")
        .withColumnRenamed("Y", "geoY")
        .drop("OBJECTID")
        .drop("PERMITID")
        .drop("GEOCODESTATUS");

    df = df.withColumn("id", concat(
           df.col("state"),
           lit("_"),
           df.col("county"), lit("_"),
           df.col("datasetId")));

    // Shows at most 5 rows from the dataframe
    System.out.println("*** Dataframe transformed");
    df.show(5);

    // df.printSchema();

    // Get number of partitions
    // System.out.println("*** Looking at partitions");
    // Partition[] partitions = df.rdd().partitions();
    // int partitionCount = partitions.length;
    // System.out.println("Partition count before repartition: " + partitionCount);

    // Repartition
    // df = df.repartition(4);
    // System.out.println("Partition count after repartition: " +
    //    df.rdd().partitions().length);

    // Stop SparkSession
    spark.stop();
  }
}