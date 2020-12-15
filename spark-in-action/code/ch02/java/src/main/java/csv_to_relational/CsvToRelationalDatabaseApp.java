package csv_to_relational;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;


public class CsvToRelationalDatabaseApp {

  String DATAFILE = "/home/carl/Code/Hadoop/spark/spark-in-action/code/ch02/data/authors.csv";

  public static void main(String[] args) {
    CsvToRelationalDatabaseApp app = new CsvToRelationalDatabaseApp();
    app.start();
  }

  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("CSV to DB")
        .master("local")
        .getOrCreate();

    // Reads the CSV file
    Dataset<Row> df = spark.read()
        .format("csv")
        .option("header", "true")
        .load(DATAFILE);

    // Creates a new virtual column
    df = df.withColumn(
        "name",
        concat(df.col("lname"), lit(", "), df.col("fname")));

    // Save in the Postgres table
    String dbConnectionUrl = "jdbc:postgresql://localhost/sparkdb";

    // Properties to connect to the database, the JDBC driver is part of our pom.xml
    Properties prop = new Properties();
    prop.setProperty("driver", "org.postgresql.Driver");
    prop.setProperty("user", "sparkuser");
    prop.setProperty("password", "sparkpw");

    // Write in a table called ch02
    df.write()
        .mode(SaveMode.Overwrite)
        .jdbc(dbConnectionUrl, "ch02", prop);

    System.out.println("Process complete");
  }
}