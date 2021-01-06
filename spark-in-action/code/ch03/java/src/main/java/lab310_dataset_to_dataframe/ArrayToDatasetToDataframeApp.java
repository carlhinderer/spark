package lab310_dataset_to_dataframe;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class ArrayToDatasetToDataframeApp {

  public static void main(String[] args) {
    ArrayToDatasetToDataframeApp app = new ArrayToDatasetToDataframeApp();
    app.start();
  }

  private void start() {

    // Start a SparkSession
    SparkSession spark = SparkSession.builder()
        .appName("Array to dataframe")
        .master("local")
        .getOrCreate();

    // Array to Dataset
    String[] stringList = new String[] { "Jean", "Liz", "Pierre", "Lauric" };
    List<String> data = Arrays.asList(stringList);
    Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
    ds.show();
    ds.printSchema();

    // Dataset to Dataframe
    Dataset<Row> df = ds.toDF();
    df.show();
    df.printSchema();
  }
}