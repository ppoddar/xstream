package xstream.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import xstream.spark.TimeseriesRDD;


public class SparkDemo {
    public static void main(String[] args) throws Exception {
        
        SparkService spark = new SparkService();
        TimeseriesRDD rdd =  spark.setSeries("nosql://localhost:5000/kvstore/traffic");
        final String sqlText = "select avg(medianMeasuredTime) from " 
                + rdd.getSeriesName()
                + "  where medianMeasuredTime > 70";
      
        Dataset<Row> rs = spark.executeQuery(sqlText);
        rs.show();
    }
}
