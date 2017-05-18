package xstream.example;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import xstream.cli.CommandLineClient;
import xstream.spark.TimeseriesRDD;

/**
 * Runs Apache Spark driver as a service.
 * 
 * @author pinaki poddar
 *
 */
public class SparkService {
    private SparkSession _spark;
    private TimeseriesRDD _series;
    
    public SparkService() {
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Spark Service");
        _spark = new SparkSession.Builder()
                .config(conf)
                .getOrCreate();
    }
     
    
    public void stop() {
        _spark.sparkContext().stop();
    }
    
    public TimeseriesRDD setSeries(String url) {
        return _series = new TimeseriesRDD(_spark, url);
    }
    
    public Dataset<Row> executeQuery(String sql) {
        return _series.toDataFrame().sqlContext().sql(sql);
    }
    
    public static void main(String[] args) throws Exception {
        SparkService service = new SparkService();
        CommandLineClient cli = new CommandLineClient();
        cli.setSparkSession(service._spark);
        cli.run();
        
        
    }

}
