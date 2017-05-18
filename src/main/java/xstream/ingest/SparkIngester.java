package xstream.ingest;

import java.io.File;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import scala.reflect.ClassManifestFactory$;
import xstream.TimeSeriesBuilder;
import xstream.WritableTimeSeries;
import xstream.spark.F1;
import xstream.spark.F2;

@SuppressWarnings("serial")
public class SparkIngester implements Serializable {
    public static void main(String[] args) {
        //System.setProperty("spark.serializer", KryoSerializer.class.getName());
        //System.setProperty("spark.kryo.registrator", KryoRegistrator.class.getName());

        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Spark Service");
        SparkSession  spark = new SparkSession.Builder()
                .config(conf)
                .getOrCreate();
        
        String dir = "/Users/ppoddar/Downloads/traffic_feb_june/";
        File rootDir = new File(dir);
        
        final String[] fieldDefs = {
                "status STRING",
                "avgMeasuredTime DOUBLE",
                "avgSpeed DOUBLE",
                "extID INTEGER",
                "medianMeasuredTime DOUBLE",
                "TIMESTAMP STRING",
                "vehicleCount INTEGER",
                "id INTEGER", // does not accept underscore as first char
                "REPORT_ID INTEGER"
                
        };
        
        final String url = "nosql://localhost:5000/kvstore/traffic";
        
        final int headerLines = 1;
        final int timestampFieldIndex = 5;
        final DateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        if (!rootDir.exists()) {
            //error(1, "input file " + args[0] + " not found");
        }
        if (rootDir.isDirectory()) {
           
           RDD<Tuple2<String,String>> input = spark.sparkContext()
                   .wholeTextFiles(rootDir.getAbsolutePath(), 100);
           

           RDD<Long> ingested = input.map(new F1<Tuple2<String,String>,Long> (){
               public Long apply(Tuple2<String,String> csvFile) {
                   String content = csvFile._2;
                   String[] lines = content.split("\\r?\\n");
                   System.err.println("Loading " + csvFile._1 + " (" + lines.length + " lines)");
                   long start = System.currentTimeMillis();
                   final WritableTimeSeries timeseries = 
                           new TimeSeriesBuilder()
                           .withSeriesURL(url)
                           .withFieldDefinitions(fieldDefs)
                           .openForWrite();
                   for (int i = headerLines; i < lines.length; i++) {
                       String[] values = lines[i].split(",");
                       try {
                           Date date = timestampFormat.parse(values[timestampFieldIndex]);
                           long ts = date.getTime();
                           timeseries.write(ts, (Object[])values);
                       } catch (Exception ex) {
                           ex.printStackTrace();
                       }
                   }
                   return System.currentTimeMillis()-start;
               }
           }, ClassManifestFactory$.MODULE$.fromClass(Long.class));
           
           ingested.reduce(new F2<Long,Long,Long>() {

            @Override
            public Long apply(Long arg0, Long arg1) {
                System.err.println("reduce " + arg0 + " " + arg1);
                return null;
            }});
        }
        
        
        
            

    }

}
