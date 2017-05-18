package xstream.spark;

import java.io.Serializable;

import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

import scala.collection.JavaConverters;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;
import xstream.Event;
import xstream.ReadableTimeSeries;
import xstream.Slot;
import xstream.TimeSeries;
import xstream.TimeSeriesBuilder;
import xstream.util.NoSQLURL;

/**
 * TimeSeriesRDD is the primary integration point for Apache Spark and Oracle
 * NoSQL Timeseries. 
 * 
 * TimeSeries is persisted in Oracle NoSQL as a series of  time slots.  
 * A time slot logically contains a sequence of {@link Event}. The 
 * {@link Event#getTimestamp() timestamp} of consecutive events in a 
 * {@link Slot slot} are monotonic. A time slot is stored in a single row 
 * in Oracle NoSQL database.
 * 
 * A TimeSeriesRDD maps integral number of time slots to a Spark partition.
 * 
 * The advantage of integrating timeseries with Spark is to use SQL
 * and advanced analysis features of Spark.
 * 
 * @author pinaki poddar
 *
 */
@SuppressWarnings("serial")
public class TimeseriesRDD extends RDD<Event> {
    private final NoSQLURL _url;
    private transient ReadableTimeSeries _series;
    private transient final SparkSession _session;
    
    public transient static ClassTag<Event> EVENT_TAG = 
            ClassManifestFactory$.MODULE$.fromClass(Event.class);
    
    /**
     * Creates an RDD in given Spark context and based on given timeseries
     * URL. 
     * @param session a spark session
     * @param timeseriesURL an URL for Oracle NoSQL Timeseries. The URL is
     * of the form <code>nosql://host:port/store-name/timeseries-name</code>
     */
    public TimeseriesRDD(SparkSession session, String timeseriesURL) {
        super(session.sparkContext(), new ArrayBuffer<Dependency<?>>(), EVENT_TAG);
        _url = new NoSQLURL(timeseriesURL);
        _session = session;
    }
    
    public String getSeriesName() {
        return _url.getSeriesName();
    }
    
    /**
     * Computes the partition.
     * Each partition is one slot in underlying timeseries.
     * <em>note</em>:  currently, for simplicity, partition index is equal 
     * to slot index. A more intelligent solution would be to assign multiple
     * slots to a partition.
     */
    @Override
    public scala.collection.Iterator<Event> compute(Partition partition, 
            TaskContext taskCtx) {
        java.util.Iterator<Event> jIt = getTimeSeries()
                .readBySlot(partition.index());
        return JavaConverters.asScalaIteratorConverter(jIt).asScala();
    }
    
    /**
     * Gets the partitions.
     * Number of partition is equal to number of slot in underlying timeseries.
     * <em>note</em>:  currently, for simplicity, partition index is equal 
     * to slot index. A more intelligent solution would be to assign multiple
     * time slots to a partition.
     */
    @Override
    public Partition[] getPartitions() {
        int nSlot = getTimeSeries().getSlotCount();
        Partition[] slots = new SlotPartition[nSlot];
        for (int i = 0; i < nSlot; i++) {
            slots[i] = new SlotPartition(i);
        }
        return slots;
    }
    
    /**
     * Converts this RDD to a DataFrame or, more precisely, a {@link Dataset}
     * of {@link Row}.
     * Each {@link Event} is transformed to a {@link Row}.
     * 
     * @return a data set of row.
     */
    public Dataset<Row> toDataFrame() {
        JavaRDD<Row> rdd = this.toJavaRDD().map(new Function<Event, Row>() {
            @Override
            public Row call(Event event) throws Exception {
                return RowFactory.create(event.values());
            }
        });
        
        Dataset<Row> df = _session.sqlContext().createDataFrame(rdd, 
                SchemaMapper.buildSchema(getTimeSeries()));
        
        df.createOrReplaceTempView(getSeriesName());
        
        return df;

    }
    
    
    /**
     * Lazily initializes the time series with given URL.
     */
    ReadableTimeSeries getTimeSeries() {
        return _series != null ? _series  
                : new TimeSeriesBuilder()
                    .withSeriesURL(_url.toString())
                    .openForRead();
    }

    
    /**
     * A RDD partition mapping to slot(s) of {@link TimeSeries}.
     * 
     *
     */
    class SlotPartition implements Partition, Serializable {
        private final int _idx;
        
        SlotPartition(int idx) {
            _idx = idx;
        }
        
        @Override
        public int index() {
            return _idx;
        }

        @Override
        public boolean org$apache$spark$Partition$$super$equals(Object arg0) {
            return false;
        }
    }
}
