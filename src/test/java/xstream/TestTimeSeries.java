package xstream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Test;

import oracle.kv.KVStore;
import oracle.kv.table.RecordDef;
import xstream.Event;
import xstream.ReadableTimeSeries;
import xstream.TimeSeries;
import xstream.TimeSeriesBuilder;
import xstream.WritableTimeSeries;
import xstream.util.NoSQLURL;
import xstream.util.Sequence;
import xstream.util.SequenceBuilder;

public class TestTimeSeries {
    static NoSQLURL storeURL = new NoSQLURL("nosql://localhost:5000/kvstore/");
    @Test
    public void testCreate() {
        TimeSeries series = createSeries(storeURL);
        validateSeries(series, 0, 1, 
                TimeSeries.UNDEFINED_TIMESTAMP, 
                TimeSeries.UNDEFINED_TIMESTAMP);
    }
    
    @Test
    public void testOpenForRead() {
        TimeSeries series = createSeries(storeURL);
        TimeSeries series2 = openForRead(series);

        validateSeries(series2, 0, 1, TimeSeries.UNDEFINED_TIMESTAMP, 
                TimeSeries.UNDEFINED_TIMESTAMP);

    }
    
    @Test
    public void testOpenForWrite() {
        TimeSeries series = createSeries(storeURL);
        TimeSeries series2 = openForWrite(series);

        validateSeries(series2, 0, 1, TimeSeries.UNDEFINED_TIMESTAMP, 
                TimeSeries.UNDEFINED_TIMESTAMP);
    }

    
    
    @Test
    public void testSeriesMustBeCreatedWithFieldDefinition() {
        String seriesName = "Series" + System.currentTimeMillis();
        
        try {
            new TimeSeriesBuilder()
                .withSeriesURL(storeURL + seriesName)
                .create();
                fail("expecetd to fail to create without field definitions");
        } catch (IllegalArgumentException ex) {
            
        }
        
    }
    
     
    @Test
    public void testSingleSequeneIsMonotonic() {
        KVStore store = storeURL.openStore();
        Sequence seq = new SequenceBuilder()
                .withStore(store)
                .withName("testsequence")
                .withIncrement(10)
                .build();
        long prev = -1;
        for (int i = 0; i < 50; i++) {
            long v = seq.next();
            Assert.assertTrue(v > prev);
            prev = v;
        }
    }
    
    @Test
    public void testConcurrentSequeneIsMonotonic() {
        final KVStore store = storeURL.openStore();
        final AtomicLong previous = new AtomicLong(-1);
        int N = 10;
        ExecutorService threadPool = 
        Executors.newFixedThreadPool(N);
        for (int i = 0; i < N; i++) {
            
        Runnable client = new Runnable() {
            Sequence seq;
            {
                seq = new SequenceBuilder()
                        .withStore(store)
                        .withName("testsequence")
                        .withIncrement(10)
                        .build();
            }
            @Override
            public void run() {
                for (int i = 0; i < 50; i++) {
                    long v = seq.next();
                    Assert.assertTrue(v > previous.get());
                    previous.set(v);
                }
            }
        };
        threadPool.submit(client);
        }
        
        threadPool.shutdown();
        try {
            threadPool.awaitTermination(N, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            fail();
        }
    }

    @Test
    public void testClosedSeries() {
        String seriesName = "WritableSeries" + System.currentTimeMillis();
        String[] fieldDefs = {"x DOUBLE", "y DOUBLE"};
        WritableTimeSeries series = new TimeSeriesBuilder()
                .withSeriesURL(storeURL + seriesName)
                .withFieldDefinitions(fieldDefs)
                .create();
        series.write(10, new Object[]{10,20});
        series.close();
        assertTrue(series.isClosed());
        try {
            
            series.write(11, new Object[]{10,20});
            fail("expected to fail on write to closed series");
        } catch (IllegalStateException ex) {
            
        }
    }
    


    @Test
    public void tetsEventDefinitionHasTimestamp() {
        String seriesName = "TestTimeSeries" + System.currentTimeMillis();
        String[] fieldDefs = {"x DOUBLE", "y DOUBLE"};
        WritableTimeSeries series = new TimeSeriesBuilder()
                .withSeriesURL(storeURL + seriesName)
                .withFieldDefinitions(fieldDefs)
                .create();
        RecordDef eventDef = series.getEventDefinition().asRecordDef();
        
        Assert.assertEquals(fieldDefs.length+1, eventDef.getNumFields());
        Assert.assertEquals(TimeSeriesSchema.TIMESTAMP.getName(), 
                eventDef.getFieldName(0));
        
    }
    
    @Test
    public void eventMustBeLaterThanLatestEvent() {
        String seriesName = "TestTimeSeries" + System.currentTimeMillis();
        String[] fieldDefs = {"x DOUBLE", "y DOUBLE"};
        WritableTimeSeries series = new TimeSeriesBuilder()
                .withSeriesURL(storeURL + seriesName)
                .withFieldDefinitions(fieldDefs)
                .create();
        
        Map<String,Object> eventData = new HashMap<String,Object>();
        long ts = System.currentTimeMillis();
        eventData.put("x", 10.0); eventData.put("y", 20.0);
        series.write(ts, eventData);
        
        series.write(ts+1, eventData);
        
        try {
            series.write(ts, eventData);
            Assert.fail("expected to fail to write  event earlier than latest event");
        } catch (IllegalStateException ex) {
            
        }
        
        
        
    }

    @Test
    public void testReadWrite() {
        String seriesName = "RW" + System.currentTimeMillis();
        String[] fieldDefs = {"x INTEGER"};
        int L = 10;
        WritableTimeSeries wseries = new TimeSeriesBuilder()
                .withSeriesURL(storeURL + seriesName)
                .withFieldDefinitions(fieldDefs)
                .withSlotEventLimit(L)
                .create();
        int N = L*2+7;
        assertTrue(N/L > 0);   // at least one slot is used
        assertTrue(N%L != 0);  // some slots are not full
        for (int i = 0; i < N; i++) {
            wseries.write(i, new Object[] {i});
        }
        wseries.close();
        
        validateSeries(wseries, N, N/L+1, 0, N-1);
        
        ReadableTimeSeries rseries = new TimeSeriesBuilder()
                .withSeriesURL(storeURL + seriesName)
                .openForRead();
        Iterator<Event> events = rseries.read();
        int i = 0;
        while (events.hasNext()) {
            Event e = events.next();
            assertEquals(i, e.getTimestamp());
            assertEquals(i, e.get("x"));
            i++;
        }
        assertEquals(i, N);
        
    }

    @Test
    public void testAddEvent() {
        String seriesName = "TestAddTimeSeries" + System.currentTimeMillis();
        String[] fieldDefs = {"x DOUBLE", "y DOUBLE"};

        WritableTimeSeries series = new TimeSeriesBuilder()
                .withSeriesURL(storeURL + seriesName)
                .withFieldDefinitions(fieldDefs)
                .create();

        int initialSize = series.size();
        
        
        long startTime = System.currentTimeMillis();
        int N = 2000;
        Map<String, Object> data = new HashMap<String, Object>();
        for (int i = 0; i <  N; i++) {
            data.put("x", 10.0 + i); data.put("y", 20.0 + i);
            series.write(startTime+i, data);
        }
        int finalSize = initialSize + N;
        Assert.assertEquals(finalSize, series.size());
        series.close();

        assertEquals(startTime + N - 1, series.getTimeRange().getEndTime());

        ReadableTimeSeries series2 = new TimeSeriesBuilder()
                .withSeriesURL(storeURL + seriesName)
                .openForRead();

        long start = System.currentTimeMillis();
        
        Iterator<Event> iterator = series2.read();

        assertNotNull(iterator);

        int i = 0;
        while (iterator.hasNext()) {
            i++;
            Event event = iterator.next();
            assertNotNull(event);
            if (i <= initialSize) continue;
//            int n = (i-initialSize);
//            assertEquals( "timestamp mismatch at event index " + i, 
//                    startTime + n, event.getTimestamp());
//            assertEquals(10.0 + n, event.get("x"));
//            assertEquals(20.0 + n, event.get("y"));
        }
        assertFalse(iterator.hasNext());
        
        long elapsedTime = System.currentTimeMillis() - start;
        long readingRate = (i == 0) ? 0 : (i*1000/elapsedTime);
            
        System.err.println("read " + i + " events in " + elapsedTime + " ms"
                + " reading rate=" + readingRate + " events/sec");
        
        assertTrue(readingRate > 0);

    }

    @Test
    public void testQueryTimeseries() {
        String seriesName = "QueryTimeSeries" + System.currentTimeMillis();

        WritableTimeSeries series = 
        new TimeSeriesBuilder().withSeriesURL(storeURL+seriesName)
        .withFieldDefinitions("x DOUBLE", "y DOUBLE")
        .create();
        int N = 3;
        long t0 = System.currentTimeMillis();
        Map<String, Object> data = new HashMap<String, Object>();
        for (int i = 0; i < N; i++) {
            data.put("x", 10.0 + i);
            data.put("y", 20.0 + i);
            series.write(t0 + i, data);
        }
        series.close();

    }

    //@Test
    public void testLongTimeseries() {
        String seriesName = "LongTimeSeries";
        int[] slot_sizes = new int[] {256, 1024, 1024*4, 1024*10, 1024*100};
        int N = 10000;
        for (int i = 0; i < slot_sizes.length; i++) {
            long t = measureIngestRate(storeURL+seriesName+"_"+i, slot_sizes[i], N);
            System.err.println("wrote " + N + " events in " + t + " ms"
                    + " ingest rate " + (1000 * N)/ t + " events/sec. Slot Size " + slot_sizes[i]);
        }
    }
    
    @Test
    public void testIngestionRate() {
        int N = 100*100;
        long t = measureIngestRate(storeURL+"IngestTest"+System.currentTimeMillis(), 
                TimeSeriesSchema.DEFAULT_EVENT_PER_SLOT_LIMIT, N);
        System.err.println("ingest rate " + 1000 * N / t + " events/sec slot size ");
    }
    
    long measureIngestRate(String seriesURL, int M, int N) {
        WritableTimeSeries series = 
        new TimeSeriesBuilder()
            .withSeriesURL(seriesURL)
            .withFieldDefinitions("x DOUBLE", "y DOUBLE")
            .withSlotEventLimit(M)
            .create();

        long start = System.currentTimeMillis();
        Map<String, Object> data = new HashMap<String, Object>();
        for (int t = 0; t < N; t++) {
            data.put("x", 10.0 + t);data.put("y", 20.0 + t);
            series.write(t, data);
        }
        long stop = System.currentTimeMillis();
        series.close();
        return stop - start;
    }
    
    @Test
    public void testValidURL() throws Exception {
        NoSQLURL url = new NoSQLURL("nosql://localhost/db/series1");
        
        Assert.assertEquals("nosql", url.getProtocol());
        Assert.assertEquals("localhost", url.getHost());
        Assert.assertEquals("db", url.getStoreName());
        Assert.assertEquals("series1", url.getSeriesName());
        
        url = new NoSQLURL("nosql://1.2.3.4/kvstore/series2");
        
        Assert.assertEquals("nosql", url.getProtocol());
        Assert.assertEquals("1.2.3.4", url.getHost());
        Assert.assertEquals("kvstore", url.getStoreName());
        Assert.assertEquals("series2", url.getSeriesName());

    }
    protected TimeSeries createSeries(NoSQLURL url) {
        String seriesName = "Test" + System.currentTimeMillis()%1000;
        return createSeries(url.toString() + seriesName);
    }

    /**
     * Create a new series with unique name.
     * @param url
     * @return
     */
    protected TimeSeries createSeries(String url) {
        String[] fieldDefs = {"x DOUBLE", "y DOUBLE"};
        WritableTimeSeries series = new TimeSeriesBuilder()
                .withSeriesURL(url)
                .withFieldDefinitions(fieldDefs)
                .create();
        // a series must be closed for it is to be persistent
        series.close();
        return series;
    }
    
    ReadableTimeSeries openForRead(TimeSeries series) {
        assertNotNull(series);
        assertTrue(series.isClosed());
        return new TimeSeriesBuilder()
            .withSeriesURL(series.getURL().toString())
            .openForRead();
    }
    
    WritableTimeSeries openForWrite(TimeSeries series) {
        assertNotNull(series);
        assertTrue(series.isClosed());
        return new TimeSeriesBuilder()
            .withSeriesURL(series.getURL().toString())
            .openForWrite();
    }
    

    protected void validateSeries(TimeSeries series, int eventCount,
            int slotCount, long t0, long tN) {
        assertNotNull("series is null", series);
        assertEquals("series event count mismatch", eventCount, series.getEventCount());
        assertEquals("slot count mismatch", slotCount, series.getSlotCount());
        TimeRange timeRange = series.getTimeRange();
        assertNotNull(timeRange);
        assertEquals(t0, timeRange.getStartTime());
        assertEquals(tN, timeRange.getEndTime());
        
    }

 
}
