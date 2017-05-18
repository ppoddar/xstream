package xstream;


import static xstream.TimeSeriesSchema.EVENT_COUNT;
import static xstream.TimeSeriesSchema.EVENT_LIMIT;
import static xstream.TimeSeriesSchema.READ_SLOT_INDEX;
import static xstream.TimeSeriesSchema.SERIES_NAME;
import static xstream.TimeSeriesSchema.TIMESTAMP_FIRST;
import static xstream.TimeSeriesSchema.TIMESTAMP_LAST;
import static xstream.TimeSeriesSchema.WRITE_SLOT_INDEX;
import static xstream.TimeSeriesSchema.SLOT_COUNT;

import oracle.kv.KVStore;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import xstream.util.Assert;
import xstream.util.NoSQLURL;
import xstream.util.RMWLock;
import xstream.util.StringHelper;
import xstream.util.Updater;
/**
 * A builder for timeseries.
 * 
 * @author pinaki poddar
 *
 */
public class TimeSeriesBuilder {
    private String _seriesURL;
    private String[] _fieldDefs;
    private int _slotEventLimit = -1;
    
    /**
     * sets URL for the timeseries.
     * @param url URL defines name and location timeseries The name follows
     * a typical HTTP URL  syntax where the path specify store name and
     * series name is consecutive segments. For example,
     * <code>nosql://localhost:5000/mystore/myseries</code>
     * @return this same builder.
     */
    public TimeSeriesBuilder withSeriesURL(String url) {
        _seriesURL = url;
        return this;
    }
    
    /**
     * Declares the event fields of a timeseries. Each field of an event
     * is described in same syntax of a database column. At minimum, a
     * field is described as <code>name db-type</code> where <code>name</code> 
     * is the name of an event property and <code>db-type</code> is moniker
     * of a database type such as <code>integer</code>, <code>string</code>
     * etc.
     * <br>
     * Field definitions are mandatory when a {@link #create() new series is created}.
     * They are optional to {@link #openForRead()} or {@link #openForWrite()}
     * an existing timeseries. If they are specified then they are used
     * to match existing definition.
     * 
     * @param defs one or more field definitions. Must be specified for creating
     * a series. Can be null for opening existing series.
     * 
     * @return this same builder.
     */
    public TimeSeriesBuilder withFieldDefinitions(String... defs) {
        _fieldDefs = defs;
        return this;
    }
    
    /**
     * Declares number of events in a time slot in a time series.
     * Each time slot in a time series carries same number of events,
     * though each event need not contain same fields.
     * 
     * Slot event limit is used to {@link #create() create} a timeseries.
     * The slot event limit, if specified with a positive number, is used 
     * to verify the value with while {@link #openForRead()} or {@link #openForWrite()}
     * an existing timeseries.
     * 
     * @param l number of events per slot. Must be greater than zero for
     * creating a series. Can be less than zero to open existing series.
     * 
     * @return this same builder.
     */
    public TimeSeriesBuilder withSlotEventLimit(int l) {
        Assert.assertTrue(l > 0, new IllegalArgumentException());
        _slotEventLimit = l;
        return this;
    }
    
    /**
     * Opens an existing timeseries to write more events. The events are added at
     * the end of the series.
     * @return a series for new events be written at the end.
     */
    public synchronized WritableTimeSeries openForWrite() {
        NoSQLURL url = new NoSQLURL(_seriesURL);
        KVStore store = url.openStore();
        TimeSeriesRegistry registry = TimeSeriesRegistry.getInstance(store);
        String seriesName = url.getSeriesName();
        Table seriesTable = registry.getSeriesTable(seriesName, true);
        Row row = registry.getSeriesRow(seriesName, true);
        
        WritableTimeSeries ws = new WritableTimeSeries(_seriesURL, row, seriesTable);
        // match(ws)
        return ws;
    }
    
    /**
     * Creates a writable timeseries.
     * @return a writable timeseries.
     */
    public synchronized WritableTimeSeries create() {
        NoSQLURL seriesUrl = new NoSQLURL(_seriesURL);
        final KVStore store = seriesUrl.openStore();
        final String seriesName = seriesUrl.getSeriesName();
        TimeSeriesRegistry registry = TimeSeriesRegistry.getInstance(store);
        Row seriesRow = registry.getSeriesRow(seriesName, false);
        if (seriesRow != null) {
            throw new RuntimeException(seriesUrl + " already exists");
        }
        WritableTimeSeries series = null;
            Row template = registry.getRegistryTable().createRow();
            SERIES_NAME.set(template, seriesName);
            SLOT_COUNT.set(template, 0);
            EVENT_COUNT.set(template, 0L);
            EVENT_LIMIT.set(template, 
                    _slotEventLimit < 0 ? TimeSeriesSchema.DEFAULT_EVENT_PER_SLOT_LIMIT : _slotEventLimit);
            TIMESTAMP_FIRST.set(template, TimeSeries.UNDEFINED_TIMESTAMP);
            TIMESTAMP_LAST.set(template, TimeSeries.UNDEFINED_TIMESTAMP);
            READ_SLOT_INDEX.set(template, 0);
            WRITE_SLOT_INDEX.set(template, 0);
            new RMWLock().update(store, 
                    template, new Updater() {
                        @Override
                        public void update(Row row) {
                        }
                    });
            Table seriesTable = registry.defineSeriesTable(seriesName,
                    StringHelper.join(',', _fieldDefs).toString());
            series = new WritableTimeSeries(_seriesURL, template, seriesTable);
        
        // match (ws)
        series.flush(false);
        return series;
    }

    public synchronized TimeSeries getOrCreate(boolean forRead) {
        NoSQLURL seriesUrl = new NoSQLURL(_seriesURL);
        final KVStore store = seriesUrl.openStore();
        final String seriesName = seriesUrl.getSeriesName();
        TimeSeriesRegistry registry = TimeSeriesRegistry.getInstance(store);
        Table seriesTable = registry.getSeriesTable(seriesName, false);
        if (seriesTable ==  null) {
            return create();
        } else {
            return forRead ? openForRead() : openForWrite();
        }
        
    }

    /**
     * Opens a timeseries to read.
     * 
     * @return a readable timeseries.
     */
    public synchronized ReadableTimeSeries openForRead() {
        NoSQLURL seriesURL = new NoSQLURL(_seriesURL);
        KVStore store = seriesURL.openStore();
        String seriesName = seriesURL.getSeriesName();
        TimeSeriesRegistry registry = TimeSeriesRegistry.getInstance(store);
        Table seriesTable = registry.getSeriesTable(seriesName, true);
        Row row = registry.getSeriesRow(seriesName, true);
        ReadableTimeSeries series = 
                new ReadableTimeSeries(_seriesURL, row, seriesTable);
        //match(series);
        return series;
    }
    
     //private static int MAX_TRIAL = 20;

    
    


}

//public synchronized ReadableTimeSeries openOrCreateForRead() {
//prepareSeries(null);
//ReadableTimeSeries rs = new ReadableTimeSeries(_seriesURL, _slotEventLimit, _fieldDefs);
//return configueSeries(rs);
//}

//public synchronized WritableTimeSeries openOrCreateForWrite() {
//prepareSeries(null);
//WritableTimeSeries ws = new WritableTimeSeries(_seriesURL, _slotEventLimit, _fieldDefs);
//return configueSeries(ws);
//}

