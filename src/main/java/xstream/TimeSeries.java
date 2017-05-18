package xstream;

import static xstream.TimeSeriesSchema.*;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.Consistency;
import oracle.kv.KVStore;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.RecordDef;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableIterator;
import xstream.util.Assert;
import xstream.util.NoSQLURL;

/**
 * An abstract implementation of timeseries using partitioned time slots
 * appropriate for storage and cluster computing framework.
 * 
 * @author pinaki poddar
 *
 */
public abstract class TimeSeries implements Externalizable {
    private NoSQLURL _uri;
    private transient final String _name;
    protected transient final KVStore _store; // store where series data is stored
    private transient final Table _table; // table where series data is stored
    protected transient final Row _metadata; // row that describes the series
    private transient final  EventMetadata _eventMetadata; // definition of event
    // current slot where event will be read from or written to
    private transient Slot _currentSlot;
    private transient boolean _isClosed;
    
    
    public static long UNDEFINED_TIMESTAMP = -1;
    
    public static final Logger _logger = Logger.getLogger(TimeSeries.class.getName());
    
    /**
     * Creates a new timeseries of given name from a row. The given row
     * contains essential metadata about the series, but not the events.
     * <br>
     * The table that contains the events in slots must be defined prior
     * to create a series.
     * The table column for events define the event definition.
     * 
     * @param uri
     *            URL of the timeseries. An URL encodes the database where 
     *            the series is persisted and the series name
     * @param metadata
     *   a row containing essential metadata such as number of slots, 
     *   number of events, maximum number of events in a slot.
     *   The record definition of event is sourced from the table.
     *   
     */
    TimeSeries(String uri, Row metadata, Table table, boolean forRead) {
        Assert.assertNotNull(uri, "uri for series is  null");
        Assert.assertNotNull(metadata, "metadata for series is  null");
        Assert.assertNotNull(table, "table  for series " + uri + " does not exist");
        
        _uri = new NoSQLURL(uri);
        _name = _uri.getSeriesName();
        _store = _uri.openStore();
        _metadata = metadata;
        _table = table;
       _eventMetadata = new EventMetadata(createEventDefinition());
       
    }
    
    /**
     * Creates an empty array of event element. The structure of event
     * varies between series. 
     */
    ArrayValue createEventsArray() {
        return _table.getField(EVENTS.getName()).createArray();
    }
    
    RecordDef createEventDefinition() {
        FieldDef eventsArray = _table.getField(EVENTS.getName());
        RecordValue eventRecord = eventsArray.createArray().getDefinition()
                .getElement().createRecord();
        if (!TIMESTAMP.existsIn(eventRecord) && !isUniform()) {
            eventRecord.put(TIMESTAMP.getName(), 0L);
        }
        RecordDef eventDef = eventRecord.getDefinition();
        return eventDef;
    }
    
    /**
     * Creates a  slot with given index.
     * 
     * @param idx index of the slot
     * @return a slot with given index and other default values.
     */
    protected Slot emptySlot(int idx) {
        Row slot = _table.createRow();
        SLOT_INDEX.set(slot, idx);
        EVENT_COUNT.set(slot, 0L);
        EVENT_LIMIT.set(slot, getSlotEventLimit());
        FIRST_EVENT_INDEX.set(slot, -1);
        LAST_EVENT_INDEX.set(slot, -1);
        TIMESTAMP_FIRST.set(slot, TimeSeries.UNDEFINED_TIMESTAMP);
        TIMESTAMP_LAST.set(slot, TimeSeries.UNDEFINED_TIMESTAMP);

        return new Slot(this, slot);
    }

    
    boolean isUniform() {
        return false;
    }
    
    public final NoSQLURL getURL() {
        return _uri;
    }
    
    /**
     * gets the store that stores event data of this series
     * @return
     */
    final KVStore getStore() {
        return _store;
    }

    
    /**
     * gets the table that stores event data of this series
     * @return
     */
    final Table getTable() {
        return _table;
    }
    
    /**
     * Gets definition of events. An event is a composite structure
     * of one of more simple properties of type string, numeric, boolean
     * or timestamp.
     * 
     * @return definition of an {@link Event}.
     */
    public final EventMetadata getEventDefinition() {
        return _eventMetadata;
    }
    
    /**
     * Gets total number of events in this series.
     * Total number of events in a series is sum of number of events
     * in each slot.
     * 
     * @return a positive number including zero. 
     */
    public final long getEventCount() {
        return EVENT_COUNT.getLong(_metadata);
    
    }
    
    /**
     * Gets name of this series. A name uniquely identifies a series in a database. 
     * @return name of this series
     */
    public final String getName() {
        return _name;
    }
    

    /**
     * Returns number of events per slot.
     * 
     * @return number of events per slot.
     */
    public final int getSlotEventLimit() {
        return EVENT_LIMIT.getInt(_metadata);
    }
    
    /**
     * Returns a slot at given index, or null if no such slot exists and
     * mustExist is set to false. Performs a query if index of current slot is
     * not same as the given index.
     * 
     * @param idx
     *            an index to the slot
     * @param mustExist
     *            if true and if slot does not exist, raises an exception
     * @return the current slot if it has the same index.
     */
    protected Slot findSlot(int idx, boolean mustExist) {
        if (_currentSlot != null && _currentSlot.getIndex() == idx) {
            return _currentSlot;
        }
        String sql = "SELECT *  FROM " + _table.getName() 
                + " WHERE " + SLOT_INDEX + "=" + idx;
        TableIterator<RecordValue> iterator = query(sql, Consistency.NONE_REQUIRED);
        if (iterator.hasNext()) {
            RecordValue record = iterator.next();
            Slot slot = new Slot(this, asRow(record));
            slot.previous(_currentSlot);
            return slot;
        }
        if (mustExist) {
            throw new ArrayIndexOutOfBoundsException("block index " + idx + " not present. SQL:" + sql);
        } else {
            return null;
        }
    }

    /**
     * Finds a slot at given 0-based index.
     * 
     * @param idx
     *            index of the slot.
     * 
     * @return null if no such slot exists.
     * 
     */
    protected Slot findSlot(int idx) {
        return findSlot(idx, false);
    }

    /**
     * Returns total number of time slots.
     * 
     * @return total number of slots
     */
    public final int getSlotCount() {
        return SLOT_COUNT.getInt(_metadata);
    }
    
    protected Slot getCurrentSlot() {
        _logger.log(Level.ALL, "setting current slot " + _currentSlot);
        return _currentSlot;
    }
    
    protected void setCurrentSlot(Slot slot) {
        _currentSlot = slot;
    }

    /**
     * Returns total no. of events in this stream.
     * 
     * @return total number of events
     */
    public int size() {
        String sql = "SELECT " 
                + SLOT_INDEX
                + "," + FIRST_EVENT_INDEX
                + "," + LAST_EVENT_INDEX
                + " FROM " + getName();
        TableIterator<RecordValue> rs = query(sql, Consistency.NONE_REQUIRED);
        int size = 0;
        while (rs.hasNext()) {
            RecordValue row = rs.next();
            int idx = SLOT_INDEX.getInt(row);
            if (_currentSlot.getIndex() == idx) continue;
            int first = FIRST_EVENT_INDEX.getInt(row);
            int last = LAST_EVENT_INDEX.getInt(row);
            size += Math.max(0, last-first);
        }
        return size + _currentSlot.size();
    }

    TableIterator<RecordValue> query(String sql, Consistency consistency) {
        ExecuteOptions options = null;
        if (consistency != null) {
            options = new ExecuteOptions();
            options.setConsistency(Consistency.ABSOLUTE);
        }   
        return query(sql, options);

    }

    RecordValue querySingle(String sql) {
        _logger.log(Level.FINE, "Query(single):" + sql);
        TableIterator<RecordValue> rs = query(sql, (ExecuteOptions)null);
        if (!rs.hasNext()) {
            throw new RuntimeException("Query " + sql + " produced no result" + ". Expected a single result");
        }
        RecordValue result = rs.next();
        if (rs.hasNext()) {
            throw new RuntimeException("Query " + sql + " produced multiple result" + ". Expected a single result");
        }
        return result;
    }
    
    private TableIterator<RecordValue> query(String sql,ExecuteOptions options) {
        _logger.log(Level.FINE, "Query:" + sql);
//        System.err.println("Query:" + sql);
        try {
            if (options != null) {
                return getStore().executeSync(sql, options).iterator();
            } else {
                return getStore().executeSync(sql).iterator();
            }
        } catch (Exception ex) {
            System.err.println(ex);
            ex.printStackTrace();
        }
        return null;
        
    }
    
    /**
     * flushes accumulated changes.
     * 
     * @param updateAggregate if true updates aggregate values
     * in parent timeseries
     */
    public void flush(boolean updateAggregate) {
        Slot slot = getCurrentSlot();
        slot.flush();
    }

    public void close() {
        if (!isClosed()) {
            _logger.log(Level.FINEST, "Closing " + this);
        }
        _isClosed = true;
        _store.close();
        
    }
    
    public boolean isClosed() {
        return _isClosed;
    }
    
    public String toString() {
        return _uri.toString();
    }

    
//    public TimeSeries window(long tMin, long tMax) {
//        TableIterator<RecordValue> results =
//        query("SELECT " + BLOCK_INDEX + " FROM " + getName()
//        + " WHERE " + TIMESTAMP_START + ">=" + tMin
//        + " ORDER BY " + TIMESTAMP_START);
//        
//        int s1 = results.next().get(0).asInteger().get();
//        
//        results = 
//        query("SELECT " + BLOCK_INDEX + " FROM " + getName()
//        + " WHERE " + TIMESTAMP_LAST + "<=" + tMax
//        + " ORDER BY " + TIMESTAMP_LAST + " DESC");
//        
//        int s2 = results.next().get(0).asInteger().get();
//        
//        PersistentTimeSeries w = this.copy();
//        w._currentBlock = this.findSlot(s2, true).copy();
//        w._firstBlockIndex = s1;
//        w._lastBlockIndex = s2;
//        
//        return w;
//    }


    public final TimeRange getTimeRange() {
        long startTime = TIMESTAMP_FIRST.getLong(_metadata);
        long stopTime = TIMESTAMP_LAST.getLong(_metadata);
        return new TimeRange(startTime, stopTime);

    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(_uri);
        
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, 
                                            ClassNotFoundException {
        _uri = (NoSQLURL)in.readObject();
        
    }
    

    protected Row asRow(RecordValue record) {
        if (Row.class.isInstance(record)) {
            return Row.class.cast(record);
        }
        Row row = _table.createRow();
        int N = record.getDefinition().getNumFields();
        for (int i = 0; i < N; i++) {
            row.put(i, record.get(i));
        }
        return row;
        
    }
    
    
    boolean _timeOrderStrict = true;
    boolean isTimeOrderStrict() {
        return _timeOrderStrict;
    }
    public void setTimeOrderStrict(boolean flag) {
        _timeOrderStrict = flag;
    }
    
    boolean _autoSortEvent = true;
    boolean isAutoSortEvent() {
        return _autoSortEvent;
    }
    public void setAutoSortEvent(boolean flag) {
        _autoSortEvent = flag;
    }

}
