package xstream;

import static xstream.TimeSeriesSchema.EVENT_COUNT;
import static xstream.TimeSeriesSchema.SLOT_COUNT;
import static xstream.TimeSeriesSchema.TIMESTAMP;
import static xstream.TimeSeriesSchema.TIMESTAMP_FIRST;
import static xstream.TimeSeriesSchema.TIMESTAMP_LAST;

import java.util.HashMap;
import java.util.Map;

import oracle.kv.table.RecordDef;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import xstream.util.Assert;
import xstream.util.Sequence;
import xstream.util.SequenceBuilder;

/**
 * A timeseries to write new events.
 * The metaphor for a WritableTimeSeries is an output file. 
 * The main difference is while a byte is the atomic element to be written
 * to a file, an {@link Event} is the atomic element to be written to a
 * {@link WritableTimeSeries}.
 * An event can be written by {@link #write(long, Map)}
 * to the series.
 * <code>
 *      Event e = newEvent(...);  
 *      addEvent(...)
 * </code>
 * or two steps can be collapsed into one     
 * <code>
 *      write(...);
 * </code>
 * 
 * The timestamp for events must be monotonically increasing.
 * 
 * @author pinaki poddar
 *
 */
public class WritableTimeSeries extends TimeSeries {
    Sequence _slotSequence;
    /**
     * Creates a timeseries.  
     * @param seriesURL
     * @param fieldDefs
     */
    WritableTimeSeries(String seriesURL, Row row, Table table) {
        super(seriesURL, row, table, false);
        
        _slotSequence = new SequenceBuilder()
                .withStore(_store)
                .withName(getName()+ "_slots")
                .withIncrement(40)
                .build();
        
        
        int slotIdx = (int)_slotSequence.current();
        
        System.err.println(this + " current slot " + slotIdx);
        Slot slot = findSlot(slotIdx, false);
        if (slot == null) {
            System.err.println(this + " current slot " + slotIdx + " not found"
                    + " creating empty slot");
            slot = emptySlot(slotIdx);
        }
        setCurrentSlot(slot);
    }
    
    

    
    /**
     * Writes event to time series.
     * @param ts the timestamp for the event. The timestamp must be later
     * than the latest timestamp.
     * @param values map of values of the event properties. A key in the given
     * map should correspond to a {@link EventMetadata#getPropertyNames()
     * defined property} of event. 
     * If a property of the given event is not defined, the behavior vary from
     * i) warning to ii) runtime exception iii) to failure.
     * @return an event that has been added to this series.
     */
    // TODO: missing value of write
    public Event write(long ts, Map<String, Object> values) {
        Assert.assertFalse(isClosed(), new IllegalStateException(
                "cannot insert event to " + getName() + " because it is closed"));
        Event e = newEvent(ts, values);
        
        getSlotToWrite().insertEvent(e);
        return e;
    }
    
    /**
     * Writes event to time series.
     * @param ts the timestamp for the event. The timestamp must be later
     * than the latest timestamp.
     * @param values the values of the event properties. The values must
     * be in same order as they are available in {@link EventMetadata#getPropertyNames()}.
     * @return the event that has been created implicitly
     */
    public Event write(long ts,Object[] values) {
        Assert.assertFalse(isClosed(), new IllegalStateException(
                "cannot insert event to " + getName() + " because " + this + " is closed"));
        Event e = newEvent(ts, values);
        getSlotToWrite().insertEvent(e);
        return e;
    }
    
    /**
     * Creates and populates a new event.
     * 
     * @param ts timestamp of the event
     * @param values values of the event indexed by property name.
     * 
     * @return an event
     */
    Event newEvent(long ts, Map<String,Object> values) {
        values.put(TIMESTAMP.getName(), ts);
        Event event = getEventDefinition().newEvent(values);
        return event;
    }

    /**
     * Creates and populates a new event.
     * 
     * @param ts timestamp of the event
     * @param values values of the event in order of property. The order
     * is same as the order of {@link EventMetadata#getPropertyNames()}.
     * 
     * @return an event
     */
    Event newEvent(long ts, Object[] array) {
        Map<String, Object> values = new HashMap<String, Object>();
        RecordDef recordDef = getEventDefinition().asRecordDef();
        for (int i = 1; i < recordDef.getNumFields(); i++) {
            values.put(recordDef.getFieldName(i), array[i-1]);
        }
        return newEvent(ts, values);
     }
    
    /**
     * Gets the last slot. 
     * @return the last slot. 
     */
    
    protected Slot getSlotToWrite() {
        Slot current = getCurrentSlot();
        // TODO: read database for the last slot
        if (current == null || current.isFull()) {
//            TimeSeriesRegistry registry =  TimeSeriesRegistry.getInstance(getStore());
            
            if (current != null) this.flush(true);
            int nextIdx = (int)_slotSequence.next();
            current = emptySlot(nextIdx);
            setCurrentSlot(current);
            return current;
        }
        return current;
    }

     
     public void flush(boolean updateAggregate) {
         Slot slot = getCurrentSlot();
         slot.flush();
         if (updateAggregate) {
             SLOT_COUNT.set(_metadata, getSlotCount()+1);
             EVENT_COUNT.set(_metadata, getEventCount() + slot.getEventCount());
             changeTimerange(slot.getTimeRange());
         }
         _store.getTableAPI().put(_metadata, null, null);
     }

     public void close() {
         flush(true);
        super.close();
         
     }
     
     void changeTimerange(TimeRange range) {
         System.err.println("series timerange: " + getTimeRange());
         System.err.println("slot timerange  : " + range);
         TimeRange newRange = getTimeRange().add(range);
         TIMESTAMP_FIRST.set(_metadata, newRange.getStartTime());
         TIMESTAMP_LAST.set(_metadata, newRange.getEndTime());
         System.err.println("series timerange: " + getTimeRange() + " (After)");
     }

}
