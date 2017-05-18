package xstream;

import static xstream.TimeSeriesSchema.EVENTS;
import static xstream.TimeSeriesSchema.EVENT_COUNT;
import static xstream.TimeSeriesSchema.EVENT_LIMIT;
import static xstream.TimeSeriesSchema.FIRST_EVENT_INDEX;
import static xstream.TimeSeriesSchema.LAST_EVENT_INDEX;
import static xstream.TimeSeriesSchema.NEXT_SLOT;
import static xstream.TimeSeriesSchema.PREV_SLOT;
import static xstream.TimeSeriesSchema.SLOT_FIELDS;
import static xstream.TimeSeriesSchema.SLOT_INDEX;
import static xstream.TimeSeriesSchema.TIMESTAMP_FIRST;
import static xstream.TimeSeriesSchema.TIMESTAMP_LAST;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.logging.Level;

import oracle.kv.Consistency;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldValue;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.TableIterator;
import xstream.util.Assert;
import xstream.util.RMWLock;
import xstream.util.Updater;

/**
 * A time slot contains a series of events whose timestamps form a monotonic
 * sequence and metadata about the timeslot such as time range of the events
 * and slot index. <br>
 * Maximum number of events in a slot is fixed, though events can
 *  vary in structure and storage. <br>
 * A slot is a row in a  timeseries
 * {@link TimeSeries#getTable() database table}.
 * 
 * @author pinaki poddar
 *
 */
public class Slot implements Comparable<Slot> {
    private final TimeSeries _timeseries;
    private final Row _row;
    private final List<Event> _events; // actual events
    

    /**
     * Create a slot from given row. The event data is fetched.
     * 
     * @param record
     *            a row with specific structure. The row must contain all meta
     *            data fields and event data as an array of record values.
     * 
     */
    Slot(TimeSeries series, Row row) {
       Assert.assertNotNull(row, "can not initaize slot from null record");
       for (Field f : SLOT_FIELDS ) {
           f.assertExistsIn(row);
       }
        _timeseries = series;
        _row = row;
        _events = fetchEvents(row.get(EVENTS.getName()));
    }
    
    /**
     * Gets the index of the first readable event.
     * The index refers to the event array. There may be events in a slot 
     * that are not readable.
     * 
     * @return If no event exists in this slot, returns a negative number.
     */
    public int getFirstEventIndex() {
        return FIRST_EVENT_INDEX.getInt(_row);
    }
    
    /**
     * Sets index of the first event.
     * The index refers to the event array.
     * 
     * @param idx 0-based index of the first event
     */
    public void setFirstEventIndex(int idx) {
        FIRST_EVENT_INDEX.set(_row, idx);
    }
    
    /**
     * Index of the last event.
     * @return If no event exists in this slot, returns -1.
     * 
     */
    public int getLastEventIndex() {
        return LAST_EVENT_INDEX.getInt(_row);
    }
    
    public void setLastEventIndex(int idx) {
        LAST_EVENT_INDEX.set(_row, idx);
    }
    
    /**
     * Gets the timestamp of the first event.
     * @return If no event exists in this slot, the timestamp is undefined.
     * 
     */
    public long getFirstEventTimestamp() {
        return TIMESTAMP_FIRST.getLong(_row);
    }
    public void setFirstEventTimestamp(long t) {
        TIMESTAMP_FIRST.set(_row, t);
    }
    
    /**
     * Gets the timestamp of the last event.
     * @return If no event exists in this slot, the timestamp is undefined.
     * 
     */
    public long getLastEventTimestamp() {
        return TIMESTAMP_LAST.getLong(_row);
    }
    
    public void setLastEventTimestamp(long t) {
        TIMESTAMP_LAST.set(_row, t);
    }
    

    /**
     * Adds an event to this slot. The event timestamp must be greater or equal
     * than last timestamp. If this is the first event, updates the start time.
     * Updates last timestamp.
     * 
     * @param event an event to be added
     */
    void insertEvent(Event event) {
        Assert.assertNotNull(event, new IllegalArgumentException());
        Assert.assertFalse(isFull(),new IllegalStateException(this + " is full"));
        
        ensureTemporalOrder(event, getLastEventTimestamp(), _events);

    }
    
    /**
     * ensures the given event is added in temporal order in the given
     * list of events. The given event may have timestamp earlier than
     * the last event of the given list of events. Then the given event
     * is not added at the end, but inserted  at a position to maintain
     * temporal order.
     * 
     * @param event
     * @param last
     * @param events
     * 
     * @return true if the given event is the highest timestamp.
     */
    void ensureTemporalOrder(Event event, long last, List<Event> events) {
        long t = event.getTimestamp();
        if (t < 0) {
            handleError("invalid event timestamp " + t 
            + ". Timestamp must be greater than equal to zero", true);
            return;
        }
        if (t >= last || last < 0) {
            events.add(event);
            if (getFirstEventIndex() < 0) {
                setFirstEventIndex(0);
                setFirstEventTimestamp(t);
            }
            setLastEventIndex(getFirstEventIndex() + _events.size());
            setLastEventTimestamp(t);
            return;
        }
        boolean strict = _timeseries.isTimeOrderStrict();
        boolean autoSort = _timeseries.isAutoSortEvent();
        handleError("event " + " timestamp " + t + " is earlier than last event  " 
                    + getLastEventTimestamp(), strict);
       if (strict) {
           return;
       } else if (autoSort) {
           if (t <= getFirstEventTimestamp()) {
               handleError("unordered event timestamp " 
                       + t + " can not be ordered in a slot starting at timestamp "
                       + getFirstEventTimestamp(), true);
           }
           TimeSeries._logger.log(Level.FINE, 
                   "sorting unordered event timestamp " + t
                   + " because it is earlier than last timestamp " + last);
           ListIterator<Event> iterator = events.listIterator(events.size()-1);
           while (iterator.hasPrevious()) {
               int idx = iterator.previousIndex();
               Event previous = iterator.previous();
               if (t >= previous.getTimestamp()) {
                   TimeSeries._logger.log(Level.FINE, "inserting unordered event at " + idx);
                   events.add(idx, event);
                   return;
               }
           }
           handleError("unexpected error. unordered event " + t
                   + " slot time starts at " + getFirstEventTimestamp(), true);
       } else {
            TimeSeries._logger.log(Level.WARNING, "unordered event " + t);
       }
        
    }

    /**
     * handles a logical or argument error by either throwing an exception
     * or printing a warning message.
     * 
     * @param msg
     */
    void handleError(String msg, boolean fatal) {
        if (fatal) {
            throw new IllegalArgumentException(msg);
        } 
        TimeSeries._logger.log(Level.FINE, msg);
        
    }

    TimeRange getTimeRange() {
        return new TimeRange(getFirstEventTimestamp(), 
                getLastEventTimestamp());
    }

    /**
     * Fetch events from database only if not fetched.
     * If the events are null in the database, an empty array is used in-memory
     * to mark that events have been fetched.
     * 
     * @param eventRecords a database record for a time slot as an array of events
     * @return empty list if database record is null. Otherwise, a list of Event
     * objects. Each element of given input array of record is converted
     * to a Event object.
     */
    private List<Event> fetchEvents(FieldValue eventRecords) {
        if (eventRecords == null || eventRecords.isNull()) {
            TimeSeries._logger.log(Level.FINE, "database event record is null for slot "); 
            return new LinkedList<Event>();
        }
        
        if (!eventRecords.isArray()) 
            throw new RuntimeException("events are not an array"
                + " database record type is " + eventRecords.getType());
        
        List<Event> events = new LinkedList<Event>();
        ArrayValue array = eventRecords.asArray();
        for (int i = 0; i < array.size(); i++) {
             FieldValue elementValue = array.get(i);
             Event event = _timeseries.getEventDefinition()
                     .newEvent(elementValue.asRecord());
             events.add(event);
        }
        return events;
    }
    

    /**
     * Returns index of this slot.
     * 
     * @return index of this slot.
     */
    public int getIndex() {
        return SLOT_INDEX.getInt(_row);
    }
    
    public int getNext() {
        return NEXT_SLOT.getInt(_row);
    }
    
    public int getPrev() {
        return PREV_SLOT.getInt(_row);
    }
    
    public void next(Slot next) {
        if (next == null) return;
        NEXT_SLOT.set(_row, next.getIndex());
    }

    
    public void previous(Slot prev) {
        if (prev == null) return;
        PREV_SLOT.set(_row, prev.getIndex());
        prev.next(this);
    }
    
    public boolean hasNext() {
        return NEXT_SLOT.existsIn(_row) &&
               NEXT_SLOT.getInt(_row) >= 0;
    }

    public boolean hasPrev() {
        return PREV_SLOT.existsIn(_row) &&
               PREV_SLOT.getInt(_row) >= 0;
    }

    /**
     * Returns number of events in this slot.
     * 
     * @return number of events in this slot.
     */
    public final int size() {
        return (getLastEventIndex() - getFirstEventIndex());
    }

    /**
     * Returns index of first event in this slot
     * 
     * @return index of first event in this slot.
     */
    public final int offset() {
        return getFirstEventIndex();
    }

    /**
     * Affirms if this block is active
     * 
     * @return true if active
     */
    public boolean isActive() {
        // TODO:
        return true;
    }


    /**
     * Affirms if this block has room for another event to be inserted.
     * 
     * @return true if no more event can be written to this slot
     */
    public boolean isFull() {
        return getLastEventIndex() >= getEventLimit();
    }
    
    /**
     * Gets maximum number of events that can be written to this slot.
     * @return maximum number of events that can be written to this slot.
     */
    public int getEventLimit() {
        return EVENT_LIMIT.getInt(_row);
    }

    /**
     * Returns an event at given index.
     * 
     * @param i
     *            event index. Event index takes offset into account i.e. Event
     *            index 0 is equivalent to absolute index m if slot offset is
     *            m. Must be within range of this slot.
     * @return an event at given index
     */
    public Event getEvent(int i) {
        Assert.assertNotNull(_events, new RuntimeException("no events are available"));
        if (getFirstEventIndex() == getLastEventIndex()) { // special case when events are not initialized
            Assert.assertTrue(i >= getFirstEventIndex() && i <= getLastEventIndex(), 
                    new RuntimeException("event at index " + i 
                    + " is not avaialble. valid event index range is " 
                    + "(" + getFirstEventIndex() + "," + getLastEventIndex() + "]"));
            // no event is available 
            return null;
        } else {
            Assert.assertTrue(i >= getFirstEventIndex() && i < getLastEventIndex(), 
                new RuntimeException("event at index " + i 
                + " is not avaialble. valid event index range is " 
                + "(" + getFirstEventIndex() + "," + getLastEventIndex() + "]"));
            return _events.get(i);
        }
    }
    
    public long getEventCount() {
        if (getFirstEventIndex() < 0) return 0;
        return getLastEventIndex() - getFirstEventIndex();
    }

    /**
     * Flush the current state this slot to the row.  
     */

    void flush() {
        EVENT_COUNT.set(_row, getEventCount());
        ArrayValue array = _timeseries.createEventsArray();
         if (_events != null) {
             for (Event e : _events) {
                 array.add(e.getRecord());
             } 
         } 
         _row.put(EVENTS.getName(), array);
         
        if (isOverlap()) {
            throw new RuntimeException("slot " + this + " overlaps");
        }
        _timeseries._store.getTableAPI().put(_row, null, null);
    }
    
    boolean isOverlap() {
        long t0 = getFirstEventTimestamp();
        long tN = getLastEventTimestamp();
        
        String sql = "select  " 
                + SLOT_INDEX.getName() + ", "
                + TIMESTAMP_FIRST.getName() + ", "
                + TIMESTAMP_LAST.getName()
             + " from " + _timeseries.getName()
             + " where " + TIMESTAMP_FIRST.getName() + ">= 0 AND "
                         + TIMESTAMP_LAST.getName() + ">= 0 AND "
                 + " (( " + TIMESTAMP_FIRST.getName() + " > " + t0 
                     + " AND " + TIMESTAMP_FIRST.getName() + " < " + tN 
                 + ") OR (" + TIMESTAMP_FIRST.getName() + " < " + t0 
                    + " and " + TIMESTAMP_LAST.getName() + " < " + tN + "))";
       TableIterator<RecordValue> rs = _timeseries.query(sql, Consistency.ABSOLUTE);
       boolean overlap = rs.hasNext();
       if (overlap) {
//           System.err.println("overlaps slot " + getIndex()
//           + " " + t0 + " - " + tN);
       }
       while (rs.hasNext()) {
           RecordValue r = rs.next();
//           System.err.println("\t" + SLOT_INDEX.getInt(r)
//           + " " + TIMESTAMP_FIRST.getLong(r)
//           + " - " + TIMESTAMP_LAST.getLong(r));
       }
       return overlap;
    }
    
    @Override
    public int compareTo(Slot o) {
        long t = getFirstEventTimestamp();
        long t2 = o.getFirstEventTimestamp();
        if (t < t2) return -1;
        if (t > t2) return 1;
        return 0;
    }


    public Iterator<Event> iterator() {
        return new EventIterator();
    }

    public String toString() {
        return "Slot-" + getIndex() + " [" + getFirstEventIndex() + ":" + getLastEventIndex() + "]";
    }

    private class EventIterator implements Iterator<Event> {
        int _pos;

        EventIterator() {
            _pos = 0;
        }

        @Override
        public boolean hasNext() {
            return _pos < _events.size();
        }

        @Override
        public Event next() {
            Event event = getEvent(_pos);
            _pos++;
            return event;
        }

    }
    

    
}