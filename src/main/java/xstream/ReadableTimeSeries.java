package xstream;

import static xstream.TimeSeriesSchema.*;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;

import org.apache.commons.collections4.iterators.LazyIteratorChain;

import oracle.kv.Consistency;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableIterator;

public class ReadableTimeSeries extends TimeSeries {
    
    ReadableTimeSeries(String config, Row row, Table table) {
        super(config, row, table, true);
    }
    
    public Slot initSlot() {
        return findSlot(0);
    }
     
    /**
     * Generates a cursor to read events of given time slot.
     * @param slotIndex index of a slot
     * @return an iterator of event
     */
    public Iterator<Event> readBySlot(int slotIndex) {
            return findSlot(slotIndex, true).iterator();
    }
    
    public Iterator<Event> read() {
        return readByTime(-1 /* beginning o time*/, -1 /* end of time */);
    }
    /**
     * A cursor to read events between given start and end time.
     * 
     * @param startTime any negative value implies from the first available event 
     * @param endTime any negative value implies till the last available event 
     * @return an iterator of event
     */

    public Iterator<Event> readByTime(final long startTime, final long endTime) {
        final Set<Integer> slots = findSlotIndicesByTime(
                startTime < 0 ? Long.MIN_VALUE :startTime,
                endTime < 0   ? Long.MAX_VALUE : endTime);
        _logger.log(Level.FINE, "readByTime() found slots " + slots 
                + " for time between (" + startTime + "," + endTime + ")");
        LazyIteratorChain<Event> chain = new LazyIteratorChain<Event>() {
            
            Iterator<Integer> iterators = slots.iterator();
            @Override
            protected Iterator<? extends Event> nextIterator(int iteratorIndex) {
                _logger.log(Level.FINE, "readByTime.nextIterator() " + iteratorIndex);
                if (iterators.hasNext()) { 
                    Slot slot = findSlot(iterators.next());
                    return slot.iterator();
                } else {
                    return null;
                }
            }
        };
        return chain;
    }
    
    /**
     * Events are not ordered by slot index. Events in each slot are time
     * ordered. We need to sort the slots based on their start time.
     *  
     * @param startTime
     * @param endTime
     * @return
     */
    Set<Integer> findSlotIndicesByTime(long startTime, long endTime) {
        String sql = "SELECT " + SLOT_INDEX.getName() + ", "
                +  TIMESTAMP_FIRST.getName() + ", "
                +  TIMESTAMP_LAST.getName()
                + " FROM " + getName();
        String q1 = TIMESTAMP_FIRST + ">=" +  startTime;
        String q2 = TIMESTAMP_LAST  + "<=" +  endTime;
        if (startTime != Long.MIN_VALUE) {
            if (endTime != Long.MAX_VALUE) {
                sql += " WHERE " + q1 + " AND " + q2;
            }
        } else if (endTime != Long.MAX_VALUE) {
            sql += " WHERE " + q2;
        }
        sql += " ORDER BY " + TIMESTAMP_FIRST.getName();
        
        TableIterator<RecordValue> rs = query(sql, Consistency.NONE_REQUIRED);  
        Set<Integer> result = new TreeSet<Integer>();
        try {
            while (rs.hasNext()) {
                RecordValue r = rs.next();
                result.add(SLOT_INDEX.getInt(r));
            }
        } finally {
            rs.close();
        }
        return result;
    }
    


}
