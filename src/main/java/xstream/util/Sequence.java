package xstream.util;

import oracle.kv.KVStore;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;

/**
 * A sequence for a distributed database.
 * <br>
 * A sequence provides series of monotonic, unique values. These values
 * can be useful for an application as primary key or other identifiers.
 * <br>
 * Usage:
 * 
 *  
 * @author pinaki poddar
 *
 */
public class Sequence implements Updater {
    private KVStore _store;
    
    // row that represents this sequence
    private Row _row;
    // master table where this sequence is stored as a row
    private Table _table;
    // name of this sequence used as an identifier 
    private final String _name;
    
    // initial value of this sequence. if the sequence exists, and
    // its next available value is greater then initial value 
    private   long _initialValue;
    // the number of values reserved in a batch
    private  final int _length;
    
    private int _cursor;
    
    private final RMWLock _lock;
    
    public static int DEFAULT_LENGTH   = 100;
    public static int DEFAULT_ATTEMPTS = 10;

    // name of the field that stores the current value of the sequence
    public static final String NAME       = "name";
    public static final String NEXT_VALUE = "next";
    
    /**
     * Creates a in-memory sequence possibly reading its current state 
     * from database.
     * 
     * @param api the connection to store
     * @param table the table containing the sequences as row
     * @param name name of the sequence
     * @param length number of values to reserve in a batch
     * @param maxReservationAttempt how many times to try to reserve
     * @param start the starting value if not known pass any negative number
     * @param isNew if true then no row of the given name must exist in given table
     */
    Sequence(KVStore store, Table table, String name, 
            int length, long initial) {
        _store = store;
        _table = table;
       _name   = name;
       _length = length;
       _lock = new RMWLock();
       
       PrimaryKey pk = _table.createPrimaryKey();
       pk.put(NAME, _name);
       _row = _store.getTableAPI().get(pk, null);
       if (_row == null) {
           _initialValue = initial;
           _row = _table.createRow();
           _row.put(NAME, _name);
           _row.put(NEXT_VALUE, _initialValue +_length);
       } 
       _row = _lock.update(_store, _row, this);
       reset(_row);
    }
    
    private void reset(Row row) {
        _initialValue = _row.get(NEXT_VALUE).asLong().get() - _length;
        _cursor = 0;
    }
    
    
    /**
     * Gets next value in this sequence. Repeated call to this method 
     * would return numbers that are strictly monotonic, but not 
     * necessarily contiguous.
     *  
     * @return a number
     */
    public long next() {
        if (_cursor >= _length) {
            _row = _lock.update(_store, _row, this);
            reset(_row);
        }
        return _initialValue + _cursor++;
    }
    
    @Override
    public void update(Row row) {
        long currentValue = row.get(NEXT_VALUE).asLong().get();
        row.put(NEXT_VALUE,  currentValue + _length);
    }
    
    public long current() {
        return _initialValue + _cursor;
    }
    
    public String toString() {
        return _name + ":" + current()
                + " (" + _initialValue + "-" + (_initialValue+_length) + "]";
    }
}
