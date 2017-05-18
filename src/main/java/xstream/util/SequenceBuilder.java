package xstream.util;

import oracle.kv.KVStore;
import oracle.kv.table.Table;

public class SequenceBuilder {
    private KVStore _store;
    private String _name;
    private int _increment;
    private long _initialValue;
    
    public static String SEQUENCE_TABLE = "Sequences";
    private static final Object tableLock = new Object();
    
    
    public Sequence build() {
        Table sequenceTable = _store.getTableAPI().getTable(SEQUENCE_TABLE);
        if (sequenceTable == null) {
            sequenceTable = defineSequenceTable(_store, SEQUENCE_TABLE);
        }
        return new Sequence(_store, sequenceTable, _name, 
                _increment, _initialValue);
    }
    
    public SequenceBuilder withStore(KVStore store) {
        _store = store;
        return this;
    }
    
    public SequenceBuilder withName(String name) {
        _name = name;
        return this;
    }
    
    public SequenceBuilder withIncrement(int inc) {
        _increment = inc;
        return this;
    }
    
    public SequenceBuilder withInitialValue(int value) {
        _initialValue = value;
        return this;
    }
    
    
    
    /**
     * Define a sequence.
     * @param store the store where sequence is define
     * @param tableName name of the table that contains sequence data
     * @return a table
     */
    public static Table defineSequenceTable(KVStore store, String tableName) {
        synchronized (tableLock) {
            String sql = "CREATE TABLE IF NOT EXISTS " + tableName 
                + " (" + Sequence.NAME + " STRING, " 
                + Sequence.NEXT_VALUE + " LONG,"
                + " PRIMARY KEY(" + Sequence.NAME + "))";
            store.executeSync(sql);
            return store.getTableAPI().getTable(tableName);
        }
    }

}
