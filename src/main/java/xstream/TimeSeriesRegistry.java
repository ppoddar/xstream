package xstream;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;


import oracle.kv.Consistency;
import oracle.kv.KVStore;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import xstream.util.NoSQLURL;

import static xstream.TimeSeriesSchema.*;

/**
 * Registry of {@link TimeSeries}.
 * 
 * The content of the registry is maintained on a database table. 
 * A registry is a sigleton per database.
 * A registry is obtained by static <code>getInstance(NoSQLURL)</code> method.
 * <br>
 * @author pinaki poddar
 *
 */
class TimeSeriesRegistry  implements Externalizable {
    private NoSQLURL _url;
    private transient  Table _registry;
    private transient  KVStore _store;
    
    private static Logger _logger = Logger.getLogger(
            TimeSeriesRegistry.class.getName());
    
    /**
     * Creates a registry for given store.
     * @param url URL for a store.
     * @return a registry for the store.
     */
    public static TimeSeriesRegistry getInstance(KVStore store) {
        return new TimeSeriesRegistry(store);
    }
    

    /**
     * private constructor for registry that are singleton per-database. 
     */
    private TimeSeriesRegistry(KVStore store) {
        _store = store;
        _registry = defineRegistryTable(TimeSeriesSchema.REGISTRY_TABLE);
    }
    
    /**
     * Affirms if the given series name has been registered.
     * 
     * @param name name of a series
     * @return true if the name has been registered.
     */
    boolean isRegistered(String name) {
        return getSeriesRow(name, false) != null;
    }
    
    public Table getRegistryTable() {
        return _registry;
    }
    
    public Table getSeriesTable(String tableName, boolean mustExist) {
        Table table = _store.getTableAPI().getTable(tableName);
        if (mustExist && table == null) {
            throw new RuntimeException(tableName + " does not exist");
        }
        return table;
        
    }
    public Row getSeriesRow(String name, boolean mustExist) {
        PrimaryKey pk = _registry.createPrimaryKey();
        pk.put("name", name);
        Row row = _store.getTableAPI().get(pk, 
                new ReadOptions(Consistency.NONE_REQUIRED, 1000, TimeUnit.MILLISECONDS));
        if (mustExist && row == null) {
            throw new RuntimeException(name + " does not exist");
        }
        return row;
    }
    
    
    /**
     * Defines a database table to store metadata about many timeseries.
     * @param name name of the registry table.
     * @return a table where registry content is stored.
     */
    private Table defineRegistryTable(String registryTableName) {
        Table t = _store.getTableAPI().getTable(registryTableName);
        if (t != null) return t;
        
        String ddl = "CREATE TABLE IF NOT EXISTS " + registryTableName 
               + " (" + getDefinitions(SERIES_FIELDS) +
                    ", PRIMARY KEY (" + SERIES_NAME.getName() + "))";
       _logger.log(Level.FINE, "defining  " + ddl);
        _store.executeSync(ddl);
        t = _store.getTableAPI().getTable(registryTableName);
             
        return t;
    }
    
    /*
    StringBuilder eventDef = new StringBuilder();
    eventDef.append(TIMESTAMP).append(' ').append("LONG");
    for (String def : fieldDefs) {
        eventDef.append(',').append(def);
    }
   
    String ddl = "CREATE TABLE " + name 
            + " (" + SLOT_INDEX + " INTEGER" 
            + ", " + EVENT_LIMIT + " INTEGER" 
            + ", " + EVENT_COUNT + " INTEGER" 
            + ", " + FIRST_EVENT_INDEX + " INTEGER" 
            + ", " + LAST_EVENT_INDEX + " INTEGER"
            + ", " + TIMESTAMP_FIRST + " LONG" 
            + ", " + TIMESTAMP_LAST + " LONG" 
            + ", " + EVENTS + " ARRAY(RECORD(" 
                   + eventDef.toString() + "))"
            + ", PRIMARY KEY (" + SLOT_INDEX + "))";
    // (SHARD(" + TIMESTAMP_START + ")"
    // + ", " + BLOCK_INDEX + "))";
    */
    /**
     * Defines a database table to store event data in a series.
     * @param name name of the series table.
     * @param a comma-separated string where each part is a definition
     * of database column.
     * @return a table where registry content is stored.
     */
    public Table defineSeriesTable(String seriesName, String fieldDefs) {
        Table t = _store.getTableAPI().getTable(seriesName);
        if (t != null) return t;
        
        StringBuilder eventDef = new StringBuilder();
        eventDef.append(TIMESTAMP).append(' ').append(TIMESTAMP.getType())
                .append(",")
                .append(fieldDefs);
        String eventsArray = " ARRAY(RECORD(" + eventDef + "))";
        
        String ddl = "CREATE TABLE IF NOT EXISTS " + seriesName 
               + " (" + getDefinitions(SLOT_FIELDS)
               + "," + EVENTS.getName() + eventsArray      
               + ", PRIMARY KEY (" + SLOT_INDEX.getName() + "))";
        _logger.log(Level.FINE, "defining  " + ddl);
        _store.executeSync(ddl);
        
        String createIndex = "CREATE INDEX " + TIMESTAMP_FIRST.getName() + "_idx"
                + " ON " + seriesName + " (" + TIMESTAMP_FIRST.getName() + ")";
        
        _store.executeSync(createIndex);
        
        t = _store.getTableAPI().getTable(seriesName);
             
        return t;
    }
    
    /**
     * Reserves a value in database.
     * <br>
     * Uses a Read-Modify-Update (RMW) pattern to reserve a integer value
     * in the database that is monotonic.
     * 
     * @param row
     * @param version
     * @return
     */

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(_url);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _url = (NoSQLURL)in.readObject();
        _store = _url.openStore();
        _registry = defineRegistryTable(_url.getSeriesName());
    }
    
    /**
     * Creates a string acceptable as part of a DDL for given database 
     * fields.
     * @param fields
     * @return
     */
    String getDefinitions(Field[] fields) {
        StringBuilder buf = new StringBuilder();
        for (Field f : fields) {
            if (buf.length() > 0) buf.append(", ");
            buf.append(f.getName()).append(" ")
               .append(f.getType().toString());
        }
        return buf.toString();
    }
}
