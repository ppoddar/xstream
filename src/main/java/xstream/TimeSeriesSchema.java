package xstream;

import oracle.kv.KVStore;
import oracle.kv.table.FieldDef;
import oracle.kv.table.RecordDef;
import oracle.kv.table.Table;
import oracle.kv.table.FieldDef.Type;

/**
 * Defines customized table schema for timeseries.
 * Match existing table definition with series schema.
 * Match existing event definition with event schema.
 * 
 * 
 * @author pinaki poddar
 *
 */
class TimeSeriesSchema {
    public static final String REGISTRY_TABLE = "timeseries_registry";
    public static final int DEFAULT_EVENT_PER_SLOT_LIMIT = 1024;
    
  //  public static class X {
    // defines fields whose values are for a series
    public static final Field SERIES_NAME       = new Field("name",        Type.STRING);
    public static final Field SLOT_COUNT        = new Field("slotCount",   Type.INTEGER);
    public static final Field READ_SLOT_INDEX   = new Field("readSlotIdx", Type.INTEGER);
    public static final Field WRITE_SLOT_INDEX  = new Field("writeSlotIdx",Type.INTEGER);
    public static final Field NEXT_SLOT_INDEX   = new Field("nextSlotIdx", Type.INTEGER);
    public static final Field TIMESTAMP_FIRST   = new Field("tsFirst",     Type.LONG);
    public static final Field TIMESTAMP_LAST    = new Field("tsLast",      Type.LONG);
    public static final Field TIME_ZONE         = new Field("timeZone",    Type.STRING);
    public static final Field EVENT_DEFINITION  = new Field("eventDef",    Type.STRING);
    public static final Field INTERVAL_IS_UNIFORM = new Field("uniform",   Type.LONG);
    public static final Field TIME_INTERVAL     = new Field("interval",    Type.LONG);
  //  }
    
    // defines fields for a time slot
    public static final Field SLOT_INDEX        = new Field("idx",         Type.INTEGER);      // index of a block >= 0
    public static final Field EVENT_COUNT       = new Field("size",        Type.LONG);   // #event in a slot
    public static final Field LAST_EVENT_INDEX  = new Field("lastIdx",     Type.INTEGER); // index  of last events in a block > 0
    public static final Field FIRST_EVENT_INDEX = new Field("firstIdx",    Type.INTEGER);// index of first event in a block < no of events in a block
    public static final Field EVENT_LIMIT       = new Field("limit",       Type.INTEGER);   // maximum capacity of events in a block 
    public static final Field EVENTS            = new Field("events",      Type.ARRAY);   // event data  
    
    public static final Field NEXT_SLOT = new Field("next", Type.INTEGER);
    public static final Field PREV_SLOT = new Field("prev", Type.INTEGER);
    
 // time stamp on  events 
    public static final Field TIMESTAMP  = new Field("time", Type.LONG); 
    
    public static final Field[] SERIES_FIELDS = {
            SERIES_NAME, SLOT_COUNT, EVENT_LIMIT, EVENT_COUNT,
            READ_SLOT_INDEX, WRITE_SLOT_INDEX, NEXT_SLOT_INDEX,
            TIMESTAMP_FIRST, TIMESTAMP_LAST,
            TIME_ZONE, 
            EVENT_DEFINITION,
            INTERVAL_IS_UNIFORM, TIME_INTERVAL
    };
    public static final Field[] SLOT_FIELDS = {
            SLOT_INDEX,
            EVENT_COUNT, EVENT_LIMIT, 
            TIMESTAMP_FIRST, TIMESTAMP_LAST,
            FIRST_EVENT_INDEX, LAST_EVENT_INDEX
    };

    /**
     * Defines a table for a timeseries.
     * 
     * @param store
     *            connection to store. Must not be null.
     * @param name
     *            name of the timeseries. Must not be null.
     * @param fieldDefs
     *            definitions of event properties. Can be null if timeseries
     *            exists. Must not be null or empty.
     * @return
     */
    public static Table defineTable(KVStore store, String name, String... fieldDefs) {
        if (fieldDefs == null || fieldDefs.length == 0) {
            throw new IllegalArgumentException("can not define timeseries" + " with null or empty field definitions");
        }
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

        store.executeSync(ddl);
        return store.getTableAPI().getTable(name);
    }

    /**
     * match existing event definition on the schema versus the field
     * given definitions. 
     * 
     *  // TODO:
     *  
     * @param table
     * @param fieldDefs
     */
    public static void matchDefinition(Table table, String... fieldDefs) {
        if (fieldDefs == null || fieldDefs.length == 0) {
            return;
        }
        //table.getField(EVENTS).asArray().getElement().asRecord();
        
        assertField(table, SLOT_INDEX);
        assertField(table, FIRST_EVENT_INDEX);
        assertField(table, TIMESTAMP_FIRST);
        assertField(table, LAST_EVENT_INDEX);
        assertField(table, TIMESTAMP_LAST);
        assertField(table, EVENT_LIMIT);
        assertField(table, EVENT_COUNT);
        assertField(table, EVENTS, fieldDefs);
        
    }
    private static void assertField(Table table, Field field, String...elementDefs) {
        assertField(table, field.getName(), field.getType(), elementDefs);
    }
    private static void assertField(Table table, String fieldName, 
            Type fieldType, String...elementDefs) {
        FieldDef field = table.getField(fieldName);
        if (field == null) {
            throw new RuntimeException(fieldName + " does not exist in " + table.getName());
        }
        if (field.getType() != fieldType) {
            throw new RuntimeException("Type of " + fieldName + " " 
                    + field.getType() + " does not match expecetd " + fieldType);
        }
        if (elementDefs != null && field.isArray()) {
            FieldDef elem = field.asArray().getElement();
            for (String elemDef : elementDefs) {
                assertElementDef(elem.asRecord(), elemDef);
            }
        }
    }

    private static void assertElementDef(RecordDef record, String fieldDef) {
        String elementName = fieldDef.split(" ")[0].trim();
        String elementTypeName = fieldDef.split(" ")[1].trim();
        FieldDef elemDef = record.getFieldDef(elementName);
        if (elemDef == null) {
            throw new RuntimeException(elementName + " does not exist in");
        }
        if (!elemDef.getType().toString().equalsIgnoreCase(elementTypeName)) {
            throw new RuntimeException(elementTypeName + " does not match " + elemDef.getType());
        }
    }


}
