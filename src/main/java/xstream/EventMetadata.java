package xstream;

import static xstream.TimeSeriesSchema.TIMESTAMP;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldDef.Type;
import oracle.kv.table.FieldValue;
import oracle.kv.table.RecordDef;
import oracle.kv.table.RecordValue;
import xstream.util.Assert;

/**
 * Metadata abount a {@link Event}.
 * The metadata wraps a {@link RecordDef definition} of database record.
 * 
 * @author pinaki poddar
 *
 */
@SuppressWarnings("serial")
public class EventMetadata implements Serializable {
    private final RecordDef _schema;
    static Map<Type, Converter> _converters;
    static Map<String, Type> _dbTypes;
    // initializes allowed converters
    static {
        _converters = new HashMap<Type, Converter>();
        _dbTypes = new HashMap<String, Type>();
        
        //_dbTypes.put(Type.BOOLEAN.name(), Type.BOOLEAN);
        //_dbTypes.put(Type.FLOAT.name(), Type.FLOAT);
        //_dbTypes.put(Type.NUMBER.name(), Type.NUMBER);
        //_dbTypes.put(Type.TIMESTAMP.name(), Type.TIMESTAMP);
        
        initializeTypeConversion(Type.DOUBLE, new Converter.DoubleValue());
        initializeTypeConversion(Type.INTEGER, new Converter.IntegerValue());
        initializeTypeConversion(Type.LONG, new Converter.LongValue());
        initializeTypeConversion(Type.STRING, new Converter.StringValue());
        
    }
    
    static void initializeTypeConversion(Type type, Converter converter) {
        _dbTypes.put(type.name().toUpperCase(), type);
        _converters.put(type, converter);
    }
    /**
     * Create event definition from database field definition of an event.
     * 
     * @param recordDef database definition of an event object
     */
    public EventMetadata(RecordDef recordDef) {
        Assert.assertTrue(TIMESTAMP.isDefined(recordDef),
                "event definition does not contain " + TIMESTAMP);
        _schema = recordDef;
        for (int i = 0; i < _schema.getNumFields(); i++) {
            Type dbType = _schema.getFieldDef(i).getType();
            _converters.put(dbType, _converters.get(dbType));
        }
    }
    
    
    public RecordDef asRecordDef() {
        return _schema;
    }
    
    public List<String> getPropertyNames() {
        return _schema.getFieldNames();
    }
    
    /**
     * Creates a new event from user values.
     * @param values the map contains field values indexed by field name.
     * The schema definition must have the field name defined.
     * 
     * @return an event populated
     */
    Event newEvent(Map<String, Object> values) {
        RecordValue record = _schema.createRecord();
        for (Map.Entry<String, Object> e : values.entrySet()) {
            set(e.getKey(), record, e.getValue());
        }
        return new Event(this, record);
    }

    
    /**
     * Creates a new event from database values.
     * @param ts
     * @param record
     * @return
     */
    Event newEvent(RecordValue record) {
        Event event = new Event(this, record);
        return event;  
    }
    
    Object convertFromDatabaseType(FieldValue field) {
        if (field.isInteger()) return field.asInteger().get();
        else if (field.isLong()) return field.asLong().get();
        else if (field.isDouble()) return field.asDouble().get();
        else if (field.isString()) return field.asString().get();
        else throw new RuntimeException("unsupported field type " + field.getType());
    }
    
    Object get(String propertyName, RecordValue record) {
        if (!record.contains(propertyName)) {
            throw new IllegalArgumentException("missing property " + propertyName
                    + " available properties are " + record.getFieldNames());
        }
        FieldValue fieldValue = record.get(propertyName);
        if (fieldValue.isNull()) return null;
        if (fieldValue.isInteger()) {
            return fieldValue.asInteger().get();
        } else if (fieldValue.isLong()) {
            return fieldValue.asLong().get();
        } else if (fieldValue.isDouble()) {
            return fieldValue.asDouble().get();
        } else if (fieldValue.isString()) {
            return fieldValue.asString().get();
        }
        
        throw new RuntimeException();
    }
    
    /**
     * 
     * @param propertyTypeName name of database property
     * @param record the database record to be populated
     * @param userValue the value specified by the user to populate the record
     * @param dbType type of value acceptable by database record
     */
    void set(String propertyName, RecordValue record, Object userValue) {
        FieldDef property = record.getDefinition().getFieldDef(propertyName);
        if (property == null) {
            throw new RuntimeException("property [" + propertyName 
                    + "] does not exists in record " 
                    + record.getDefinition().getFieldNames());
        }
        
        Type dbType = property.getType();
        Converter converter = _converters.get(dbType);
        converter.set( propertyName,  record,  userValue);
    }
    
    Type getDataBaseType(String propertyTypeName) {
        if (!_dbTypes.containsKey(propertyTypeName)) {
            throw new RuntimeException("no conversion for type " + propertyTypeName);
        }
        return _dbTypes.get(propertyTypeName);
    }

    
    
    /**
     * converters convert user value to database value.
     * @author pinaki poddar
     *
     */
    interface Converter {
        void set(String propertyName, RecordValue record, Object userValue);
        
        public class IntegerValue implements Converter {
            @Override
            public void set(String propertyName, RecordValue record, Object userValue) {
                record.put(propertyName, Integer.parseInt(userValue.toString()));
            }
        }
        
        public class DoubleValue implements Converter {
            @Override
            public void set(String propertyName, RecordValue record, Object userValue) {
                record.put(propertyName, Double.parseDouble(userValue.toString()));
            }
        }
        
        public class LongValue implements Converter {
            @Override
            public void set(String propertyName, RecordValue record, Object userValue) {
                record.put(propertyName, Long.parseLong(userValue.toString()));
            }
        }

        public class StringValue implements Converter {
            @Override
            public void set(String propertyName, RecordValue record, Object userValue) {
                record.put(propertyName, userValue.toString());
            }
        }

    }
    
}
