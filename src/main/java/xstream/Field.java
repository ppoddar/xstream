package xstream;

import oracle.kv.table.FieldDef.Type;
import oracle.kv.table.FieldValue;
import oracle.kv.table.RecordDef;


import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import xstream.util.Assert;

public class Field {
    private String _name;
    private Type _dbType;
    public Field(String name, Type dbType) {
        _name = name;
        _dbType = dbType;
    }
    
    public void set(Row row, Object value) {
        set(row.asRecord(), value);
    }
    
    public void set(RecordValue row, Object value) {
        Class<?> cls = value.getClass();
//        if (!row.contains(_name)) throw new RuntimeException("record "
//                + row.getDefinition().getFieldNames() + " does not contain "
//                + "[" + _name + "] property");
        if (cls == long.class || cls == Long.class) {
            row.put(_name, Long.class.cast(value));
        } else if (cls == int.class || cls == Integer.class) {
            row.put(_name, Integer.class.cast(value));
        } else if (cls == String.class) {
            row.put(_name, String.class.cast(value));
        } else {
            throw new RuntimeException(this + " cannot set " + value);
        }
    }
    
    public String getName() {
        return _name;
    }
    
    public Type getType() {
        return _dbType;
    }
    
    public long getLong(Row row) {
        long v = row.get(_name).asLong().get();
        return v;
    }
    
    public int getInt(Row row) {
        if (row == null) throw new IllegalArgumentException("canot get "
                + this + " from null row");
        try {
            FieldValue data = row.get(_name);
            int v = data.asInteger().get();
            return v;
        } catch (IllegalArgumentException ex) {
            throw new RuntimeException(this + " row " + row, ex);
        }
    }

    public int getInt(RecordValue row) {
        if (row == null) throw new IllegalArgumentException("canot get "
                + this + " from null row");
        try {
            FieldValue data = row.get(_name);
            if (data != null) {
                if (data.asInteger() == null) {
                    return (int)data.asLong().get();
                } else {
                    return data.asInteger().get();
                }
            } else {
                return 0;
            }
        } catch (IllegalArgumentException ex) {
            throw new RuntimeException(this + " row " + row, ex);
        }
    }
    
    public int getInt(RecordValue row, int def) {
        if (!row.contains(_name)) return def;
        int v = row.get(_name).asInteger().get();
        return v;
    }

  
    public long getLong(RecordValue record) {
        long v = record.get(_name).asLong().get();
        return v;
    }
    
    public boolean isDefined(RecordDef def) {
        return def.contains(_name);
    }
    
    public boolean existsIn(RecordValue record) {
        return record.contains(_name);
    }
    
    public void assertExistsIn(RecordValue record) {
        Assert.assertTrue(this.existsIn(record),
                this + " does not exist in record " 
                        + record.getDefinition().getFieldNames());
            
        
    }

    
    public String toString() {
        return getName();
    }


}
