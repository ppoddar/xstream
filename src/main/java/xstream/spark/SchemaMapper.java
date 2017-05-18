package xstream.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import oracle.kv.table.FieldDef;
import oracle.kv.table.RecordDef;
import oracle.kv.table.FieldDef.Type;
import xstream.Event;
import xstream.TimeSeries;

public class SchemaMapper {
    
    /**
     * Builds schema required for Spark SQL Engine.
     * The conventional encoding mechanics of Spark is not applicable in 
     * this case, because the event schema (properties and types) is
     * not known at compile-time. 
     * <p>
     * Each field of record that defines an {@link Event} is converted to
     * a {@link StructField} that are used by Spark SQL Engine to model schema.
     * 
     * @return a {@link StructType} to represent fields of {@link Event}.
     */
    static StructType buildSchema(TimeSeries series) {
        List<StructField> fields = new ArrayList<StructField>();
        RecordDef def = series.getEventDefinition().asRecordDef();
        int fieldCount = def.getNumFields();
        for (int i = 0; i < fieldCount; i++) {
            fields.add(createStructField(def.getFieldName(i), def.getFieldDef(i)));
        }
        return DataTypes.createStructType(fields);
    }

    /**
     * Creates a {@link StructField} from given name and {@link FieldDef}
     * which is a particle of Oracle NoSQL schema.
     * 
     * @param name name of the field.
     * @param fieldDef definition of the field.
     * 
     * @return a {@link StructField}
     */
    private static StructField createStructField(String name, FieldDef fieldDef) {
        // the last argument mist be MetaData.empty() instead of null
        StructField field = new StructField(name, toSparkDataType(fieldDef.getType()),
                false /* is null */ , Metadata.empty());
        return field;
    }
    
    /**
     * Converts {@link Type Oracle NoSQL schema type} to {@link DataType Apache
     * Spark type}.
     *  
     * @param type {@link Type Oracle NoSQL schema type}
     * 
     * @return an {@link DataType Apache Spark type} 
     * 
     * @exception if given type can not be mapped to Spark.
     */
    private static DataType toSparkDataType(Type type) {
        if (type == Type.INTEGER) return DataTypes.IntegerType;;
        if (type == Type.LONG) return DataTypes.LongType;;
        if (type == Type.DOUBLE) return DataTypes.DoubleType;;
        if (type == Type.STRING) return DataTypes.StringType;;
        throw new RuntimeException("unmapped database type " + type);
    }
}
