package artur.avro.schema.converter;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import com.google.common.base.Optional;

/**
 * Reads JSON object and transform it into GenericData.Record
 */
public class JsonRecordReader implements RecordReader {

    private final ObjectMapper mapper;

    public JsonRecordReader(ObjectMapper mapper){
        this.mapper = mapper;
    }

    public JsonRecordReader() {
        this(new ObjectMapper());
    }

    @Override
    @SuppressWarnings("unchecked")
    public GenericData.Record read(Schema schema, byte[] data) {
        try {
            return read(schema, mapper.readValue(data, Map.class));
        } catch (IOException e) {
            throw new AvroRuntimeException("Cannot convert to JSON. " + e);
        }
    }

    private GenericData.Record read(Schema schema, Map<String, Object> json){
        Deque<String> path = new ArrayDeque<>();
        return readRecord(schema, json, path);
    }

    private GenericData.Record readRecord(Schema schema, Map<String, Object> json, Deque<String> path) {

        GenericRecordBuilder record = new GenericRecordBuilder(schema);

        for(Map.Entry<String, Object> element : json.entrySet()){
            Optional<Schema.Field> field = Optional.fromNullable(schema.getField(element.getKey()));
            if(field.isPresent()){
                record.set(field.get(), read(schema, field.get(), element.getValue(), path));
            }
        }
        return record.build();
    }

    private Object read(Schema schema, Schema.Field field, Object value, Deque<String> path) {

        Object result = null;

        switch (schema.getType()) {
            case RECORD : break;
            case ARRAY : break;
            case MAP : break;
            case UNION : break;
            case STRING : break;
            case INT : break;
            case LONG : break;
            case FLOAT : break;
            case DOUBLE : break;
            case BOOLEAN : break;
            case ENUM : break;
            case NULL : result = value == null ? value : new Object(); break;
            default : throw new AvroRuntimeException("Unsupported type " + field.schema().getType());
        }
        return result;
    }
}
