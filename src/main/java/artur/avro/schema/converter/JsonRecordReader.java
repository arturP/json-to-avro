package artur.avro.schema.converter;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.*;

import com.google.common.base.Optional;

/**
 * Reads JSON object and transform it into GenericData.Record
 */
public class JsonRecordReader implements RecordReader {

    private final ObjectMapper mapper;
    private static final Object NOT_VALID = new Object();

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

        for (Map.Entry<String, Object> element : json.entrySet()){
            Optional<Schema.Field> field = Optional.fromNullable(schema.getField(element.getKey()));
            if (field.isPresent()) {
                record.set(field.get(), read(field.get().schema(), field.get(), element.getValue(), path));
            }
        }
        return record.build();
    }

    private List<Object> readArray(Schema schema, Schema.Field field, List<Object> items, Deque<String> path) {
        List<Object> result = new ArrayList<>();
        for (Object element : items) {
            result.add(read(schema.getElementType(), field, element, path));
        }
        return result;
    }

    private Map<String, Object> readMap(Schema schema, Schema.Field field, Map<String, Object> items, Deque<String> path){
        Map<String, Object> result = new HashMap<>(items.size());
        for (Map.Entry<String, Object> element : items.entrySet()) {
            result.put(element.getKey(), read(schema.getElementType(), field, element.getValue(), path));
        }
        return result;
    }

    private Object read(Schema schema, Schema.Field field, Object value, Deque<String> path) {
        boolean pushed = !field.name().equals(path.peek());
        if(pushed) {
            path.push(field.name());
        }

        Object result;

        switch (schema.getType()) {
            case RECORD : result = createRecord(schema, value, path); break;
            case ARRAY : result = createArray(schema, field, value, path); break;
            case MAP : result = createMap(schema, field, value, path); break;
            case UNION : result = createUnion(schema, field, value, path); break;
            case STRING : result = createSimple(value, String.class); break;
            case INT : result = createSimple(value, Integer.class); break;
            case LONG : result = createSimple(value, Long.class); break;
            case FLOAT : result = createSimple(value, Float.class); break;
            case DOUBLE : result = createSimple(value, Double.class); break;
            case BOOLEAN : result = createSimple(value, Boolean.class); break;
            //case ENUM : break;
            case NULL : result = value == null ? value : NOT_VALID; break;
            default : throw new AvroRuntimeException("Unsupported type " + field.schema().getType());
        }

        if(pushed) {
            path.pop();
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    private Object createRecord(Schema schema, Object value, Deque<String> path) throws AvroTypeException {
        if (Map.class.isInstance(value)) {
            return readRecord(schema, (Map<String, Object>)value, path);
        } else {
            throw new AvroTypeException("Unexpected field type for: " + value);
        }
    }

    @SuppressWarnings("unchecked")
    private Object createArray(Schema schema, Schema.Field field, Object value, Deque<String> path) throws AvroTypeException {
        if (List.class.isInstance(value)) {
            return readArray(schema, field, (List<Object>)value, path);
        } else {
            throw new AvroTypeException("Unexpected field type for: " + value);
        }
    }

    @SuppressWarnings("unchecked")
    private Object createMap(Schema schema, Schema.Field field, Object value, Deque<String> path) throws AvroTypeException {
        if (Map.class.isInstance(value)) {
            return readMap(schema, field, (Map<String, Object>)value, path);
        } else {
            throw new AvroTypeException("Unexpected field type for: " + value);
        }
    }

    private Object createUnion(Schema schema, Schema.Field field, Object value, Deque<String> path) throws AvroTypeException {
        for (Schema type : schema.getTypes()) {
            Object unionValue = read(type, field, value, path);
            if (unionValue != NOT_VALID) {
                return unionValue;
            } else {
                continue;
            }
        }
        throw new AvroTypeException("Cannot read union value for: " + value);
    }

    private <T> Object createSimple(Object value, Class<T> type){
        if (type.isInstance(value)) {
            return value;
        } else {
            throw new AvroTypeException("Unexpected field type for: " + value);
        }
    }
}
