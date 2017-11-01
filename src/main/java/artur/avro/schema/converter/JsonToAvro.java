package artur.avro.schema.converter;

/**
 *
 */
public interface JsonToAvro {
    byte[] convert(String schema, String data);
}
