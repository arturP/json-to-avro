package artur.avro.schema.converter;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 *
 */
public class JsonToAvroBinary {

    private RecordReader recordReader;

    public JsonToAvroBinary(RecordReader reader) {
        this.recordReader = reader;
    }

    public byte[] convert(String schema, String data) {
        return convert(new Schema.Parser().parse(schema), data.getBytes());
    }

    public byte[] convert(Schema schema, byte[] data) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        GenericDatumWriter<Object> writer = new GenericDatumWriter<>(schema);
        try {
            writer.write(recordReader.read(schema, data), encoder);
            encoder.flush();
        } catch (IOException e) {
            throw new AvroRuntimeException("Cannot convert to Binary Avro " + e);
        }

        return outputStream.toByteArray();
    }
}
