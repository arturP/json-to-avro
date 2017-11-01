package artur.avro.schema.converter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;

import java.io.*;

/**
 *
 */
public class AvroToJson {

    public String convertToJson(String schemaStr, byte[] avro) {
        boolean pretty = false;
        GenericDatumReader<GenericRecord> reader = null;
        JsonEncoder encoder = null;
        ByteArrayOutputStream output = null;
        try {
            Schema schema = new Schema.Parser().parse(schemaStr);
            reader = new GenericDatumReader<>(schema);
            InputStream input = new ByteArrayInputStream(avro);
            output = new ByteArrayOutputStream();
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
            encoder = EncoderFactory.get().jsonEncoder(schema, output, pretty);
            Decoder decoder = DecoderFactory.get().binaryDecoder(input, null);
            GenericRecord datum;
            while (true) {
                try {
                    datum = reader.read(null, decoder);
                } catch (EOFException eofe) {
                    break;
                }
                writer.write(datum, encoder);
            }
            encoder.flush();
            output.flush();
            return new String(output.toByteArray());
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            try { if (output != null) output.close(); } catch (Exception e) { }
        }
        return null;
    }
}
