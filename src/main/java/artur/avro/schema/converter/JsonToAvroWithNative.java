package artur.avro.schema.converter;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import java.io.*;

/**
 *
 */
public class JsonToAvroWithNative implements JsonToAvro {

    @Override
    public byte[] convert(String schemaStr, String data) {
        InputStream input = null;
        DataFileWriter<GenericRecord> writer = null;
        ByteArrayOutputStream output = null;
        try {
            Schema schema = new Schema.Parser().parse(schemaStr);
            DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            input = new ByteArrayInputStream(data.getBytes());
            output = new ByteArrayOutputStream();
            DataInputStream din = new DataInputStream(input);
            writer = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>());
            writer.create(schema, output);
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
            GenericRecord datum;
            while (true) {
                try {
                    datum = reader.read(null, decoder);
                } catch (EOFException eofe) {
                    break;
                }
                writer.append(datum);
            }
            writer.flush();
            return output.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try { input.close(); } catch (Exception e) { }
        }
        return null;
    }
}
