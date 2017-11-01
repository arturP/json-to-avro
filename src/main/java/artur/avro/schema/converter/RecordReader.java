package artur.avro.schema.converter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

/**
 *
 */
public interface RecordReader {

    GenericData.Record read(Schema schema, byte[] data);
}
