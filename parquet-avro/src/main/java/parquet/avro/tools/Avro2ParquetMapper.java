package parquet.avro.tools;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.mapreduce.Mapper;

public class Avro2ParquetMapper extends
    Mapper<AvroKey<GenericRecord>, Void, Void, GenericRecord> {

  @Override
  protected void map(AvroKey<GenericRecord> key, Void value,
      Context context) throws IOException, InterruptedException {
    context.write(null, key.datum());
  }
}
