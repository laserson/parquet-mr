package parquet.avro.tools;

import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import parquet.avro.AvroParquetOutputFormat;
import parquet.avro.AvroSchemaConverter;
import parquet.hadoop.metadata.CompressionCodecName;

public class Avro2Parquet extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    Path schemaPath = new Path(args[0]);
    Path inputPath = new Path(args[1]);
    Path outputPath = new Path(args[2]);

    Job job = new Job(getConf());
    job.setJarByClass(getClass());
    Configuration conf = job.getConfiguration();

    FileSystem fs = FileSystem.get(conf);
    InputStream in = fs.open(schemaPath);
    Schema avroSchema = new Schema.Parser().parse(in);

    System.out.println(new AvroSchemaConverter().convert(avroSchema).toString());

    FileInputFormat.addInputPath(job, inputPath);
    job.setOutputFormatClass(AvroParquetOutputFormat.class);
    AvroParquetOutputFormat.setOutputPath(job, outputPath);
    AvroParquetOutputFormat.setSchema(job, avroSchema);
    AvroParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
    AvroParquetOutputFormat.setCompressOutput(job, true);
    /* Impala likes Parquet files to have only a single row group.
     * Setting the block size to a larger value helps ensure this to
     * be the case, at the expense of buffering the entire mapper's
     * split in memory. 
     */
    AvroParquetOutputFormat.setBlockSize(job,
        (int) fs.getFileStatus(inputPath).getBlockSize() * 5);

    job.setMapperClass(Avro2ParquetMapper.class);
    job.setNumReduceTasks(0);

    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Avro2Parquet(), args);
    System.exit(exitCode);
  }

}
