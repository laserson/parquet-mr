package parquet.hadoop.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.CompressorStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DecompressorStream;
import org.apache.hadoop.io.compress.snappy.SnappyCompressor;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor;

import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.ParquetWriter;

public class RawSnappyCodec implements Configurable, CompressionCodec {
  Configuration conf;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  public static boolean isNativeSnappyLoaded(Configuration conf) {
    return true;
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out)
      throws IOException {
    return createOutputStream(out, createCompressor());
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out,
      Compressor compressor) throws IOException {
    int bufferSize = conf.getInt(ParquetOutputFormat.PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE) * 3;
    return new CompressorStream(out, compressor, bufferSize);
  }

  @Override
  public Class<? extends Compressor> getCompressorType() {
    return SnappyCompressor.class;
  }

  @Override
  public Compressor createCompressor() {
    int bufferSize = conf.getInt(ParquetOutputFormat.PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE) * 3;
    return new SnappyCompressor(bufferSize);
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in)
      throws IOException {
    return createInputStream(in, createDecompressor());
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in,
      Decompressor decompressor) throws IOException {
    int bufferSize = conf.getInt(ParquetOutputFormat.PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE) * 3;
    return new DecompressorStream(in, decompressor, bufferSize);
  }

  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    return SnappyDecompressor.class;
  }

  @Override
  public Decompressor createDecompressor() {
    int bufferSize = conf.getInt(ParquetOutputFormat.PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE) * 3;
    return new SnappyDecompressor(bufferSize);
  }

  @Override
  public String getDefaultExtension() {
    return ".snappy";
  }
}
