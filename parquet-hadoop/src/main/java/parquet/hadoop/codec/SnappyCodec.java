package parquet.hadoop.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DecompressorStream;
import org.apache.hadoop.io.compress.snappy.SnappyCompressor;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor;

import parquet.hadoop.ParquetWriter;
// SWITCH

/**
 * This class creates stream snappy compressors/decompressors (rather than block codecs).
 */
public class SnappyCodec implements Configurable, CompressionCodec {
  /** Configuration key for the Snappy stream buffer size */
  public static final String SNAPPY_STREAM_BUFFER_SIZE_KEY = "snappy.stream.buffer.size";
  // What should the right size be?  I'd expect it should be tied to the page size, but setting
  // it too small can cause some map tasks to fail.
  /** Default value for Snappy stream buffer size is 10x the default Parquet page size */
  public static final int DEFAULT_SNAPPY_STREAM_BUFFER_SIZE = 2 * ParquetWriter.DEFAULT_PAGE_SIZE;

  Configuration conf;

  /**
   * Set the configuration to be used by this object.
   *
   * @param conf the configuration object.
   */
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Return the configuration used by this object.
   *
   * @return the configuration object used by this objec.
   */
  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * Create a {@link CompressionOutputStream} that will write to the given
   * {@link OutputStream}.
   *
   * @param out the location for the final output stream
   * @return a stream the user can write uncompressed data to have it compressed
   * @throws IOException
   */
  @Override
  public CompressionOutputStream createOutputStream(OutputStream out)
      throws IOException {
    return createOutputStream(out, createCompressor());
  }

  /**
   * Create a {@link CompressionOutputStream} that will write to the given
   * {@link OutputStream} with the given {@link Compressor}.
   *
   * @param out        the location for the final output stream
   * @param compressor compressor to use
   * @return a stream the user can write uncompressed data to have it compressed
   * @throws IOException
   */
  @Override
  public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor)
      throws IOException {
    int bufferSize = conf.getInt(SNAPPY_STREAM_BUFFER_SIZE_KEY, DEFAULT_SNAPPY_STREAM_BUFFER_SIZE);
    return new SnappyCompressorStream(out, bufferSize);
  }

  /**
   * Get the type of {@link Compressor} needed by this {@link CompressionCodec}.
   *
   * @return the type of compressor needed by this codec.
   */
  @Override
  public Class<? extends Compressor> getCompressorType() {
    return SnappyCompressor.class;
  }

  /**
   * Create a new {@link Compressor} for use by this {@link CompressionCodec}.
   *
   * @return a new compressor for use by this codec
   */
  @Override
  public Compressor createCompressor() {
    int bufferSize = conf.getInt(SNAPPY_STREAM_BUFFER_SIZE_KEY, DEFAULT_SNAPPY_STREAM_BUFFER_SIZE);
    return new SnappyCompressor(bufferSize);
//    return new SnappyCompressor(); // SWITCH
  }

  /**
   * Create a {@link CompressionInputStream} that will read from the given
   * input stream.
   *
   * @param in the stream to read compressed bytes from
   * @return a stream to read uncompressed bytes from
   * @throws IOException
   */
  @Override
  public CompressionInputStream createInputStream(InputStream in)
      throws IOException {
    return createInputStream(in, createDecompressor());
  }

  /**
   * Create a {@link CompressionInputStream} that will read from the given
   * {@link InputStream} with the given {@link Decompressor}.
   *
   * @param in           the stream to read compressed bytes from
   * @param decompressor decompressor to use
   * @return a stream to read uncompressed bytes from
   * @throws IOException
   */
  @Override
  public CompressionInputStream createInputStream(InputStream in,
      Decompressor decompressor) throws IOException {
    int bufferSize = conf.getInt(SNAPPY_STREAM_BUFFER_SIZE_KEY, DEFAULT_SNAPPY_STREAM_BUFFER_SIZE);
    return new DecompressorStream(in, decompressor, bufferSize);
  }

  /**
   * Get the type of {@link Decompressor} needed by this {@link CompressionCodec}.
   *
   * @return the type of decompressor needed by this codec.
   */
  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    return SnappyDecompressor.class;
  }

  /**
   * Create a new {@link Decompressor} for use by this {@link CompressionCodec}.
   *
   * @return a new decompressor for use by this codec
   */
  @Override
  public Decompressor createDecompressor() {
    int bufferSize = conf.getInt(SNAPPY_STREAM_BUFFER_SIZE_KEY, DEFAULT_SNAPPY_STREAM_BUFFER_SIZE);
    return new SnappyDecompressor(bufferSize);
  }

  /**
   * Get the default filename extension for this kind of compression.
   *
   * @return <code>.snappy</code>.
   */
  @Override
  public String getDefaultExtension() {
    return ".snappy";
  }
}
