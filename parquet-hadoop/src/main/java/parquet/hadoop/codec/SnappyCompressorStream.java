package parquet.hadoop.codec;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.io.compress.CompressorStream;
import org.apache.hadoop.io.compress.snappy.SnappyCompressor;

public class SnappyCompressorStream extends CompressorStream {

  protected int bufferSize;

  public SnappyCompressorStream(OutputStream out, int initialBufferSize) {
    super(out);
    if (initialBufferSize <= 0) {
      throw new IllegalArgumentException("Illegal bufferSize");
    }

    this.compressor = new SnappyCompressor(initialBufferSize);
    this.bufferSize = initialBufferSize;
    this.buffer = new byte[bufferSize];
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (len > bufferSize) {
      bufferSize = len;
      compressor = new SnappyCompressor(bufferSize);
      buffer = new byte[bufferSize];
    }
    while (true) {
      try {
	compressor.setInput(b, off, len);
	do {
	  compress();
	} while (!compressor.needsInput());
	break;
      } catch (NoClassDefFoundError e) {
	bufferSize *= 2;
	compressor = new SnappyCompressor(bufferSize);
	buffer = new byte[bufferSize];
      }
    }
  }

  @Override
  protected void compress() throws IOException {
    int len = compressor.compress(buffer, 0, buffer.length);
    if (len > 0) {
      out.write(buffer, 0, len);
    }
  }

  @Override
  public void finish() throws IOException {
    if (!compressor.finished()) {
      compressor.finish();
      while (!compressor.finished()) {
	compress();
      }
    }
  }

  @Override
  public void resetState() throws IOException {
    compressor.reset();
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      finish();
      out.close();
      closed = true;
    }
  }

  private byte[] oneByte = new byte[1];
  @Override
  public void write(int b) throws IOException {
    oneByte[0] = (byte)(b & 0xff);
    write(oneByte, 0, oneByte.length);
  }
}
