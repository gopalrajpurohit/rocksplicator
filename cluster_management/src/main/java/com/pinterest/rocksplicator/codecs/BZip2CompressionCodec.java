package com.pinterest.rocksplicator.codecs;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class BZip2CompressionCodec<S> extends AbstractCompressionCodec<S> {

  BZip2CompressionCodec(Codec<S, byte[]> delegate) {
    super(delegate);
  }

  @Override
  protected OutputStream createCompressedOutputStream(OutputStream stream) throws IOException {
    return new BZip2CompressorOutputStream(stream);
  }

  @Override
  protected InputStream createDecompressedInputStream(InputStream stream) throws IOException {
    return new BZip2CompressorInputStream(stream);
  }
}
