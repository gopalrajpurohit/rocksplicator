package com.pinterest.rocksplicator.codecs;

import com.pinterest.rocksplicator.thrift.commons.io.CompressionAlgorithm;

import org.json.simple.JSONObject;

public class ZkBZIP2CompressedShardMapCodec extends ZkShardMapCodec {

  private static final Codec<JSONObject, byte[]> baseCodec = new SimpleJsonObjectByteArrayCodec();
  private static final Codec<JSONObject, byte[]> bzippedCompressedCoded =
      Codecs.getCompressedCodec(baseCodec, CompressionAlgorithm.BZIP2);


  @Override
  public JSONObject decode(byte[] data) throws CodecException {
    return bzippedCompressedCoded.decode(data);
  }

  @Override
  public byte[] encode(JSONObject obj) throws CodecException {
    return bzippedCompressedCoded.encode(obj);
  }
}

