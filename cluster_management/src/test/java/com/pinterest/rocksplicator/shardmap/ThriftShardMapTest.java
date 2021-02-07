package com.pinterest.rocksplicator.shardmap;

import com.pinterest.rocksplicator.codecs.Codec;
import com.pinterest.rocksplicator.codecs.CodecException;
import com.pinterest.rocksplicator.codecs.Codecs;
import com.pinterest.rocksplicator.codecs.SimpleJsonObjectDecoder;
import com.pinterest.rocksplicator.thrift.commons.io.CompressionAlgorithm;
import com.pinterest.rocksplicator.thrift.commons.io.SerializationProtocol;
import com.pinterest.rocksplicator.thrift.shardmap.CShardMap;
import com.pinterest.rocksplicator.thrift.shardmap.TShardMap;

import com.google.common.base.Stopwatch;
import org.json.simple.JSONObject;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

public class ThriftShardMapTest {
  private static File file = new File(
      "/Users/grajpurohit/shard_maps/config.manageddata.rocksplicator"
          + ".rockstore-shared-readonly2");

  private static byte[] fileData;
  private static JSONObject jsonShardMap;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    InputStream is = new FileInputStream(file);

    int read = -1;
    while ((read = is.read()) != -1) {
      stream.write(read);
    }
    stream.close();
    fileData = stream.toByteArray();

    SimpleJsonObjectDecoder decoder = new SimpleJsonObjectDecoder();
    jsonShardMap = decoder.decode(fileData);
  }

  @Test
  public void testJShardMap() throws Exception {
    Codec<JSONObject, byte[]> jsonCodec = new Codec<JSONObject, byte[]>() {

      @Override
      public byte[] encode(JSONObject obj) throws CodecException {
        return obj.toJSONString().getBytes();
      }

      @Override
      public JSONObject decode(byte[] data) throws CodecException {
        return new SimpleJsonObjectDecoder().decode(data);
      }
    };
    print(jsonCodec, jsonShardMap, "JShardMap  Textual\t\t");
  }


  @Test
  public void testCShardMap() throws Exception {
    ShardMap shardMap = ShardMaps.fromJson(jsonShardMap);
    CShardMap cShardMap = ShardMaps.toCShardMap(shardMap);

    Codec<CShardMap, byte[]>
        binaryCodec =
        Codecs.createThriftCodec(CShardMap.class, SerializationProtocol.BINARY);
    Codec<CShardMap, byte[]>
        compactCodec =
        Codecs.createThriftCodec(CShardMap.class, SerializationProtocol.COMPACT);

    Codec<CShardMap, byte[]>
        wrappedBinaryGZIPCodec =
        Codecs.getCompressedCodec(binaryCodec, CompressionAlgorithm.GZIP);
    Codec<CShardMap, byte[]>
        wrappedBinarySnappyCodec =
        Codecs.getCompressedCodec(binaryCodec, CompressionAlgorithm.SNAPPY);
    Codec<CShardMap, byte[]>
        wrappedBinaryBZip2Codec =
        Codecs.getCompressedCodec(binaryCodec, CompressionAlgorithm.BZIP2);
    Codec<CShardMap, byte[]>
        wrappedCompactGZIPCodec =
        Codecs.getCompressedCodec(compactCodec, CompressionAlgorithm.GZIP);
    Codec<CShardMap, byte[]>
        wrappedCompactSnappyCodec =
        Codecs.getCompressedCodec(compactCodec, CompressionAlgorithm.SNAPPY);
    Codec<CShardMap, byte[]>
        wrappedCompactBZip2Codec =
        Codecs.getCompressedCodec(compactCodec, CompressionAlgorithm.BZIP2);

    print(binaryCodec, cShardMap, "CShardMap Binary\t\t");
    print(compactCodec, cShardMap, "CShardMap Compact\t\t");

    print(wrappedBinaryGZIPCodec, cShardMap, "CShardMap BinaryGZIP\t");
    print(wrappedCompactGZIPCodec, cShardMap, "CShardMap CompactGZIP\t");

    print(wrappedBinarySnappyCodec, cShardMap, "CShardMap BinarySnappy\t");
    print(wrappedCompactSnappyCodec, cShardMap, "CShardMap CompactSnappy\t");

    print(wrappedBinaryBZip2Codec, cShardMap, "CShardMap BinaryBZip2\t");
    print(wrappedCompactBZip2Codec, cShardMap, "CShardMap CompactBZip2\t");
  }

  @Test
  public void testTShardMap() throws Exception {
    ShardMap shardMap = ShardMaps.fromJson(jsonShardMap);
    TShardMap tShardMap = ShardMaps.toTShardMap(shardMap);

    Codec<TShardMap, byte[]>
        binaryCodec =
        Codecs.createThriftCodec(TShardMap.class, SerializationProtocol.BINARY);
    Codec<TShardMap, byte[]>
        compactCodec =
        Codecs.createThriftCodec(TShardMap.class, SerializationProtocol.COMPACT);

    Codec<TShardMap, byte[]>
        wrappedBinaryGZIPCodec =
        Codecs.getCompressedCodec(binaryCodec, CompressionAlgorithm.GZIP);
    Codec<TShardMap, byte[]>
        wrappedBinarySnappyCodec =
        Codecs.getCompressedCodec(binaryCodec, CompressionAlgorithm.SNAPPY);
    Codec<TShardMap, byte[]>
        wrappedBinaryBZip2Codec =
        Codecs.getCompressedCodec(binaryCodec, CompressionAlgorithm.BZIP2);
    Codec<TShardMap, byte[]>
        wrappedCompactGZIPCodec =
        Codecs.getCompressedCodec(compactCodec, CompressionAlgorithm.GZIP);
    Codec<TShardMap, byte[]>
        wrappedCompactSnappyCodec =
        Codecs.getCompressedCodec(compactCodec, CompressionAlgorithm.SNAPPY);
    Codec<TShardMap, byte[]>
        wrappedCompactBZip2Codec =
        Codecs.getCompressedCodec(compactCodec, CompressionAlgorithm.BZIP2);

    print(binaryCodec, tShardMap, "TShardMap Binary\t\t");
    print(compactCodec, tShardMap, "TShardMap Compact\t\t");

    print(wrappedBinaryGZIPCodec, tShardMap, "TShardMap BinaryGZIP\t");
    print(wrappedCompactGZIPCodec, tShardMap, "TShardMap CompactGZIP\t");

    print(wrappedBinarySnappyCodec, tShardMap, "TShardMap BinarySnappy\t");
    print(wrappedCompactSnappyCodec, tShardMap, "TShardMap CompactSnappy\t");

    print(wrappedBinaryBZip2Codec, tShardMap, "TShardMap BinaryBZip2\t");
    print(wrappedCompactBZip2Codec, tShardMap, "TShardMap CompactBZip2\t");
  }


  private <T> void print(Codec<T, byte[]> shardMapCodec, T object, String prefix)
      throws Exception {
    Stopwatch stopwatch = Stopwatch.createUnstarted();
    stopwatch.start();
    byte[] ser = shardMapCodec.encode(object);
    long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
    System.out.println(
        String.format("%s Encode: time: %8d\t length= %10d", prefix, elapsed, ser.length));

    stopwatch.reset();
    stopwatch.start();
    T newCShardMap = shardMapCodec.decode(ser);
    elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
    byte[] newSer = shardMapCodec.encode(newCShardMap);
    System.out.println(
        String.format("%s Decode: time: %8d\t length= %10d", prefix, elapsed, newSer.length));
  }
}

