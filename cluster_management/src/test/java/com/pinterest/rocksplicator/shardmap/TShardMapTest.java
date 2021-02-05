package com.pinterest.rocksplicator.shardmap;

import com.pinterest.rocksplicator.codecs.Codec;
import com.pinterest.rocksplicator.codecs.Codecs;
import com.pinterest.rocksplicator.codecs.SimpleJsonObjectDecoder;
import com.pinterest.rocksplicator.thrift.commons.io.CompressionAlgorithm;
import com.pinterest.rocksplicator.thrift.commons.io.SerializationProtocol;
import com.pinterest.rocksplicator.thrift.shardmap.TShardMap;

import com.google.common.base.Stopwatch;
import org.json.simple.JSONObject;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

public class TShardMapTest {

  @Test
  public void test() throws Exception {
    File file = new File(
        "/Users/grajpurohit/shard_maps/config.manageddata.rocksplicator"
            + ".rockstore-shared-readonly2");

    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    InputStream is = new FileInputStream(file);

    int read = -1;
    while ((read = is.read()) != -1) {
      stream.write(read);
    }
    stream.close();
    byte[] data = stream.toByteArray();

    SimpleJsonObjectDecoder decoder = new SimpleJsonObjectDecoder();

    Stopwatch stopwatch = Stopwatch.createUnstarted();

    stopwatch.start();
    JSONObject jsonShardMap = decoder.decode(data);
    long elapsed = stopwatch.elapsed(TimeUnit.MICROSECONDS);
    System.out.println(String.format("JSON Deserialize: time: %d", elapsed));

    stopwatch.reset();
    stopwatch.start();
    byte[] ser = jsonShardMap.toJSONString().getBytes();
    elapsed = stopwatch.elapsed(TimeUnit.MICROSECONDS);
    System.out.println(String.format("JSON Serialize: time: %d\t length=%d", elapsed, ser.length));

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
        wrappedCompactGZIPCodec =
        Codecs.getCompressedCodec(compactCodec, CompressionAlgorithm.GZIP);
    Codec<TShardMap, byte[]>
        wrappedCompactSnappyCodec =
        Codecs.getCompressedCodec(compactCodec, CompressionAlgorithm.SNAPPY);

    print(binaryCodec, tShardMap, "Binary");
    print(compactCodec, tShardMap, "Compact");

    print(wrappedBinaryGZIPCodec, tShardMap, "BinaryGZIP");
    print(wrappedBinarySnappyCodec, tShardMap, "BinarySnappy");

    print(wrappedCompactGZIPCodec, tShardMap, "CompactGZIP");
    print(wrappedCompactSnappyCodec, tShardMap, "CompactSnappy");
  }

  private void print(Codec<TShardMap, byte[]> tShardMapCodec, TShardMap thrift, String prefix)
      throws Exception {
    Stopwatch stopwatch = Stopwatch.createUnstarted();
    stopwatch.start();
    byte[] ser = tShardMapCodec.encode(thrift);
    long elapsed = stopwatch.elapsed(TimeUnit.MICROSECONDS);
    System.out
        .println(String.format("%s Serialize: time: %d\t length=%d", prefix, elapsed, ser.length));

    stopwatch.reset();
    stopwatch.start();
    TShardMap newTShardMap = tShardMapCodec.decode(ser);
    elapsed = stopwatch.elapsed(TimeUnit.MICROSECONDS);
    byte[] newSer = tShardMapCodec.encode(newTShardMap);
    System.out.println(
        String.format("%s DeSerialize: time: %d\t length=%d", prefix, elapsed, newSer.length));
  }
}
