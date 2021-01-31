package com.pinterest.rocksplicator.shardmap;

public interface ResourceMapBuilder {
  PartitionBuilder addResource(String resourceName, int num_shards);
  ShardMapBuilder build();
}
