package com.pinterest.rocksplicator.shardmap;

public interface PartitionBuilder {
  ReplicaBuilder addPartition(String partitionName);
  ResourceMapBuilder build();
}
