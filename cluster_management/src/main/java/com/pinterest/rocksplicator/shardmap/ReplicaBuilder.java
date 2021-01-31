package com.pinterest.rocksplicator.shardmap;

public interface ReplicaBuilder {
  InstanceBuilder addReplica(String state);
  PartitionBuilder build();
}
