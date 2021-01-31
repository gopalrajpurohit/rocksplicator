package com.pinterest.rocksplicator.shardmap;

/**
 * Represents a shardMap for entire cluster. Internal representation can vary from implementation
 * to implementation. Currently we only support JSON representation on the disk.
 */
public interface ShardMapBuilder {
  ResourceMapBuilder resourceBuilder();
}
