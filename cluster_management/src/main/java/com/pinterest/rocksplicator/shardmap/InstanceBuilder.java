package com.pinterest.rocksplicator.shardmap;

public interface InstanceBuilder {
  DomainBuilder onInstance(String host, int port);
}
