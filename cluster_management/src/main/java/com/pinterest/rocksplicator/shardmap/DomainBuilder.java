package com.pinterest.rocksplicator.shardmap;

public interface DomainBuilder {
  DomainBuilder withDomain(String domain);
  DomainBuilder withAvailabilityZone(String availability_zone);
  DomainBuilder withPlacementGroup(String placement_group);
  ReplicaBuilder build();
}
