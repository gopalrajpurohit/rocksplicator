package com.pinterest.rocksplicator.shardmap;

public class DoNothing {
  public void doNothing() {
    ShardMapBuilder shardMapBuilder = null;

    shardMapBuilder = shardMapBuilder.resourceBuilder()
        .addResource("grajpurohit", 10)
        .addPartition("grajpurohit_0")

        .addReplica("MASTER")
        .onInstance("10.2.2.0", 9090)
        .withAvailabilityZone("az")
        .withPlacementGroup("pg")
        .build()

        .addReplica("SLAVE")
        .onInstance("10.2.2.1", 9090)
        .withAvailabilityZone("az")
        .withPlacementGroup("pg")
        .withDomain("domain")
        .build()

        .build()

        .build()

        .build();

  }
}
