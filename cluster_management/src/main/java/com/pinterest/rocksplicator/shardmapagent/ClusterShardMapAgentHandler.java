package com.pinterest.rocksplicator.shardmapagent;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class ClusterShardMapAgentHandler implements Closeable {

  private final String shardMapDir;
  private final String zkShardMapSvr;
  private final Supplier<Set<String>> clustersSupplier;
  private final ConcurrentHashMap<String, ClusterShardMapAgent> clusterAgents;

  private final ScheduledExecutorService scheduledExecutorService
      = Executors.newSingleThreadScheduledExecutor();

  public ClusterShardMapAgentHandler(
      final String zkShardMapSvr,
      final String shardMapDir,
      final Supplier<Set<String>> clustersSupplier) {
    this.zkShardMapSvr = zkShardMapSvr;
    this.shardMapDir = shardMapDir;
    this.clustersSupplier = clustersSupplier;
    this.clusterAgents = new ConcurrentHashMap<>();

    update();

    scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        update();
      }
    }, 60, 60, TimeUnit.SECONDS);
  }

  private void update() {
    Set<String> clustersWithAgents = new HashSet<>(clusterAgents.keySet());
    Set<String> clustersRequiringAgents = new HashSet(clustersSupplier.get());

    // First remove the agents that are no longer required.
    for (String cluster : clustersWithAgents) {

      // Do not remove the cluster agents which are required to be available.
      if (clustersRequiringAgents.contains(cluster)) {
        continue;
      }

      try {
        clusterAgents.remove(cluster).close();
      } catch (Throwable throwable) {
        throwable.printStackTrace();
      }
    }

    //Second construct the agents that are required but not available
    for (String cluster : clustersRequiringAgents) {
      if (clusterAgents.contains(cluster)) {
        // Already the agent is available. No need to create another agent for same cluster.
        continue;
      }

      try {
        ClusterShardMapAgent agent =
            new ClusterShardMapAgent(this.zkShardMapSvr, cluster, shardMapDir);
        agent.startNotification();
        clusterAgents.put(cluster, agent);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void close() throws IOException {
    scheduledExecutorService.shutdown();
    while (!scheduledExecutorService.isTerminated()) {
      try {
        scheduledExecutorService.awaitTermination(100, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    for (String cluster : clusterAgents.keySet()) {
      try {
        clusterAgents.get(cluster).close();
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }
    clusterAgents.clear();
  }
}
