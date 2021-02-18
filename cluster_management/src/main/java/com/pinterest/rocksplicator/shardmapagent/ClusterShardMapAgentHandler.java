/// Copyright 2021 Pinterest Inc.
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
/// http://www.apache.org/licenses/LICENSE-2.0

/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.

//
// @author Gopal Rajpurohit (grajpurohit@pinterest.com)
//

package com.pinterest.rocksplicator.shardmapagent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOG = LoggerFactory.getLogger(ClusterShardMapAgentHandler.class);

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
        try {
          update();
        } catch (Throwable throwable) {
          LOG.error("Error while updating cluster to watch list");
        }
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
        LOG.error(String.format("Stop Watching cluster: %s", cluster));
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
        LOG.error(String.format("Start Watching cluster: %s", cluster));
        ClusterShardMapAgent agent = new ClusterShardMapAgent(this.zkShardMapSvr, cluster, shardMapDir);
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
        LOG.error(String.format("Closing: Stop Watching cluster: %s", cluster));
        clusterAgents.get(cluster).close();
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }
    clusterAgents.clear();
  }
}
