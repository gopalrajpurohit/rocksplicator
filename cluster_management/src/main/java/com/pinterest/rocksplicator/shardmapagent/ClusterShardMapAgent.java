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

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import com.pinterest.rocksplicator.ShardMapAgent;
import com.pinterest.rocksplicator.codecs.CodecException;
import com.pinterest.rocksplicator.codecs.ZkGZIPCompressedShardMapCodec;
import com.pinterest.rocksplicator.utils.ZkPathUtils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.utils.CloseableExecutorService;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClusterShardMapAgent implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ShardMapAgent.class);

  private final boolean CACHE_DATA = true;
  private final boolean DO_NOT_COMPRESS = false;

  private final String shardMapDir;
  private final String tempShardMapDir;
  private final String clusterName;
  private final String zkConnectString;
  private final CuratorFramework zkShardMapClient;
  private final PathChildrenCache pathChildrenCache;
  private final ExecutorService executorService;
  private final ConcurrentHashMap<String, JSONObject> shardMapsByResources;
  private final ZkGZIPCompressedShardMapCodec gzipCodec;

  public ClusterShardMapAgent(String zkConnectString, String clusterName, String shardMapDir)
      throws Exception {
    this.clusterName = clusterName;
    this.shardMapDir = shardMapDir;
    this.tempShardMapDir = shardMapDir + "/" + ".temp";
    this.zkConnectString = zkConnectString;

    this.zkShardMapClient = CuratorFrameworkFactory
        .newClient(this.zkConnectString,
            new BoundedExponentialBackoffRetry(
                100, 10000, 10));

    this.zkShardMapClient.start();
    this.zkShardMapClient.blockUntilConnected(60, TimeUnit.SECONDS);

    this.executorService = Executors.newSingleThreadExecutor();
    this.shardMapsByResources = new ConcurrentHashMap<>();
    this.gzipCodec = new ZkGZIPCompressedShardMapCodec();

    this.pathChildrenCache = new PathChildrenCache(
        zkShardMapClient,
        ZkPathUtils.getClusterShardMapParentPath(this.clusterName),
        CACHE_DATA, DO_NOT_COMPRESS,
        new CloseableExecutorService(executorService));

    new File(this.shardMapDir).mkdirs();
    new File(this.tempShardMapDir).mkdirs();
  }

  public void startNotification() throws Exception {
    final AtomicBoolean initialized = new AtomicBoolean(false);
    LOG.error(String.format("Initializing shardMap for cluster=%s", clusterName));
    this.pathChildrenCache.getListenable()
        .addListener(new PathChildrenCacheListener() {
          @Override
          public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
              throws Exception {
            switch (event.getType()) {
              case INITIALIZED: {
                List<ChildData> childrenData = event.getInitialData();
                if (childrenData != null) {
                  for (ChildData childData : childrenData) {
                    if (childData != null) {
                      add(childData.getPath(), childData.getData());
                    }
                  }
                }
              }
              dump();
              initialized.set(true);
              break;
              case CHILD_ADDED:
              case CHILD_UPDATED: {
                ChildData childData = event.getData();
                if (childData != null) {
                  add(childData.getPath(), childData.getData());
                }
              }
              if (initialized.get()) {
                dump();
              }
              break;
              case CHILD_REMOVED: {
                ChildData childData = event.getData();
                if (childData != null) {
                  remove(childData.getPath());
                }
              }
              if (initialized.get()) {
                dump();
              }
              break;
            }
          }
        });

    /**
     * This will initialize the cluster data in background.
     */
    this.pathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);

    while (!initialized.get()) {
      Thread.sleep(100);
    }
    LOG.error(String.format("Initialized shardMap for cluster=%s", clusterName));
  }

  private void add(String resourcePath, byte[] data) {
    if (data == null) {
      return;
    }
    try {
      String[] splits = resourcePath.split("/");
      if (splits.length <= 0) {
        return;
      }
      String resourceName = splits[splits.length - 1];
      try {
        JSONObject jsonObject = gzipCodec.decode(data);
        this.shardMapsByResources.put(resourceName, jsonObject);
      } catch (CodecException e) {
        e.printStackTrace();
      }
    } catch (Throwable throwable) {

    }
  }

  private void remove(String resourcePath) {
    try {
      String[] splits = resourcePath.split("/");
      if (splits.length <= 0) {
        return;
      }
      String resourceName = splits[splits.length - 1];
      this.shardMapsByResources.remove(resourceName);
    } catch (Throwable throwable) {

    }
  }

  /**
   * TODO: grajpurohit : potential to improve and make it more efficient.
   *
   * If this becomes too heavy, we may consider a better approach by
   * 1. enqueue dump calls in a queue.
   * 2. a separate thread to wake up and watch the queue...
   * 3. If there n items in the queue, discard first n-1 items and pickup first
   * one after discarding all but last one.
   * 4. Only process the last call and dump the data from shardMapsByResource..
   * This will prevent multiple updates to the file, when there are very fast changes
   * coming in but we can't keep up with dumping the data on the disk.
   */
  private void dump() {
    try {
      Map<String, JSONObject> localCopy = new HashMap<>(this.shardMapsByResources);

      // Create a Cluster ShardMap.
      JSONObject clusterShardMap = new JSONObject();

      for (Map.Entry<String, JSONObject> entry : localCopy.entrySet()) {
        String resourceName = entry.getKey();
        JSONObject resourceMap = (JSONObject) entry.getValue().get("shard_map");
        clusterShardMap.put(resourceName, resourceMap.get(resourceName));
      }

      try {
        File tempFile = File.createTempFile(
            clusterName,
            "-" + Long.toString(System.currentTimeMillis()),
            new File(tempShardMapDir));

        LOG.error(String.format(
            "Dumping new shard_map for cluster=%s at file_location: %s",
            clusterName, tempFile.getPath()));

        FileWriter fileWriter = new FileWriter(tempFile);
        fileWriter.write(clusterShardMap.toJSONString());
        fileWriter.close();

        // Now move the dumped data file to intended file.
        File finalDestinationFile = new File(shardMapDir, clusterName);
        LOG.error(String.format(
            "Moving shard_map file for cluster=%s from: %s -> to: %s",
            clusterName, tempFile.getPath(),
            finalDestinationFile.getPath()));

        try {
          Files.move(tempFile.toPath(), finalDestinationFile.toPath(), ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException ae) {
          Files.move(tempFile.toPath(), finalDestinationFile.toPath(), REPLACE_EXISTING);
        }
      } catch (IOException e) {
        LOG.error(String.format("Error dumping shard_map for cluster=%s", clusterName), e);
      }
    } catch (Throwable throwable) {
      LOG.error(String.format("Error dumping shard_map for cluster=%s", clusterName), throwable);
    }
  }

  @Override
  public void close() throws IOException {
    this.pathChildrenCache.close();
    this.zkShardMapClient.close();
  }
}
