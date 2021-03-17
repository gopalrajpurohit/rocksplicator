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

package com.pinterest.rocksplicator.utils;

public class ZkPathUtils {
<<<<<<< HEAD
  private static final String BASE_PATH_PER_RESOURCE_GZIPPED_SHARD_MAP
=======

  private static final String BASE_PATH_PER_RESOURCE_SHARD_MAP
>>>>>>> grajpurohit/rocksplicator/client_agent_downloading_zk_compressed_shard_map
      = "/rocksplicator-shard_map/gzipped-json/byResource";
  private static final String BASE_PATH_PER_RESOURCE_BZIPPED_SHARD_MAP
      = "/rocksplicator-shard_map/bzip2-json/byResource";

  public static String getClusterShardMapParentPath(String clusterName, boolean bzipped) {
    return String.format("%s/%s",
        (bzipped)? BASE_PATH_PER_RESOURCE_BZIPPED_SHARD_MAP : BASE_PATH_PER_RESOURCE_GZIPPED_SHARD_MAP,
        clusterName);
  }
<<<<<<< HEAD
  public static String getClusterResourceShardMapPath(String clusterName, String resourceName, boolean bzipped) {
    return String.format("%s/%s/%s",
        (bzipped) ? BASE_PATH_PER_RESOURCE_BZIPPED_SHARD_MAP : BASE_PATH_PER_RESOURCE_GZIPPED_SHARD_MAP,
        clusterName, resourceName);
=======

  public static String getClusterResourceShardMapPath(String clusterName, String resourceName) {
    return String.format("%s/%s/%s", BASE_PATH_PER_RESOURCE_SHARD_MAP, clusterName, resourceName);
>>>>>>> grajpurohit/rocksplicator/client_agent_downloading_zk_compressed_shard_map
  }
}
