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

package com.pinterest.rocksplicator.shardmap;

import com.pinterest.rocksplicator.thrift.shardmap.CDomain;
import com.pinterest.rocksplicator.thrift.shardmap.CHost;
import com.pinterest.rocksplicator.thrift.shardmap.CReplica;
import com.pinterest.rocksplicator.thrift.shardmap.CReplicaType;
import com.pinterest.rocksplicator.thrift.shardmap.CResourceMap;
import com.pinterest.rocksplicator.thrift.shardmap.CShardMap;
import com.pinterest.rocksplicator.thrift.shardmap.TInstance;
import com.pinterest.rocksplicator.thrift.shardmap.TPartition;
import com.pinterest.rocksplicator.thrift.shardmap.TReplica;
import com.pinterest.rocksplicator.thrift.shardmap.TReplicaType;
import com.pinterest.rocksplicator.thrift.shardmap.TResourceMap;
import com.pinterest.rocksplicator.thrift.shardmap.TShardMap;

import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ShardMaps {

  public static ShardMap fromJson(JSONObject shardMapJsonObject) {
    return new JsonShardMap(shardMapJsonObject);
  }

  public static TShardMap toTShardMap(ShardMap sm) {
    TShardMap shardMap = new com.pinterest.rocksplicator.thrift.shardmap.TShardMap();

    for (String resource : sm.getResources()) {
      ResourceMap rm = sm.getResourceMap(resource);
      TResourceMap trm = new TResourceMap();

      trm.setNum_shards(rm.getNumShards());
      trm.setResource_name(rm.getResource());
      trm.setPartitions_map(new HashMap<>());

      for (Partition partition : rm.getAllKnownPartitions()) {
        List<Replica> replicas = rm.getAllReplicasForPartition(partition);

        TPartition tPartition = new TPartition().setPartitionName(partition.getPartitionName());

        List<TReplica> tReplicas = replicas.stream().map(new Function<Replica, TReplica>() {
          @Override
          public TReplica apply(Replica replica) {
            TReplica tReplica = new TReplica();
            ReplicaState state = replica.getReplicaState();

            /**
             * First set the Replica Type
             */

            if (state == ReplicaState.LEADER) {
              tReplica.setReplicaType(TReplicaType.LEADER);
            } else if (state == ReplicaState.FOLLOWER) {
              tReplica.setReplicaType(TReplicaType.FOLLOWER);
            } else if (state == ReplicaState.ONLINE) {
              tReplica.setReplicaType(TReplicaType.ONLINE);
            }

            /**
             * Next set the Instance
             */
            Instance instance = replica.getInstance();
            TInstance tInstance = new TInstance()
                .setHost(instance.getHost())
                .setPort((short) instance.getPort())
                .setDomain(instance.getDomain());

            tReplica.setInstance(tInstance).setPartition(tPartition);

            return tReplica;
          }
        }).collect(Collectors.toList());

        trm.putToPartitions_map(partition.getPartitionName(), tReplicas);
      }
      shardMap.putToResource_map(resource, trm);
    }

    return shardMap;
  }

  public static CShardMap toCShardMap(ShardMap shardMap) {
    CShardMap cShardMap = new CShardMap();

    cShardMap.setResourceMap(new HashMap<>());

    Map<CHost, Integer> hostToIndexMap = new HashMap<>();
    Map<CDomain, Integer> domainToIndexMap = new HashMap<>();

    List<CHost> hosts = new LinkedList<>();
    List<CDomain> domains = new ArrayList<>();

    for (String resource : shardMap.getResources()) {
      ResourceMap resourceMap = shardMap.getResourceMap(resource);

      int num_shards = resourceMap.getNumShards();
      List<List<CReplica>> creplicas = new ArrayList<>(num_shards);
      CResourceMap cResourceMap = new CResourceMap().setReplicas(creplicas);
      cShardMap.putToResourceMap(resource, cResourceMap);

      for (int i = 0; i < num_shards; ++i) {
        creplicas.add(new ArrayList<>(3));
      }

      for (Partition partition : resourceMap.getAllKnownPartitions()) {
        List<Replica> replicas = resourceMap.getAllReplicasForPartition(partition);

        int partitionId = Integer.parseInt(partition.getPartitionName().substring(resource.length() + 1));

        for (Replica replica : replicas) {
          Instance instance = replica.getInstance();
          ReplicaState state = replica.getReplicaState();

          String domain = instance.getDomain();
          int domainIndex=-1;
          CDomain cDomain = new CDomain().setDomain(domain);
          if (!domainToIndexMap.containsKey(cDomain)) {
            domainIndex = domains.size();
            domains.add(cDomain);
          } else {
            domainIndex = domainToIndexMap.get(domain);
          }

          long longIP = 0;

          String[] octetArray = instance.getHost().split("\\.");
          for (String string : octetArray){
            long octet = Long.parseLong(string);
            longIP &= octet;
            longIP <<= 8;
          }

          CHost cHost = new CHost()
              .setHost((int)longIP)
              .setPort( (short) instance.getPort())
              .setDomainIndex( (short) domainIndex);

          int hostIndex = -1;
          if (!hostToIndexMap.containsKey(cHost)) {
            hostIndex = hosts.size();
            hosts.add(cHost);
          } else {
            hostIndex = hostToIndexMap.get(cHost);
          }

          CReplicaType cReplicaType = null;
          if (state == ReplicaState.LEADER) {
            cReplicaType = CReplicaType.LEADER;
          } else if (state == ReplicaState.FOLLOWER) {
            cReplicaType = CReplicaType.FOLLOWER;
          } else if (state == ReplicaState.ONLINE) {
            cReplicaType = CReplicaType.ONLINE;
          }

          CReplica cReplica = new CReplica()
              .setReplicaType(cReplicaType).setHostsIndex((short) hostIndex);

          creplicas.get(partitionId).add(cReplica);
        }
      }
    }

    cShardMap.setHosts(hosts);
    cShardMap.setDomains(domains);

    return cShardMap;
  }
}
