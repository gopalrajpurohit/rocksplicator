# Copyright 2021 Pinterest Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

//
// @author Gopal Rajpurohit (grajpurohit@pinterest.com)
//

namespace java com.pinterest.rocksplicator.thrift.shardmap

//*****************//

enum TReplicaType {
  LEADER = 1,
  FOLLOWER = 2,
  ONLINE = 3
}

struct TInstance {
  1: required string host,
  2: required i16 port,
  3: optional string domain
}

struct TReplica {
  3: required TReplicaType replicaType,
  1: required TPartition partition,
  2: required TInstance instance,
}

struct TPartition {
  1: required string partitionName
}

struct TResourceMap {
  1: required i32 num_shards,
  2: required string resource_name,
  3: required map<string, list<TReplica>> partitions_map,
}

struct TShardMap {
  1: map<string, TResourceMap> resource_map;
}

//**************//

enum CReplicaType {
  LEADER = 1,
  FOLLOWER = 2,
  ONLINE = 3
}

struct CDomain {
  1: string domain,
}

struct CHost {
  1: required i32 host,
  2: required i16 port,
  3: required i16 domainIndex,
}

struct CReplica {
  1: required i16 hostsIndex,
  2: required CReplicaType replicaType,
}

struct CResourceMap {
  1: required list<list<CReplica>> replicas;
}

struct CShardMap {
  1: required list<CHost> hosts,
  2: required list<CDomain> domains,
  3: required map<string, CResourceMap> resourceMap,
}

//**************//





