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

package com.pinterest.rocksplicator.publisher;

import com.google.common.base.Preconditions;
import org.apache.helix.model.ExternalView;

import java.util.List;
import java.util.Set;

public class ParallelShardMapPublisher<T> implements ShardMapPublisher<T> {

  private final List<ShardMapPublisher<T>> shardMapPublishers;

  public ParallelShardMapPublisher(final List<ShardMapPublisher<T>> shardMapPublishers) {
    Preconditions.checkNotNull(shardMapPublishers);
    this.shardMapPublishers = shardMapPublishers;
  }

  @Override
  public void publish(final Set<String> validResources,
                      final List<ExternalView> externalViews,
                      final T shardMap) {
    for (ShardMapPublisher<T> shardMapPublisher : shardMapPublishers) {
      try {
        shardMapPublisher.publish(validResources, externalViews, shardMap);
      } catch (Throwable throwable) {
        // Ensure that failure of one publisher doesn't effect the other
      }
    }
  }
}