/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.classification.InterfaceAudience;

import java.util.Collection;

/**
 * Given a set of checkable resources, this class is capable of determining
 * whether sufficient resources are available for the NN to continue operating.
 */
@InterfaceAudience.Private
final class NameNodeResourcePolicy {

  /**
   * Return true if and only if there are sufficient NN
   * resources to continue logging edits.
   * 
   * @param resources the collection of resources to check.
   * @param minimumRedundantResources the minimum number of redundant resources
   *        required to continue operation.
   * @return true if and only if there are sufficient NN resources to
   *         continue logging edits.
   */
  static boolean areResourcesAvailable(
      Collection<? extends CheckableNameNodeResource> resources,
      int minimumRedundantResources) {
    // TODO: workaround:
    // - during startup, if there are no edits dirs on disk, then there is a call to areResourcesAvailable() with no dirs at all, which was previously causing the NN to enter safemode
    if (resources.isEmpty()) {
      return true;
    }
    int requiredResourceCount = 0; // 必要资源数量
    int redundantResourceCount = 0;// 冗余资源数量
    int disabledRedundantResourceCount = 0;// 冗余资源无效数量
    for (CheckableNameNodeResource resource : resources) {
      if (!resource.isRequired()) {
        redundantResourceCount++;//TODO 目录资源检查: 不是必要资源，那么冗余资源+1
        if (!resource.isResourceAvailable()) {
          disabledRedundantResourceCount++;
        }
      } else {
        requiredResourceCount++;//TODO 目录资源检查: 必要资源+1
        if (!resource.isResourceAvailable()) {
          //TODO 目录资源检查: 必要资源无效直接返回false
          return false;
        }
      }
    }
    if (redundantResourceCount == 0) {
      // If there are no redundant resources, return true if there are any required resources available.
      //TODO 目录资源检查: 如果没有冗余的资源，只要有任一必要资源则返回True
      return requiredResourceCount > 0;
    } else {
      //TODO 目录资源检查: 如果有冗余的资源，那么 冗余资源 - 无效资源>= 最小冗余 就返回True
      return redundantResourceCount - disabledRedundantResourceCount >= minimumRedundantResources;
    }
  }
}
