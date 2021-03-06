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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.FSClusterStats;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.util.ReflectionUtils;

/** 
 * This interface is used for choosing the desired number of targets
 * for placing block replicas.
 */
@InterfaceAudience.Private
public abstract class BlockPlacementPolicy {
  static final Log LOG = LogFactory.getLog(BlockPlacementPolicy.class);

  @InterfaceAudience.Private
  public static class NotEnoughReplicasException extends Exception {
    private static final long serialVersionUID = 1L;
    NotEnoughReplicasException(String msg) {
      super(msg);
    }
  }
    
  /**
   * choose <i>numOfReplicas</i> data nodes for <i>writer</i> 
   * to re-replicate a block with size <i>blocksize</i> 
   * If not, return as many as we can.
   * 
   * @param srcPath the file to which this chooseTargets is being invoked. 
   * @param numOfReplicas additional number of replicas wanted.
   * @param writer the writer's machine, null if not in the cluster.
   * @param chosenNodes datanodes that have been chosen as targets.
   * @param blocksize size of the data to be written.
   * @return array of DatanodeDescriptor instances chosen as target 
   * and sorted as a pipeline.
   */
  abstract DatanodeDescriptor[] chooseTarget(String srcPath,
                                             int numOfReplicas,
                                             DatanodeDescriptor writer,
                                             List<DatanodeDescriptor> chosenNodes,
                                             long blocksize);

  /**
   * choose <i>numOfReplicas</i> data nodes for <i>writer</i> 
   * to re-replicate a block with size <i>blocksize</i> 
   * If not, return as many as we can.
   *
   * @param srcPath the file to which this chooseTargets is being invoked.
   * @param numOfReplicas additional number of replicas wanted.
   * @param writer the writer's machine, null if not in the cluster.
   * @param chosenNodes datanodes that have been chosen as targets.
   * @param returnChosenNodes decide if the chosenNodes are returned.
   * @param excludedNodes datanodes that should not be considered as targets.
   * @param blocksize size of the data to be written.
   * @return array of DatanodeDescriptor instances chosen as target
   * and sorted as a pipeline.
   */
  public abstract DatanodeDescriptor[] chooseTarget(String srcPath,
                                             int numOfReplicas,
                                             DatanodeDescriptor writer,
                                             List<DatanodeDescriptor> chosenNodes,
                                             boolean returnChosenNodes,
                                             HashMap<Node, Node> excludedNodes,
                                             long blocksize);

  /**
   * choose <i>numOfReplicas</i> data nodes for <i>writer</i>
   * If not, return as many as we can.
   * The base implemenatation extracts the pathname of the file from the
   * specified srcBC, but this could be a costly operation depending on the
   * file system implementation. Concrete implementations of this class should
   * override this method to avoid this overhead.
   * 
   * @param srcBC block collection of file for which chooseTarget is invoked.
   * @param numOfReplicas additional number of replicas wanted.
   * @param writer the writer's machine, null if not in the cluster.
   * @param chosenNodes datanodes that have been chosen as targets.
   * @param blocksize size of the data to be written.
   * @return array of DatanodeDescriptor instances chosen as target 
   * and sorted as a pipeline.
   */
  DatanodeDescriptor[] chooseTarget(BlockCollection srcBC,
                                    int numOfReplicas,
                                    DatanodeDescriptor writer,
                                    List<DatanodeDescriptor> chosenNodes,
                                    HashMap<Node, Node> excludedNodes,
                                    long blocksize) {
    return chooseTarget(srcBC.getName(), numOfReplicas, writer,
                        chosenNodes, false, excludedNodes, blocksize);
  }
  
  /**
   * Same as {@link #chooseTarget(String, int, DatanodeDescriptor, List, boolean, 
   * HashMap, long)} with added parameter {@code favoredDatanodes}
   * @param favoredNodes datanodes that should be favored as targets. This
   *          is only a hint and due to cluster state, namenode may not be 
   *          able to place the blocks on these datanodes.
   */
public   DatanodeDescriptor[] chooseTarget(String src,
      int numOfReplicas, DatanodeDescriptor writer,
      HashMap<Node, Node> excludedNodes,
      long blocksize, List<DatanodeDescriptor> favoredNodes) {
    // This class does not provide the functionality of placing
    // a block in favored datanodes. The implementations of this class
    // are expected to provide this functionality
    return chooseTarget(src, numOfReplicas, writer, 
        new ArrayList<DatanodeDescriptor>(numOfReplicas), false, excludedNodes, 
        blocksize);
  }

  /**
   * Verify that the block is replicated on at least minRacks different racks
   * if there is more than minRacks rack in the system.
   * 
   * @param srcPath the full pathname of the file to be verified
   * @param lBlk block with locations
   * @param minRacks number of racks the block should be replicated to
   * @return the difference between the required and the actual number of racks
   * the block is replicated to.
   */
  abstract public int verifyBlockPlacement(String srcPath,
                                           LocatedBlock lBlk,
                                           int minRacks);
  /**
   * Decide whether deleting the specified replica of the block still makes 
   * the block conform to the configured block placement policy.
   * 
   * @param srcBC block collection of file to which block-to-be-deleted belongs
   * @param block The block to be deleted
   * @param replicationFactor The required number of replicas for this block
   * @param existingReplicas The replica locations of this block that are present
                  on at least two unique racks. 
   * @param moreExistingReplicas Replica locations of this block that are not
                   listed in the previous parameter.
   * @return the replica that is the best candidate for deletion
   */
  abstract public DatanodeDescriptor chooseReplicaToDelete(BlockCollection srcBC,
                                      Block block, 
                                      short replicationFactor,
                                      Collection<DatanodeDescriptor> existingReplicas,
                                      Collection<DatanodeDescriptor> moreExistingReplicas);

  /**
   * Used to setup a BlockPlacementPolicy object. This should be defined by 
   * all implementations of a BlockPlacementPolicy.
   * 
   * @param conf the configuration object
   * @param stats retrieve cluster status from here
   * @param clusterMap cluster topology
   */
  abstract protected void initialize(Configuration conf,  FSClusterStats stats, 
                                     NetworkTopology clusterMap);
    
  /**
   * Get an instance of the configured Block Placement Policy based on the
   * value of the configuration paramater dfs.block.replicator.classname.
   * 
   * @param conf the configuration to be used
   * @param stats an object that is used to retrieve the load on the cluster
   * @param clusterMap the network topology of the cluster
   * @return an instance of BlockPlacementPolicy
   */
  public static BlockPlacementPolicy getInstance(Configuration conf, 
                                                 FSClusterStats stats,
                                                 NetworkTopology clusterMap) {
    Class<? extends BlockPlacementPolicy> replicatorClass =
                      conf.getClass("dfs.block.replicator.classname",
                                    BlockPlacementPolicyDefault.class,
                                    BlockPlacementPolicy.class);
    BlockPlacementPolicy replicator = (BlockPlacementPolicy) ReflectionUtils.newInstance(
                                                             replicatorClass, conf);
    replicator.initialize(conf, stats, clusterMap);
    return replicator;
  }

  /**
   * Adjust rackmap, moreThanOne, and exactlyOne after removing replica on cur.
   *
   * @param rackMap a map from rack to replica
   * @param moreThanOne The List of replica nodes on rack which has more than 
   *        one replica
   * @param exactlyOne The List of replica nodes on rack with only one replica
   * @param cur current replica to remove
   */
  public void adjustSetsWithChosenReplica(final Map<String, 
      List<DatanodeDescriptor>> rackMap,
      final List<DatanodeDescriptor> moreThanOne,
      final List<DatanodeDescriptor> exactlyOne, final DatanodeInfo cur) {
    
    String rack = getRack(cur);
    final List<DatanodeDescriptor> datanodes = rackMap.get(rack);
    datanodes.remove(cur);
    if (datanodes.isEmpty()) {
      rackMap.remove(rack);
    }
    if (moreThanOne.remove(cur)) {
      if (datanodes.size() == 1) {
        moreThanOne.remove(datanodes.get(0));
        exactlyOne.add(datanodes.get(0));
      }
    } else {
      exactlyOne.remove(cur);
    }
  }

  /**
   * Get rack string from a data node
   * @param datanode
   * @return rack of data node
   */
  protected String getRack(final DatanodeInfo datanode) {
    return datanode.getNetworkLocation();
  }
  
  /**
   * Split data nodes into two sets, one set includes nodes on rack with
   * more than one  replica, the other set contains the remaining nodes.
   * 
   * @param dataNodes
   * @param rackMap a map from rack to datanodes
   * @param moreThanOne contains nodes on rack with more than one replica
   * @param exactlyOne remains contains the remaining nodes
   */
  public void splitNodesWithRack(
      Collection<DatanodeDescriptor> dataNodes,
      final Map<String, List<DatanodeDescriptor>> rackMap,
      final List<DatanodeDescriptor> moreThanOne,
      final List<DatanodeDescriptor> exactlyOne) {
    for(DatanodeDescriptor node : dataNodes) {
      final String rackName = getRack(node);
      List<DatanodeDescriptor> datanodeList = rackMap.get(rackName);
      if (datanodeList == null) {
        datanodeList = new ArrayList<DatanodeDescriptor>();
        rackMap.put(rackName, datanodeList);
      }
      datanodeList.add(node);
    }
    
    // split nodes into two sets
    for(List<DatanodeDescriptor> datanodeList : rackMap.values()) {
      if (datanodeList.size() == 1) {
        // exactlyOne contains nodes on rack with only one replica
        exactlyOne.add(datanodeList.get(0));
      } else {
        // moreThanOne contains nodes on rack with more than one replica
        moreThanOne.addAll(datanodeList);
      }
    }
  }

}
