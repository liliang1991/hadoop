package org.apache.hadoop.hdfs.server.namenode.test.hdfs.manager;

import com.google.common.net.InetAddresses;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.blockmanagement.*;
import org.apache.hadoop.hdfs.server.namenode.HostFileManager;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.fs.FileNamesystem;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.hdfs.util.CyclicIteration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.*;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import static org.apache.hadoop.util.Time.now;

public class FileDatanodeManager {
    static final Log LOG = LogFactory.getLog(FileDatanodeManager.class);

    private Namesystem namesystem;
    // 负责接收管理来自 DataNode 的消息，具体的管理操作由 DatanodeManager 接管
    FileBlockManager blockManager;
    FileHeartbeatManager heartbeatManager;

    NetworkTopology networktopology;
    private final int defaultXferPort;

    private final int defaultInfoPort;

    private final int defaultInfoSecurePort;

    private final int defaultIpcPort;
    FileHostManager hostFileManager=new FileHostManager();

    DNSToSwitchMapping dnsToSwitchMapping;
    final int blockInvalidateLimit;

    private final long staleInterval;

    private final boolean avoidStaleDataNodesForRead;
    long heartbeatExpireInterval;

    private final boolean avoidStaleDataNodesForWrite;
    float ratioUseStaleDataNodesForWrite;
    boolean hasClusterEverBeenMultiRack;
    private Daemon decommissionthread = null;
    int numStaleNodes;
    HashMap<String, Integer> datanodesSoftwareVersions =
            new HashMap<String, Integer>(4, 0.75f);
    //保存系统内所有的Datanode, StorageID -> DatanodeDescriptor的对应关系
    private final NavigableMap<String, DatanodeDescriptor> datanodeMap
            = new TreeMap<String, DatanodeDescriptor>();
    private  Host2NodesMap host2DatanodeMap = new Host2NodesMap();

    FileDatanodeManager(FileBlockManager blockManager, FileNamesystem namesystem,
                        Configuration conf) throws IOException {
        this.namesystem = namesystem;
        this.blockManager = blockManager;
        this.heartbeatManager = new FileHeartbeatManager(namesystem, blockManager, conf);

        networktopology = NetworkTopology.getInstance(conf);
        this.defaultXferPort = NetUtils.createSocketAddr(
                conf.get(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY,
                        DFSConfigKeys.DFS_DATANODE_ADDRESS_DEFAULT)).getPort();
        this.defaultInfoPort = NetUtils.createSocketAddr(
                conf.get(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY,
                        DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_DEFAULT)).getPort();
        this.defaultInfoSecurePort = NetUtils.createSocketAddr(
                conf.get(DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY,
                        DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_DEFAULT)).getPort();
        this.defaultIpcPort = NetUtils.createSocketAddr(
                conf.get(DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY,
                        DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_DEFAULT)).getPort();
 /*       this.hostFileManager.refresh(conf.get(DFSConfigKeys.DFS_HOSTS, ""),
                conf.get(DFSConfigKeys.DFS_HOSTS_EXCLUDE, ""));*/
        this.dnsToSwitchMapping = ReflectionUtils.newInstance(
                conf.getClass(DFSConfigKeys.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
                        ScriptBasedMapping.class, DNSToSwitchMapping.class), conf);
 /*       if (dnsToSwitchMapping instanceof CachedDNSToSwitchMapping) {
            final ArrayList<String> locations = new ArrayList<String>();
            for (HostFileManager.Entry entry : hostFileManager.getIncludes()) {
                if (!entry.getIpAddress().isEmpty()) {
                    locations.add(entry.getIpAddress());
                }
            }
            dnsToSwitchMapping.resolve(locations);
        }*/
        ;

        final long heartbeatIntervalSeconds = conf.getLong(
                DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
                DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT);
        final int heartbeatRecheckInterval = conf.getInt(
                DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
                DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT); // 5 minutes
        this.heartbeatExpireInterval = 2 * heartbeatRecheckInterval
                + 10 * 1000 * heartbeatIntervalSeconds;
        final int blockInvalidateLimit = Math.max(20 * (int) (heartbeatIntervalSeconds),
                DFSConfigKeys.DFS_BLOCK_INVALIDATE_LIMIT_DEFAULT);
        this.blockInvalidateLimit = conf.getInt(
                DFSConfigKeys.DFS_BLOCK_INVALIDATE_LIMIT_KEY, blockInvalidateLimit);


        this.avoidStaleDataNodesForRead = conf.getBoolean(
                DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_KEY,
                DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_DEFAULT);
        this.avoidStaleDataNodesForWrite = conf.getBoolean(
                DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY,
                DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_DEFAULT);
        this.staleInterval = getStaleIntervalFromConf(conf, heartbeatExpireInterval);
        this.ratioUseStaleDataNodesForWrite = conf.getFloat(
                DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_KEY,
                DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_DEFAULT);
    }

    private static long getStaleIntervalFromConf(Configuration conf,
                                                 long heartbeatExpireInterval) {
        long staleInterval = conf.getLong(
                DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
                DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT);
        return staleInterval;
    }

    FileHeartbeatManager getHeartbeatManager() {
        return heartbeatManager;
    }

    public NetworkTopology getNetworkTopology() {
        return networktopology;
    }

    public DatanodeStatistics getDatanodeStatistics() {
        return heartbeatManager;
    }

    boolean hasClusterEverBeenMultiRack() {
        return hasClusterEverBeenMultiRack;
    }

    public void activate(final Configuration conf) {
        DecommissionManager dm = new DecommissionManager(namesystem, blockManager);
        this.decommissionthread = new Daemon(dm.new Monitor(
                conf.getInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY,
                        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_DEFAULT),
                conf.getInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_NODES_PER_INTERVAL_KEY,
                        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_NODES_PER_INTERVAL_DEFAULT)));
        decommissionthread.start();

        heartbeatManager.activate(conf);
    }

    /**
     * Is the datanode dead?
     */
    boolean isDatanodeDead(DatanodeDescriptor node) {
        return (node.getLastUpdate() <
                (Time.now() - heartbeatExpireInterval));
    }

    /**
     * @return The time interval used to mark DataNodes as stale.
     */
    long getStaleInterval() {
        return staleInterval;
    }

    /**
     * Set the number of current stale DataNodes. The HeartbeatManager got this
     * number based on DataNodes' heartbeats.
     *
     * @param numStaleNodes The number of stale DataNodes to be set.
     */
    void setNumStaleNodes(int numStaleNodes) {
        this.numStaleNodes = numStaleNodes;
    }

    /**
     * Remove a dead datanode.
     */
    void removeDeadDatanode(final DatanodeID nodeID) {
        synchronized (datanodeMap) {
            DatanodeDescriptor d;
            try {
                d = getDatanode(nodeID);
            } catch (IOException e) {
                d = null;
            }
            if (d != null && isDatanodeDead(d)) {
                NameNode.stateChangeLog.info(
                        "BLOCK* removeDeadDatanode: lost heartbeat from " + d);
                removeDatanode(d);
            }
        }
    }

  public   DatanodeDescriptor getDatanode(final String storageID) {
        return datanodeMap.get(storageID);
    }

    /**
     * Get data node by storage ID.
     *
     * @param nodeID
     * @return DatanodeDescriptor or null if the node is not found.
     * @throws UnregisteredNodeException
     */
    public DatanodeDescriptor getDatanode(DatanodeID nodeID
    ) throws UnregisteredNodeException {
        final DatanodeDescriptor node = getDatanode(nodeID.getStorageID());
        if (node == null)
            return null;
        if (!node.getXferAddr().equals(nodeID.getXferAddr())) {
            final UnregisteredNodeException e = new UnregisteredNodeException(
                    nodeID, node);
            NameNode.stateChangeLog.fatal("BLOCK* NameSystem.getDatanode: "
                    + e.getLocalizedMessage());
            throw e;
        }
        return node;
    }

    /**
     * Remove a datanode descriptor.
     *
     * @param nodeInfo datanode descriptor.
     */
    public void removeDatanode(DatanodeDescriptor nodeInfo) {
        assert namesystem.hasWriteLock();
        heartbeatManager.removeDatanode(nodeInfo);
        blockManager.removeBlocksAssociatedTo(nodeInfo);
        networktopology.remove(nodeInfo);
        decrementVersionCount(nodeInfo.getSoftwareVersion());


        namesystem.checkSafeMode();
    }
    public void removeDatanode(final DatanodeID node
    ) throws UnregisteredNodeException {
        namesystem.writeLock();
        try {
            final DatanodeDescriptor descriptor = getDatanode(node);
            if (descriptor != null) {
                removeDatanode(descriptor);
            } else {
                NameNode.stateChangeLog.warn("BLOCK* removeDatanode: "
                        + node + " does not exist");
            }
        } finally {
            namesystem.writeUnlock();
        }
    }
    private void decrementVersionCount(String version) {
        if (version == null) {
            return;
        }
        synchronized (datanodeMap) {
            Integer count = this.datanodesSoftwareVersions.get(version);
            if (count != null) {
                if (count > 1) {
                    this.datanodesSoftwareVersions.put(version, count - 1);
                } else {
                    this.datanodesSoftwareVersions.remove(version);
                }
            }
        }
    }
    public boolean shouldAvoidStaleDataNodesForWrite() {
        // If # stale exceeds maximum staleness ratio, disable stale
        // datanode avoidance on the write path
        return avoidStaleDataNodesForWrite &&
                (numStaleNodes <= heartbeatManager.getLiveDatanodeCount()
                        * ratioUseStaleDataNodesForWrite);
    }
    public List<DatanodeDescriptor> getDecommissioningNodes() {
        namesystem.readLock();
        try {
            final List<DatanodeDescriptor> decommissioningNodes
                    = new ArrayList<DatanodeDescriptor>();
            final List<DatanodeDescriptor> results = getDatanodeListForReport(
                    HdfsConstants.DatanodeReportType.LIVE);
            for(DatanodeDescriptor node : results) {
                if (node.isDecommissionInProgress()) {
                    decommissioningNodes.add(node);
                }
            }
            return decommissioningNodes;
        } finally {
            namesystem.readUnlock();
        }
    }
    public HashMap<String, Integer> getDatanodesSoftwareVersions() {
        synchronized(datanodeMap) {
            return new HashMap<String, Integer> (this.datanodesSoftwareVersions);
        }
    }
    public int getNumLiveDataNodes() {
        int numLive = 0;
        synchronized (datanodeMap) {
            for(DatanodeDescriptor dn : datanodeMap.values()) {
                if (!isDatanodeDead(dn) ) {
                    numLive++;
                }
            }
        }
        return numLive;
    }
    public int getNumDeadDataNodes() {
        int numDead = 0;
        synchronized (datanodeMap) {
            for(DatanodeDescriptor dn : datanodeMap.values()) {
                if (isDatanodeDead(dn) ) {
                    numDead++;
                }
            }
        }
        return numDead;
    }
    //rpc

    /** Sort the located blocks by the distance to the target host. */
    public void sortLocatedBlocks(final String targethost,
                                  final List<LocatedBlock> locatedblocks) {
        //sort the blocks
        // As it is possible for the separation of node manager and datanode,
        // here we should get node but not datanode only .
        Node client = getDatanodeByHost(targethost);
        if (client == null) {
            List<String> hosts = new ArrayList<String> (1);
            hosts.add(targethost);
            String rName = dnsToSwitchMapping.resolve(hosts).get(0);
            if (rName != null)
                client = new NodeBase(rName + NodeBase.PATH_SEPARATOR_STR + targethost);
        }

        Comparator<DatanodeInfo> comparator = avoidStaleDataNodesForRead ?
                new DFSUtil.DecomStaleComparator(staleInterval) :
                DFSUtil.DECOM_COMPARATOR;

        for (LocatedBlock b : locatedblocks) {
            networktopology.pseudoSortByDistance(client, b.getLocations());
            // Move decommissioned/stale datanodes to the bottom
            Arrays.sort(b.getLocations(), comparator);
        }
    }

    /**
     * Given datanode address or host name, returns the DatanodeDescriptor for the
     * same, or if it doesn't find the datanode, it looks for a machine local and
     * then rack local datanode, if a rack local datanode is not possible either,
     * it returns the DatanodeDescriptor of any random node in the cluster.
     *
     * @param address hostaddress:transfer address
     * @return the best match for the given datanode
     */
    DatanodeDescriptor getDatanodeDescriptor(String address) {
        DatanodeID dnId = parseDNFromHostsEntry(address);
        String host = dnId.getIpAddr();
        int xferPort = dnId.getXferPort();
        DatanodeDescriptor node = getDatanodeByXferAddr(host, xferPort);
        if (node == null) {
            node = getDatanodeByHost(host);
        }
        if (node == null) {
            String networkLocation = resolveNetworkLocation(dnId);

            // If the current cluster doesn't contain the node, fallback to
            // something machine local and then rack local.
            List<Node> rackNodes = getNetworkTopology()
                    .getDatanodesInRack(networkLocation);
            if (rackNodes != null) {
                // Try something machine local.
                for (Node rackNode : rackNodes) {
                    if (((DatanodeDescriptor) rackNode).getIpAddr().equals(host)) {
                        node = (DatanodeDescriptor) rackNode;
                        break;
                    }
                }

                // Try something rack local.
                if (node == null && !rackNodes.isEmpty()) {
                    node = (DatanodeDescriptor) (rackNodes
                            .get(DFSUtil.getRandom().nextInt(rackNodes.size())));
                }
            }

            // If we can't even choose rack local, just choose any node in the
            // cluster.
            if (node == null) {
                node = (DatanodeDescriptor)getNetworkTopology()
                        .chooseRandom(NodeBase.ROOT);
            }
        }
        return node;
    }
    public DatanodeDescriptor getDatanodeByXferAddr(String host, int xferPort) {
        return host2DatanodeMap.getDatanodeByXferAddr(host, xferPort);
    }

    /* Resolve a node's network location */
    private String resolveNetworkLocation (DatanodeID node) {
        List<String> names = new ArrayList<String>(1);
        if (dnsToSwitchMapping instanceof CachedDNSToSwitchMapping) {
            names.add(node.getIpAddr());
        } else {
            names.add(node.getHostName());
        }

        // resolve its network location
        List<String> rName = dnsToSwitchMapping.resolve(names);
        String networkLocation;
        if (rName == null) {
            LOG.error("The resolve call returned null! Using " +
                    NetworkTopology.DEFAULT_RACK + " for host " + names);
            networkLocation = NetworkTopology.DEFAULT_RACK;
        } else {
            networkLocation = rName.get(0);
        }
        return networkLocation;
    }
    /**
     * Parse a DatanodeID from a hosts file entry
     * @param hostLine of form [hostname|ip][:port]?
     * @return DatanodeID constructed from the given string
     */
    private DatanodeID parseDNFromHostsEntry(String hostLine) {
        DatanodeID dnId;
        String hostStr;
        int port;
        int idx = hostLine.indexOf(':');

        if (-1 == idx) {
            hostStr = hostLine;
            port = DFSConfigKeys.DFS_DATANODE_DEFAULT_PORT;
        } else {
            hostStr = hostLine.substring(0, idx);
            port = Integer.valueOf(hostLine.substring(idx+1));
        }

        if (InetAddresses.isInetAddress(hostStr)) {
            // The IP:port is sufficient for listing in a report
            dnId = new DatanodeID(hostStr, "", "", port,
                    DFSConfigKeys.DFS_DATANODE_HTTP_DEFAULT_PORT,
                    DFSConfigKeys.DFS_DATANODE_HTTPS_DEFAULT_PORT,
                    DFSConfigKeys.DFS_DATANODE_IPC_DEFAULT_PORT);
        } else {
            String ipAddr = "";
            try {
                ipAddr = InetAddress.getByName(hostStr).getHostAddress();
            } catch (UnknownHostException e) {
                LOG.warn("Invalid hostname " + hostStr + " in hosts file");
            }
            dnId = new DatanodeID(ipAddr, hostStr, "", port,
                    DFSConfigKeys.DFS_DATANODE_HTTP_DEFAULT_PORT,
                    DFSConfigKeys.DFS_DATANODE_HTTPS_DEFAULT_PORT,
                    DFSConfigKeys.DFS_DATANODE_IPC_DEFAULT_PORT);
        }
        return dnId;
    }

    /**
     * Register the given datanode with the namenode. NB: the given
     * registration is mutated and given back to the datanode.
     *
     * @param nodeReg the datanode registration
     * @throws DisallowedDatanodeException if the registration request is
     *    denied because the datanode does not match includes/excludes
     */
    public void registerDatanode(DatanodeRegistration nodeReg)
            throws DisallowedDatanodeException {
        InetAddress dnAddress = Server.getRemoteIp();
        if (dnAddress != null) {
            // Mostly called inside an RPC, update ip and peer hostname
            String hostname = dnAddress.getHostName();
            String ip = dnAddress.getHostAddress();
            if (!isNameResolved(dnAddress)) {
                // Reject registration of unresolved datanode to prevent performance
                // impact of repetitive DNS lookups later.
                LOG.warn("Unresolved datanode registration from " + ip);
                throw new DisallowedDatanodeException(nodeReg);
            }
            // update node registration with the ip and hostname from rpc request
            nodeReg.setIpAddr(ip);
            nodeReg.setPeerHostName(hostname);
        }

        try {
            nodeReg.setExportedKeys(blockManager.getBlockKeys());

            // Checks if the node is not on the hosts list.  If it is not, then
            // it will be disallowed from registering.
            if (!hostFileManager.isIncluded(nodeReg)) {
                throw new DisallowedDatanodeException(nodeReg);
            }

            NameNode.stateChangeLog.info("BLOCK* registerDatanode: from "
                    + nodeReg + " storage " + nodeReg.getStorageID());

            DatanodeDescriptor nodeS = datanodeMap.get(nodeReg.getStorageID());
            DatanodeDescriptor nodeN = host2DatanodeMap.getDatanodeByXferAddr(
                    nodeReg.getIpAddr(), nodeReg.getXferPort());

            if (nodeN != null && nodeN != nodeS) {
                NameNode.LOG.info("BLOCK* registerDatanode: " + nodeN);
                // nodeN previously served a different data storage,
                // which is not served by anybody anymore.
                removeDatanode(nodeN);
                // physically remove node from datanodeMap
                wipeDatanode(nodeN);
                nodeN = null;
            }

            if (nodeS != null) {
                if (nodeN == nodeS) {
                    // The same datanode has been just restarted to serve the same data
                    // storage. We do not need to remove old data blocks, the delta will
                    // be calculated on the next block report from the datanode
                    if(NameNode.stateChangeLog.isDebugEnabled()) {
                        NameNode.stateChangeLog.debug("BLOCK* registerDatanode: "
                                + "node restarted.");
                    }
                } else {
                    // nodeS is found
          /* The registering datanode is a replacement node for the existing
            data storage, which from now on will be served by a new node.
            If this message repeats, both nodes might have same storageID
            by (insanely rare) random chance. User needs to restart one of the
            nodes with its data cleared (or user can just remove the StorageID
            value in "VERSION" file under the data directory of the datanode,
            but this is might not work if VERSION file format has changed
         */
                    NameNode.stateChangeLog.info("BLOCK* registerDatanode: " + nodeS
                            + " is replaced by " + nodeReg + " with the same storageID "
                            + nodeReg.getStorageID());
                }

                boolean success = false;
                try {
                    // update cluster map
                    getNetworkTopology().remove(nodeS);
                    if(shouldCountVersion(nodeS)) {
                        decrementVersionCount(nodeS.getSoftwareVersion());
                    }
                    nodeS.updateRegInfo(nodeReg);

                    nodeS.setSoftwareVersion(nodeReg.getSoftwareVersion());
                    nodeS.setDisallowed(false); // Node is in the include list

                    // resolve network location
                    nodeS.setNetworkLocation(resolveNetworkLocation(nodeS));
                    getNetworkTopology().add(nodeS);

                    // also treat the registration message as a heartbeat
                    heartbeatManager.register(nodeS);
                    incrementVersionCount(nodeS.getSoftwareVersion());
                    checkDecommissioning(nodeS);
                    success = true;
                } finally {
                    if (!success) {
                        removeDatanode(nodeS);
                        wipeDatanode(nodeS);
                        countSoftwareVersions();
                    }
                }
                return;
            }

            // this is a new datanode serving a new data storage
            if ("".equals(nodeReg.getStorageID())) {
                // this data storage has never been registered
                // it is either empty or was created by pre-storageID version of DFS
                nodeReg.setStorageID(newStorageID());
                if (NameNode.stateChangeLog.isDebugEnabled()) {
                    NameNode.stateChangeLog.debug(
                            "BLOCK* NameSystem.registerDatanode: "
                                    + "new storageID " + nodeReg.getStorageID() + " assigned.");
                }
            }

            DatanodeDescriptor nodeDescr
                    = new DatanodeDescriptor(nodeReg, NetworkTopology.DEFAULT_RACK);
            boolean success = false;
            try {
                nodeDescr.setNetworkLocation(resolveNetworkLocation(nodeDescr));
                networktopology.add(nodeDescr);
                nodeDescr.setSoftwareVersion(nodeReg.getSoftwareVersion());

                // register new datanode
                addDatanode(nodeDescr);
                checkDecommissioning(nodeDescr);

                // also treat the registration message as a heartbeat
                // no need to update its timestamp
                // because its is done when the descriptor is created
                heartbeatManager.addDatanode(nodeDescr);
                success = true;
                incrementVersionCount(nodeReg.getSoftwareVersion());
            } finally {
                if (!success) {
                    removeDatanode(nodeDescr);
                    wipeDatanode(nodeDescr);
                    countSoftwareVersions();
                }
            }
        } catch (NetworkTopology.InvalidTopologyException e) {
            // If the network location is invalid, clear the cached mappings
            // so that we have a chance to re-add this DataNode with the
            // correct network location later.
            dnsToSwitchMapping.reloadCachedMappings();
            throw e;
        }
    }
    private void wipeDatanode(final DatanodeID node) {
        final String key = node.getStorageID();
        synchronized (datanodeMap) {
            host2DatanodeMap.remove(datanodeMap.remove(key));
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(getClass().getSimpleName() + ".wipeDatanode("
                    + node + "): storage " + key
                    + " is removed from datanodeMap.");
        }
    }
    private static boolean isNameResolved(InetAddress address) {
        String hostname = address.getHostName();
        String ip = address.getHostAddress();
        return !hostname.equals(ip) || address.isLoopbackAddress();
    }
    private boolean shouldCountVersion(DatanodeDescriptor node) {
        return node.getSoftwareVersion() != null && node.isAlive &&
                !isDatanodeDead(node);
    }
    private void incrementVersionCount(String version) {
        if (version == null) {
            return;
        }
        synchronized(datanodeMap) {
            Integer count = this.datanodesSoftwareVersions.get(version);
            count = count == null ? 1 : count + 1;
            this.datanodesSoftwareVersions.put(version, count);
        }
    }
    private void checkDecommissioning(DatanodeDescriptor nodeReg) {
        // If the registered node is in exclude list, then decommission it
        if (hostFileManager.isExcluded(nodeReg)) {
            startDecommission(nodeReg);
        }
    }
    void addDatanode(final DatanodeDescriptor node) {
        // To keep host2DatanodeMap consistent with datanodeMap,
        // remove  from host2DatanodeMap the datanodeDescriptor removed
        // from datanodeMap before adding node to host2DatanodeMap.
        synchronized(datanodeMap) {
            host2DatanodeMap.remove(datanodeMap.put(node.getStorageID(), node));
        }

        networktopology.add(node); // may throw InvalidTopologyException
        host2DatanodeMap.add(node);
        checkIfClusterIsNowMultiRack(node);

        if (LOG.isDebugEnabled()) {
            LOG.debug(getClass().getSimpleName() + ".addDatanode: "
                    + "node " + node + " is added to datanodeMap.");
        }
    }

    void checkIfClusterIsNowMultiRack(DatanodeDescriptor node) {
        if (!hasClusterEverBeenMultiRack && networktopology.getNumOfRacks() > 1) {
            String message = "DN " + node + " joining cluster has expanded a formerly " +
                    "single-rack cluster to be multi-rack. ";
            if (namesystem.isPopulatingReplQueues()) {
                message += "Re-checking all blocks for replication, since they should " +
                        "now be replicated cross-rack";
                LOG.info(message);
            } else {
                message += "Not checking for mis-replicated blocks because this NN is " +
                        "not yet processing repl queues.";
                LOG.debug(message);
            }
            hasClusterEverBeenMultiRack = true;
            if (namesystem.isPopulatingReplQueues()) {
                blockManager.processMisReplicatedBlocks();
            }
        }
    }
    private void countSoftwareVersions() {
        synchronized(datanodeMap) {
            HashMap<String, Integer> versionCount = new HashMap<String, Integer>();
            for(DatanodeDescriptor dn: datanodeMap.values()) {
                // Check isAlive too because right after removeDatanode(),
                // isDatanodeDead() is still true
                if(shouldCountVersion(dn))
                {
                    Integer num = versionCount.get(dn.getSoftwareVersion());
                    num = num == null ? 1 : num+1;
                    versionCount.put(dn.getSoftwareVersion(), num);
                }
            }
            this.datanodesSoftwareVersions = versionCount;
        }
    }
    private String newStorageID() {
        String newID = null;
        while(newID == null) {
            newID = "DS" + Integer.toString(DFSUtil.getRandom().nextInt());
            if (datanodeMap.get(newID) != null)
                newID = null;
        }
        return newID;
    }
public     CyclicIteration<String, DatanodeDescriptor> getDatanodeCyclicIteration(
            final String firstkey) {
        return new CyclicIteration<String, DatanodeDescriptor>(
                datanodeMap, firstkey);
    }

    /** Start decommissioning the specified datanode. */
    private void startDecommission(DatanodeDescriptor node) {
        if (!node.isDecommissionInProgress() && !node.isDecommissioned()) {
            LOG.info("Start Decommissioning " + node + " with " +
                    node.numBlocks() +  " blocks");
            heartbeatManager.startDecommission(node);
            node.decommissioningStatus.setStartTime(now());

            // all the blocks that reside on this node have to be replicated.
            checkDecommissionState(node);
        }
    }
  public   boolean checkDecommissionState(DatanodeDescriptor node) {
        // Check to see if all blocks in this decommissioned
        // node has reached their target replication factor.
        if (node.isDecommissionInProgress()) {
            if (!blockManager.isReplicationInProgress(node)) {
                node.setDecommissioned();
                LOG.info("Decommission complete for " + node);
            }
        }
        return node.isDecommissioned();
    }
    /** @return the datanode descriptor for the host. */
    public DatanodeDescriptor getDatanodeByHost(final String host) {
        return host2DatanodeMap.getDatanodeByHost(host);
    }
    /** Handle heartbeat from datanodes. */
    public DatanodeCommand[] handleHeartbeat(DatanodeRegistration nodeReg,
                                             final String blockPoolId,
                                             long capacity, long dfsUsed, long remaining, long blockPoolUsed,
                                             int xceiverCount, int maxTransfers, int failedVolumes
    ) throws IOException {
        synchronized (heartbeatManager) {
            synchronized (datanodeMap) {
                DatanodeDescriptor nodeinfo = null;
                try {
                    nodeinfo = getDatanode(nodeReg);
                } catch(UnregisteredNodeException e) {
                    return new DatanodeCommand[]{RegisterCommand.REGISTER};
                }

                // Check if this datanode should actually be shutdown instead.
                if (nodeinfo != null && nodeinfo.isDisallowed()) {
                    setDatanodeDead(nodeinfo);
                    throw new DisallowedDatanodeException(nodeinfo);
                }

                if (nodeinfo == null || !nodeinfo.isAlive) {
                    return new DatanodeCommand[]{RegisterCommand.REGISTER};
                }

                heartbeatManager.updateHeartbeat(nodeinfo, capacity, dfsUsed,
                        remaining, blockPoolUsed, xceiverCount, failedVolumes);

                // If we are in safemode, do not send back any recovery / replication
                // requests. Don't even drain the existing queue of work.
                if(namesystem.isInSafeMode()) {
                    return new DatanodeCommand[0];
                }

                //check lease recovery
                BlockInfoUnderConstruction[] blocks = nodeinfo
                        .getLeaseRecoveryCommand(Integer.MAX_VALUE);
                if (blocks != null) {
                    BlockRecoveryCommand brCommand = new BlockRecoveryCommand(
                            blocks.length);
                    for (BlockInfoUnderConstruction b : blocks) {
                        DatanodeDescriptor[] expectedLocations = b.getExpectedLocations();
                        // Skip stale nodes during recovery - not heart beated for some time (30s by default).
                        List<DatanodeDescriptor> recoveryLocations =
                                new ArrayList<DatanodeDescriptor>(expectedLocations.length);
                        for (int i = 0; i < expectedLocations.length; i++) {
                            if (!expectedLocations[i].isStale(this.staleInterval)) {
                                recoveryLocations.add(expectedLocations[i]);
                            }
                        }
                        // If we only get 1 replica after eliminating stale nodes, then choose all
                        // replicas for recovery and let the primary data node handle failures.
                        if (recoveryLocations.size() > 1) {
                            if (recoveryLocations.size() != expectedLocations.length) {
                                LOG.info("Skipped stale nodes for recovery : " +
                                        (expectedLocations.length - recoveryLocations.size()));
                            }
                            brCommand.add(new BlockRecoveryCommand.RecoveringBlock(
                                    new ExtendedBlock(blockPoolId, b),
                                    recoveryLocations.toArray(new DatanodeDescriptor[recoveryLocations.size()]),
                                    b.getBlockRecoveryId()));
                        } else {
                            // If too many replicas are stale, then choose all replicas to participate
                            // in block recovery.
                            brCommand.add(new BlockRecoveryCommand.RecoveringBlock(
                                    new ExtendedBlock(blockPoolId, b),
                                    expectedLocations,
                                    b.getBlockRecoveryId()));
                        }
                    }
                    return new DatanodeCommand[] { brCommand };
                }

                final List<DatanodeCommand> cmds = new ArrayList<DatanodeCommand>();
                //check pending replication
                List<DatanodeDescriptor.BlockTargetPair> pendingList = nodeinfo.getReplicationCommand(
                        maxTransfers);
                if (pendingList != null) {
                    cmds.add(new BlockCommand(DatanodeProtocol.DNA_TRANSFER, blockPoolId,
                            pendingList));
                }
                //check block invalidation
                Block[] blks = nodeinfo.getInvalidateBlocks(blockInvalidateLimit);
                if (blks != null) {
                    cmds.add(new BlockCommand(DatanodeProtocol.DNA_INVALIDATE,
                            blockPoolId, blks));
                }

                blockManager.addKeyUpdateCommand(cmds, nodeinfo);

                // check for balancer bandwidth update
                if (nodeinfo.getBalancerBandwidth() > 0) {
                    cmds.add(new BalancerBandwidthCommand(nodeinfo.getBalancerBandwidth()));
                    // set back to 0 to indicate that datanode has been sent the new value
                    nodeinfo.setBalancerBandwidth(0);
                }

                if (!cmds.isEmpty()) {
                    return cmds.toArray(new DatanodeCommand[cmds.size()]);
                }
            }
        }

        return new DatanodeCommand[0];
    }
    private void setDatanodeDead(DatanodeDescriptor node) {
        node.setLastUpdate(0);
    }
    public List<DatanodeDescriptor> getDatanodeListForReport(
            final HdfsConstants.DatanodeReportType type) {
        boolean listLiveNodes = type == HdfsConstants.DatanodeReportType.ALL ||
                type == HdfsConstants.DatanodeReportType.LIVE;
        boolean listDeadNodes = type == HdfsConstants.DatanodeReportType.ALL ||
                type == HdfsConstants.DatanodeReportType.DEAD;

        ArrayList<DatanodeDescriptor> nodes = null;
        final HostFileManager.MutableEntrySet foundNodes = new HostFileManager.MutableEntrySet();
        synchronized(datanodeMap) {
            nodes = new ArrayList<DatanodeDescriptor>(datanodeMap.size());
            Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator();
            while (it.hasNext()) {
                DatanodeDescriptor dn = it.next();
                final boolean isDead = isDatanodeDead(dn);
                if ( (isDead && listDeadNodes) || (!isDead && listLiveNodes) ) {
                    nodes.add(dn);
                }
                foundNodes.add(dn);
            }
        }

        if (listDeadNodes) {
            final FileHostManager.EntrySet includedNodes = hostFileManager.getIncludes();
            final FileHostManager.EntrySet excludedNodes = hostFileManager.getExcludes();
            for (HostFileManager.Entry entry : includedNodes) {
                if ((foundNodes.find(entry) == null) &&
                        (excludedNodes.find(entry) == null)) {
                    // The remaining nodes are ones that are referenced by the hosts
                    // files but that we do not know about, ie that we have never
                    // head from. Eg. an entry that is no longer part of the cluster
                    // or a bogus entry was given in the hosts files
                    //
                    // If the host file entry specified the xferPort, we use that.
                    // Otherwise, we guess that it is the default xfer port.
                    // We can't ask the DataNode what it had configured, because it's
                    // dead.
                    DatanodeDescriptor dn =
                            new DatanodeDescriptor(new DatanodeID(entry.getIpAddress(),
                                    entry.getPrefix(), "",
                                    entry.getPort() == 0 ? defaultXferPort : entry.getPort(),
                                    defaultInfoPort, defaultInfoSecurePort, defaultIpcPort));
                    dn.setLastUpdate(0); // Consider this node dead for reporting
                    nodes.add(dn);
                }
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("getDatanodeListForReport with " +
                    "includedNodes = " + hostFileManager.getIncludes() +
                    ", excludedNodes = " + hostFileManager.getExcludes() +
                    ", foundNodes = " + foundNodes +
                    ", nodes = " + nodes);
        }
        return nodes;
    }
    public void refreshNodes(final Configuration conf) throws IOException {
        refreshHostsReader(conf);
        namesystem.writeLock();
        try {
            refreshDatanodes();
            countSoftwareVersions();
        } finally {
            namesystem.writeUnlock();
        }
    }
    private void refreshHostsReader(Configuration conf) throws IOException {
        // Reread the conf to get dfs.hosts and dfs.hosts.exclude filenames.
        // Update the file names and refresh internal includes and excludes list.
        if (conf == null) {
            conf = new HdfsConfiguration();
        }
        this.hostFileManager.refresh(conf.get(DFSConfigKeys.DFS_HOSTS, ""),
                conf.get(DFSConfigKeys.DFS_HOSTS_EXCLUDE, ""));
    }
    private void refreshDatanodes() {
        for(DatanodeDescriptor node : datanodeMap.values()) {
            // Check if not include.
            if (!hostFileManager.isIncluded(node)) {
                node.setDisallowed(true); // case 2.
            } else {
                if (hostFileManager.isExcluded(node)) {
                    startDecommission(node); // case 3.
                } else {
                    stopDecommission(node); // case 4.
                }
            }
        }
    }
    void stopDecommission(DatanodeDescriptor node) {
        if (node.isDecommissionInProgress() || node.isDecommissioned()) {
            LOG.info("Stop Decommissioning " + node);
            heartbeatManager.stopDecommission(node);
            // Over-replicated blocks will be detected and processed when
            // the dead node comes back and send in its full block report.
            if (node.isAlive) {
                blockManager.processOverReplicatedBlocksOnReCommission(node);
            }
        }
    }

    /**
     * @return Return the current number of stale DataNodes (detected by
     * HeartbeatManager).
     */
    public int getNumStaleNodes() {
        return this.numStaleNodes;
    }

    /** Fetch live and dead datanodes. */
    public void fetchDatanodes(final List<DatanodeDescriptor> live,
                               final List<DatanodeDescriptor> dead, final boolean removeDecommissionNode) {
        if (live == null && dead == null) {
            throw new HadoopIllegalArgumentException("Both live and dead lists are null");
        }

        namesystem.readLock();
        try {
            final List<DatanodeDescriptor> results =
                    getDatanodeListForReport(HdfsConstants.DatanodeReportType.ALL);
            for(DatanodeDescriptor node : results) {
                if (isDatanodeDead(node)) {
                    if (dead != null) {
                        dead.add(node);
                    }
                } else {
                    if (live != null) {
                        live.add(node);
                    }
                }
            }
        } finally {
            namesystem.readUnlock();
        }

        if (removeDecommissionNode) {
            if (live != null) {
                removeDecomNodeFromList(live);
            }
            if (dead != null) {
                removeDecomNodeFromList(dead);
            }
        }
    }


    /**
     * Remove an already decommissioned data node who is neither in include nor
     * exclude hosts lists from the the list of live or dead nodes.  This is used
     * to not display an already decommssioned data node to the operators.
     * The operation procedure of making a already decommissioned data node not
     * to be displayed is as following:
     * <ol>
     *   <li>
     *   Host must have been in the include hosts list and the include hosts list
     *   must not be empty.
     *   </li>
     *   <li>
     *   Host is decommissioned by remaining in the include hosts list and added
     *   into the exclude hosts list. Name node is updated with the new
     *   information by issuing dfsadmin -refreshNodes command.
     *   </li>
     *   <li>
     *   Host is removed from both include hosts and exclude hosts lists.  Name
     *   node is updated with the new informationby issuing dfsamin -refreshNodes
     *   command.
     *   <li>
     * </ol>
     *
     * @param nodeList
     *          , array list of live or dead nodes.
     */
    private void removeDecomNodeFromList(final List<DatanodeDescriptor> nodeList) {
        // If the include list is empty, any nodes are welcomed and it does not
        // make sense to exclude any nodes from the cluster. Therefore, no remove.
        if (!hostFileManager.hasIncludes()) {
            return;
        }

        for (Iterator<DatanodeDescriptor> it = nodeList.iterator(); it.hasNext(); ) {
            DatanodeDescriptor node = it.next();
            if ((!hostFileManager.isIncluded(node)) && (!hostFileManager.isExcluded(node))
                    && node.isDecommissioned()) {
                // Include list is not empty, an existing datanode does not appear
                // in both include or exclude lists and it has been decommissioned.
                it.remove();
            }
        }
    }

    /** Prints information about all datanodes. */
  public   void datanodeDump(final PrintWriter out) {
        synchronized (datanodeMap) {
            out.println("Metasave: Number of datanodes: " + datanodeMap.size());
            for(Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator(); it.hasNext();) {
                DatanodeDescriptor node = it.next();
                out.println(node.dumpDatanode());
            }
        }
    }
    public void setBalancerBandwidth(long bandwidth) throws IOException {
        synchronized(datanodeMap) {
            for (DatanodeDescriptor nodeInfo : datanodeMap.values()) {
                nodeInfo.setBalancerBandwidth(bandwidth);
            }
        }
    }
}
