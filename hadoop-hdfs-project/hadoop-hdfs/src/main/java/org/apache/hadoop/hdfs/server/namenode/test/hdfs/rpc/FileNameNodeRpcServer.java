package org.apache.hadoop.hdfs.server.namenode.test.hdfs.rpc;

import com.google.protobuf.BlockingService;
import org.apache.commons.logging.Log;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.ha.HealthCheckFailedException;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos;
import org.apache.hadoop.ha.protocolPB.HAServiceProtocolPB;
import org.apache.hadoop.ha.protocolPB.HAServiceProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocolPB.*;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.NameNodeTest;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.fs.FileNamesystem;
import org.apache.hadoop.hdfs.server.namenode.web.resources.NamenodeWebHdfsMethods;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.WritableRpcEngine;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.proto.RefreshAuthorizationPolicyProtocolProtos;
import org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos;
import org.apache.hadoop.security.protocolPB.RefreshAuthorizationPolicyProtocolPB;
import org.apache.hadoop.security.protocolPB.RefreshAuthorizationPolicyProtocolServerSideTranslatorPB;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolPB;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolServerSideTranslatorPB;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.tools.proto.GetUserMappingsProtocolProtos;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolPB;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ClientNamenodeProtocol;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.VersionUtil;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.MAX_PATH_DEPTH;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.MAX_PATH_LENGTH;

public class FileNameNodeRpcServer implements NamenodeProtocols {
    private final RPC.Server serviceRpcServer;
    private final InetSocketAddress serviceRPCAddress;
    private static final Log LOG = NameNodeTest.LOG;
    private static final Log stateChangeLog = NameNodeTest.stateChangeLog;
    private static final Log blockStateChangeLog = NameNodeTest.blockStateChangeLog;
    protected  NameNodeTest nn;
    protected final FileNamesystem namesystem;
    protected final RPC.Server clientRpcServer;
    boolean serviceAuthEnabled;
    protected final InetSocketAddress clientRpcAddress;
    String minimumDataNodeVersion;
    private  NameNodeMetrics metrics;

    public   void start() {
        clientRpcServer.start();
        if (serviceRpcServer != null) {
            serviceRpcServer.start();
        }
    }

    /**
     * Wait until the RPC servers have shutdown.
     */
  public   void join() throws InterruptedException {
        clientRpcServer.join();
        if (serviceRpcServer != null) {
            serviceRpcServer.join();
        }
    }
 public    InetSocketAddress getServiceRpcAddress() {
        return serviceRPCAddress;
    }
    public FileNameNodeRpcServer(Configuration conf, NameNodeTest nn)
            throws IOException {
        this.nn = nn;
        this.namesystem = nn.getNamesystem();
        this.metrics = NameNodeTest.getNameNodeMetrics();

        int handlerCount =
                conf.getInt(DFS_NAMENODE_HANDLER_COUNT_KEY,
                        DFS_NAMENODE_HANDLER_COUNT_DEFAULT);
        //设置ProtolEngine，目前只支持PB协议。表示接收到的RPC协议如果是ClientNamenodeProtocolPB，
        //那么处理这个RPC协议的引擎是ProtobufRpcEngine

        RPC.setProtocolEngine(conf, ClientNamenodeProtocolPB.class,
                ProtobufRpcEngine.class);
//声明一个ClientNamenodeProtocolServerSideTranslatorPB，
        //这个类负责把Server接收到的PB格式对象的数据，拼装成NameNode内村中的数据类型，
        //调用NameNodeRpcServer类中相应的逻辑，然后再把执行结果拼装成PB格式。
        ClientNamenodeProtocolServerSideTranslatorPB
                clientProtocolServerTranslator =
                new ClientNamenodeProtocolServerSideTranslatorPB(this);
        BlockingService clientNNPbService = ClientNamenodeProtocol.
                newReflectiveBlockingService(clientProtocolServerTranslator);

        DatanodeProtocolServerSideTranslatorPB dnProtoPbTranslator =
                new DatanodeProtocolServerSideTranslatorPB(this);
        BlockingService dnProtoPbService = DatanodeProtocolProtos.DatanodeProtocolService
                .newReflectiveBlockingService(dnProtoPbTranslator);

        NamenodeProtocolServerSideTranslatorPB namenodeProtocolXlator =
                new NamenodeProtocolServerSideTranslatorPB(this);
        BlockingService NNPbService = NamenodeProtocolProtos.NamenodeProtocolService
                .newReflectiveBlockingService(namenodeProtocolXlator);

        RefreshAuthorizationPolicyProtocolServerSideTranslatorPB refreshAuthPolicyXlator =
                new RefreshAuthorizationPolicyProtocolServerSideTranslatorPB(this);
        BlockingService refreshAuthService = RefreshAuthorizationPolicyProtocolProtos.RefreshAuthorizationPolicyProtocolService
                .newReflectiveBlockingService(refreshAuthPolicyXlator);

        RefreshUserMappingsProtocolServerSideTranslatorPB refreshUserMappingXlator =
                new RefreshUserMappingsProtocolServerSideTranslatorPB(this);
        BlockingService refreshUserMappingService = RefreshUserMappingsProtocolProtos.RefreshUserMappingsProtocolService
                .newReflectiveBlockingService(refreshUserMappingXlator);

        GetUserMappingsProtocolServerSideTranslatorPB getUserMappingXlator =
                new GetUserMappingsProtocolServerSideTranslatorPB(this);
        BlockingService getUserMappingService = GetUserMappingsProtocolProtos.GetUserMappingsProtocolService
                .newReflectiveBlockingService(getUserMappingXlator);

        HAServiceProtocolServerSideTranslatorPB haServiceProtocolXlator =
                new HAServiceProtocolServerSideTranslatorPB(this);
        BlockingService haPbService = HAServiceProtocolProtos.HAServiceProtocolService
                .newReflectiveBlockingService(haServiceProtocolXlator);

        WritableRpcEngine.ensureInitialized();

        InetSocketAddress serviceRpcAddr = nn.getServiceRpcServerAddress(conf);

            serviceRpcServer = null;
            serviceRPCAddress = null;
        InetSocketAddress rpcAddr = nn.getRpcServerAddress(conf);
        String bindHost = nn.getRpcServerBindHost(conf);
        if (bindHost == null) {
            bindHost = rpcAddr.getHostName();
        }
        LOG.info("RPC server is binding to " + bindHost + ":" + rpcAddr.getPort());

        this.clientRpcServer = new RPC.Builder(conf)
                .setProtocol(
                        org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB.class)
                .setInstance(clientNNPbService).setBindAddress(bindHost)
                .setPort(rpcAddr.getPort()).setNumHandlers(handlerCount)
                .setVerbose(false)
                .setSecretManager(namesystem.getDelegationTokenSecretManager()).build();

        // Add all the RPC protocols that the namenode implements
        DFSUtil.addPBProtocol(conf, HAServiceProtocolPB.class, haPbService,
                clientRpcServer);
        DFSUtil.addPBProtocol(conf, NamenodeProtocolPB.class, NNPbService,
                clientRpcServer);
        DFSUtil.addPBProtocol(conf, DatanodeProtocolPB.class, dnProtoPbService,
                clientRpcServer);
        DFSUtil.addPBProtocol(conf, RefreshAuthorizationPolicyProtocolPB.class,
                refreshAuthService, clientRpcServer);
        DFSUtil.addPBProtocol(conf, RefreshUserMappingsProtocolPB.class,
                refreshUserMappingService, clientRpcServer);
        DFSUtil.addPBProtocol(conf, GetUserMappingsProtocolPB.class,
                getUserMappingService, clientRpcServer);

        // set service-level authorization security policy
        if (serviceAuthEnabled =
                conf.getBoolean(
                        CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
            clientRpcServer.refreshServiceAcl(conf, new HDFSPolicyProvider());
            if (serviceRpcServer != null) {
                serviceRpcServer.refreshServiceAcl(conf, new HDFSPolicyProvider());
            }
        }

        // The rpc-server port can be ephemeral... ensure we have the correct info
        InetSocketAddress listenAddr = clientRpcServer.getListenerAddress();
        clientRpcAddress = new InetSocketAddress(
                rpcAddr.getHostName(), listenAddr.getPort());
        nn.setRpcServerAddress(conf, clientRpcAddress);

        minimumDataNodeVersion = conf.get(
                DFSConfigKeys.DFS_NAMENODE_MIN_SUPPORTED_DATANODE_VERSION_KEY,
                DFSConfigKeys.DFS_NAMENODE_MIN_SUPPORTED_DATANODE_VERSION_DEFAULT);

        // Set terse exception whose stack trace won't be logged
        this.clientRpcServer.addTerseExceptions(SafeModeException.class,
                FileNotFoundException.class,
                HadoopIllegalArgumentException.class,
                FileAlreadyExistsException.class,
                InvalidPathException.class,
                ParentNotDirectoryException.class,
                UnresolvedLinkException.class,
                AlreadyBeingCreatedException.class,
                QuotaExceededException.class,
                RecoveryInProgressException.class,
                AccessControlException.class,
                SecretManager.InvalidToken.class,
                LeaseExpiredException.class,
                NSQuotaExceededException.class,
                DSQuotaExceededException.class);
    }
    @Override
    public void monitorHealth() throws HealthCheckFailedException, AccessControlException, IOException {
        nn.monitorHealth();
    }

    @Override
    public void transitionToActive(StateChangeRequestInfo req) throws ServiceFailedException, AccessControlException, IOException {
        nn.checkHaStateChange(req);
        nn.transitionToActive();
    }

    @Override
    public void transitionToStandby(StateChangeRequestInfo req) throws ServiceFailedException, AccessControlException, IOException {
        nn.checkHaStateChange(req);
        nn.transitionToStandby();
    }

    @Override
    public HAServiceStatus getServiceStatus() throws AccessControlException, IOException {
        return nn.getServiceStatus();
  }

    @Override
    public LocatedBlocks getBlockLocations(String src, long offset, long length) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        metrics.incrGetBlockLocations();
        return namesystem.getBlockLocations(getClientMachine(),
                src, offset, length);
    }

    @Override
    public FsServerDefaults getServerDefaults() throws IOException {
        return namesystem.getServerDefaults();
    }

    @Override
    public HdfsFileStatus create(String src, FsPermission masked, String clientName, EnumSetWritable<CreateFlag> flag, boolean createParent, short replication, long blockSize) throws AccessControlException, AlreadyBeingCreatedException, DSQuotaExceededException, FileAlreadyExistsException, FileNotFoundException, NSQuotaExceededException, ParentNotDirectoryException, SafeModeException, UnresolvedLinkException, SnapshotAccessControlException, IOException {
        String clientMachine = getClientMachine();
        if (stateChangeLog.isDebugEnabled()) {
            stateChangeLog.debug("*DIR* NameNode.create: file "
                    + src + " for " + clientName + " at " + clientMachine);
        }
        if (!checkPathLength(src)) {
            throw new IOException("create: Pathname too long.  Limit "
                    + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
        }
        HdfsFileStatus fileStatus = namesystem.startFile(src, new PermissionStatus(
                        getRemoteUser().getShortUserName(), null, masked),
                clientName, clientMachine, flag.get(), createParent, replication,
                blockSize);
        metrics.incrFilesCreated();
        metrics.incrCreateFileOps();
        return fileStatus;
    }

    @Override
    public LocatedBlock append(String src, String clientName) throws AccessControlException, DSQuotaExceededException, FileNotFoundException, SafeModeException, UnresolvedLinkException, SnapshotAccessControlException, IOException {
        String clientMachine = getClientMachine();
        if (stateChangeLog.isDebugEnabled()) {
            stateChangeLog.debug("*DIR* NameNode.append: file "
                    + src + " for " + clientName + " at " + clientMachine);
        }
        LocatedBlock info = namesystem.appendFile(src, clientName, clientMachine);
        metrics.incrFilesAppended();
        return info;
    }

    @Override
    public boolean setReplication(String src, short replication) throws AccessControlException, DSQuotaExceededException, FileNotFoundException, SafeModeException, UnresolvedLinkException, SnapshotAccessControlException, IOException {
        return namesystem.setReplication(src, replication);

    }

    @Override
    public void setPermission(String src, FsPermission permission) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, SnapshotAccessControlException, IOException {
        namesystem.setPermission(src, permission);

    }

    @Override
    public void setOwner(String src, String username, String groupname) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, SnapshotAccessControlException, IOException {
        namesystem.setOwner(src, username, groupname);

    }

    @Override
    public void abandonBlock(ExtendedBlock b, String src, String holder) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        if (stateChangeLog.isDebugEnabled()) {
            stateChangeLog.debug("*BLOCK* NameNode.abandonBlock: "
                    + b + " of file " + src);
        }
        if (!namesystem.abandonBlock(b, src, holder)) {
            throw new IOException("Cannot abandon block during write to " + src);
        }
    }

    @Override
    public LocatedBlock addBlock(String src, String clientName, ExtendedBlock previous, DatanodeInfo[] excludedNodes, long fileId, String[] favoredNodes) throws AccessControlException, FileNotFoundException, NotReplicatedYetException, SafeModeException, UnresolvedLinkException, IOException {
        if (stateChangeLog.isDebugEnabled()) {
            stateChangeLog.debug("*BLOCK* NameNode.addBlock: file " + src
                    + " fileId=" + fileId + " for " + clientName);
        }
        HashMap<Node, Node> excludedNodesSet = null;
        if (excludedNodes != null) {
            excludedNodesSet = new HashMap<Node, Node>(excludedNodes.length);
            for (Node node : excludedNodes) {
                excludedNodesSet.put(node, node);
            }
        }
        List<String> favoredNodesList = (favoredNodes == null) ? null
                : Arrays.asList(favoredNodes);
        LocatedBlock locatedBlock = namesystem.getAdditionalBlock(src, fileId,
                clientName, previous, excludedNodesSet, favoredNodesList);
        if (locatedBlock != null)
            metrics.incrAddBlockOps();
        return locatedBlock;
    }

    @Override
    public LocatedBlock getAdditionalDatanode(String src, ExtendedBlock blk, DatanodeInfo[] existings, DatanodeInfo[] excludes, int numAdditionalNodes, String clientName) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getAdditionalDatanode: src=" + src
                    + ", blk=" + blk
                    + ", existings=" + Arrays.asList(existings)
                    + ", excludes=" + Arrays.asList(excludes)
                    + ", numAdditionalNodes=" + numAdditionalNodes
                    + ", clientName=" + clientName);
        }

        metrics.incrGetAdditionalDatanodeOps();

        HashMap<Node, Node> excludeSet = null;
        if (excludes != null) {
            excludeSet = new HashMap<Node, Node>(excludes.length);
            for (Node node : excludes) {
                excludeSet.put(node, node);
            }
        }
        return namesystem.getAdditionalDatanode(src, blk,
                existings, excludeSet, numAdditionalNodes, clientName);
    }

    @Override
    public boolean complete(String src, String clientName, ExtendedBlock last, long fileId) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, IOException {
        if (stateChangeLog.isDebugEnabled()) {
            stateChangeLog.debug("*DIR* NameNode.complete: "
                    + src + " fileId=" + fileId + " for " + clientName);
        }
        return namesystem.completeFile(src, clientName, last, fileId);
    }

    @Override
    public DatanodeRegistration registerDatanode(DatanodeRegistration nodeReg) throws IOException {
        verifyLayoutVersion(nodeReg.getVersion());
        verifySoftwareVersion(nodeReg);
        namesystem.registerDatanode(nodeReg);
        return nodeReg;
    }
    void verifyLayoutVersion(int version) throws IOException {
        if (version != HdfsConstants.LAYOUT_VERSION)
            throw new IncorrectVersionException(version, "data node");
    }
    private void verifySoftwareVersion(DatanodeRegistration dnReg)
            throws IncorrectVersionException {
        String dnVersion = dnReg.getSoftwareVersion();
        if (VersionUtil.compareVersions(dnVersion, minimumDataNodeVersion) < 0) {
            IncorrectVersionException ive = new IncorrectVersionException(
                    minimumDataNodeVersion, dnVersion, "DataNode", "NameNode");
            LOG.warn(ive.getMessage() + " DN: " + dnReg);
            throw ive;
        }
        String nnVersion = VersionInfo.getVersion();
        if (!dnVersion.equals(nnVersion)) {
            String messagePrefix = "Reported DataNode version '" + dnVersion +
                    "' of DN " + dnReg + " does not match NameNode version '" +
                    nnVersion + "'";
            long nnCTime = nn.getFSImage().getStorage().getCTime();
            long dnCTime = dnReg.getStorageInfo().getCTime();
            if (nnCTime != dnCTime) {
                IncorrectVersionException ive = new IncorrectVersionException(
                        messagePrefix + " and CTime of DN ('" + dnCTime +
                                "') does not match CTime of NN ('" + nnCTime + "')");
                LOG.warn(ive);
                throw ive;
            } else {
                LOG.info(messagePrefix +
                        ". Note: This is normal during a rolling upgrade.");
            }
        }
    }
    @Override
    public HeartbeatResponse sendHeartbeat(DatanodeRegistration nodeReg, StorageReport[] reports, int xmitsInProgress, int xceiverCount, int failedVolumes) throws IOException {
        verifyRequest(nodeReg);
        return namesystem.handleHeartbeat(nodeReg, reports[0].getCapacity(),
                reports[0].getDfsUsed(), reports[0].getRemaining(),
                reports[0].getBlockPoolUsed(), xceiverCount, xmitsInProgress,
                failedVolumes);
    }
    void verifyRequest(NodeRegistration nodeReg) throws IOException {
        verifyLayoutVersion(nodeReg.getVersion());
        if (!namesystem.getRegistrationID().equals(nodeReg.getRegistrationID())) {
            LOG.warn("Invalid registrationID - expected: "
                    + namesystem.getRegistrationID() + " received: "
                    + nodeReg.getRegistrationID());
            throw new UnregisteredNodeException(nodeReg);
        }
    }
    @Override
    public DatanodeCommand blockReport(DatanodeRegistration nodeReg, String poolId, StorageBlockReport[] reports) throws IOException {
            verifyRequest(nodeReg);
            BlockListAsLongs blist = new BlockListAsLongs(reports[0].getBlocks());
            if (blockStateChangeLog.isDebugEnabled()) {
                blockStateChangeLog.debug("*BLOCK* NameNode.blockReport: "
                        + "from " + nodeReg + " " + blist.getNumberOfBlocks()
                        + " blocks");
            }

            namesystem.getBlockManager().processReport(nodeReg, poolId, blist);
            if (nn.getFSImage().isUpgradeFinalized() && !nn.isStandbyState())
                return new FinalizeCommand(poolId);
            return null;
    }

    @Override
    public void blockReceivedAndDeleted(DatanodeRegistration nodeReg, String poolId, StorageReceivedDeletedBlocks[] receivedAndDeletedBlocks) throws IOException {
        verifyRequest(nodeReg);
        if (blockStateChangeLog.isDebugEnabled()) {
            blockStateChangeLog.debug("*BLOCK* NameNode.blockReceivedAndDeleted: "
                    + "from " + nodeReg + " " + receivedAndDeletedBlocks.length
                    + " blocks.");
        }
        namesystem.processIncrementalBlockReport(
                nodeReg, poolId, receivedAndDeletedBlocks[0].getBlocks());
    }

    @Override
    public void errorReport(DatanodeRegistration nodeReg, int errorCode, String msg) throws IOException {
        String dnName =
                (nodeReg == null) ? "Unknown DataNode" : nodeReg.toString();

        if (errorCode == DatanodeProtocol.NOTIFY) {
            LOG.info("Error report from " + dnName + ": " + msg);
            return;
        }
        verifyRequest(nodeReg);

        if (errorCode == DatanodeProtocol.DISK_ERROR) {
            LOG.warn("Disk error on " + dnName + ": " + msg);
        } else if (errorCode == DatanodeProtocol.FATAL_DISK_ERROR) {
            LOG.warn("Fatal disk error on " + dnName + ": " + msg);
            namesystem.getBlockManager().getDatanodeManager().removeDatanode(nodeReg);
        } else {
            LOG.info("Error report from " + dnName + ": " + msg);
        }
    }

    @Override
    public BlocksWithLocations getBlocks(DatanodeInfo datanode, long size) throws IOException {
        if (size <= 0) {
            throw new IllegalArgumentException(
                    "Unexpected not positive size: " + size);
        }
        namesystem.checkSuperuserPrivilege();
        return namesystem.getBlockManager().getBlocks(datanode, size);
    }

    @Override
    public ExportedBlockKeys getBlockKeys() throws IOException {
        namesystem.checkSuperuserPrivilege();
        return namesystem.getBlockManager().getBlockKeys();
    }

    @Override
    public long getTransactionID() throws IOException {
        namesystem.checkOperation(NameNode.OperationCategory.UNCHECKED);
        namesystem.checkSuperuserPrivilege();
        return namesystem.getFSImage().getLastAppliedOrWrittenTxId();
    }

    @Override
    public long getMostRecentCheckpointTxId() throws IOException {
        namesystem.checkOperation(NameNode.OperationCategory.UNCHECKED);
        namesystem.checkSuperuserPrivilege();
        return namesystem.getFSImage().getMostRecentCheckpointTxId();
    }

    @Override
    public CheckpointSignature rollEditLog() throws IOException {
        namesystem.checkSuperuserPrivilege();
        return namesystem.rollEditLog();
    }

    @Override
    public NamespaceInfo versionRequest() throws IOException {
        namesystem.checkSuperuserPrivilege();
        return namesystem.getNamespaceInfo();
    }

    @Override
    public void errorReport(NamenodeRegistration registration, int errorCode, String msg) throws IOException {
        namesystem.checkOperation(NameNode.OperationCategory.UNCHECKED);
        namesystem.checkSuperuserPrivilege();
        verifyRequest(registration);
        LOG.info("Error report from " + registration + ": " + msg);
        if (errorCode == FATAL) {
            namesystem.releaseBackupNode(registration);
        }
    }

    @Override
    public NamenodeRegistration registerSubordinateNamenode(NamenodeRegistration registration) throws IOException {
        namesystem.checkSuperuserPrivilege();
        verifyLayoutVersion(registration.getVersion());
        NamenodeRegistration myRegistration = nn.setRegistration();
        namesystem.registerBackupNode(registration, myRegistration);
        return myRegistration;
    }

    @Override
    public NamenodeCommand startCheckpoint(NamenodeRegistration registration) throws IOException {
        namesystem.checkSuperuserPrivilege();
        verifyRequest(registration);
        if (!nn.isRole(HdfsServerConstants.NamenodeRole.NAMENODE))
            throw new IOException("Only an ACTIVE node can invoke startCheckpoint.");
        return namesystem.startCheckpoint(registration, nn.setRegistration());
    }

    @Override
    public void endCheckpoint(NamenodeRegistration registration, CheckpointSignature sig) throws IOException {
        namesystem.checkSuperuserPrivilege();
        namesystem.endCheckpoint(registration, sig);
    }

    @Override
    public RemoteEditLogManifest getEditLogManifest(long sinceTxId) throws IOException {
        namesystem.checkOperation(NameNode.OperationCategory.READ);
        namesystem.checkSuperuserPrivilege();
        return namesystem.getEditLog().getEditLogManifest(sinceTxId);
    }

    @Override
    public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
        namesystem.reportBadBlocks(blocks);

    }

    @Override
    public void commitBlockSynchronization(ExtendedBlock block, long newgenerationstamp, long newlength, boolean closeFile, boolean deleteblock, DatanodeID[] newtargets, String[] newtargetstorages) throws IOException {
        namesystem.commitBlockSynchronization(block, newgenerationstamp,
                newlength, closeFile, deleteblock, newtargets, newtargetstorages);
    }

    @Override
    public boolean rename(String src, String dst) throws UnresolvedLinkException, SnapshotAccessControlException, IOException {
            if (stateChangeLog.isDebugEnabled()) {
                stateChangeLog.debug("*DIR* NameNode.rename: " + src + " to " + dst);
            }
            if (!checkPathLength(dst)) {
                throw new IOException("rename: Pathname too long.  Limit "
                        + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
            }
            boolean ret = namesystem.renameTo(src, dst);
            if (ret) {
                metrics.incrFilesRenamed();
            }
            return ret;
    }

    @Override
    public void concat(String trg, String[] srcs) throws IOException, UnresolvedLinkException, SnapshotAccessControlException {
        namesystem.concat(trg, srcs);

    }

    @Override
    public void rename2(String src, String dst, Options.Rename... options) throws AccessControlException, DSQuotaExceededException, FileAlreadyExistsException, FileNotFoundException, NSQuotaExceededException, ParentNotDirectoryException, SafeModeException, UnresolvedLinkException, SnapshotAccessControlException, IOException {
        if (stateChangeLog.isDebugEnabled()) {
            stateChangeLog.debug("*DIR* NameNode.rename: " + src + " to " + dst);
        }
        if (!checkPathLength(dst)) {
            throw new IOException("rename: Pathname too long.  Limit "
                    + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
        }
        namesystem.renameTo(src, dst, options);
        metrics.incrFilesRenamed();
    }

    @Override
    public boolean delete(String src, boolean recursive) throws AccessControlException, FileNotFoundException, SafeModeException, UnresolvedLinkException, SnapshotAccessControlException, IOException {
        Path srcPath = new Path(src);
        return (src.length() <= MAX_PATH_LENGTH &&
                srcPath.depth() <= MAX_PATH_DEPTH);
    }

    @Override
    public boolean mkdirs(String src, FsPermission masked, boolean createParent) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, NSQuotaExceededException, ParentNotDirectoryException, SafeModeException, UnresolvedLinkException, SnapshotAccessControlException, IOException {
        if (stateChangeLog.isDebugEnabled()) {
            stateChangeLog.debug("*DIR* NameNode.mkdirs: " + src);
        }
        if (!checkPathLength(src)) {
            throw new IOException("mkdirs: Pathname too long.  Limit "
                    + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
        }
        return namesystem.mkdirs(src,
                new PermissionStatus(getRemoteUser().getShortUserName(),
                        null, masked), createParent);
    }

    @Override
    public DirectoryListing getListing(String src, byte[] startAfter, boolean needLocation) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        DirectoryListing files = namesystem.getListing(
                src, startAfter, needLocation);
        if (files != null) {
            metrics.incrGetListingOps();
            metrics.incrFilesInGetListingOps(files.getPartialListing().length);
        }
        return files;
    }

    @Override
    public SnapshottableDirectoryStatus[] getSnapshottableDirListing() throws IOException {
        SnapshottableDirectoryStatus[] status = namesystem
                .getSnapshottableDirListing();
        metrics.incrListSnapshottableDirOps();
        return status;
    }

    @Override
    public void renewLease(String clientName) throws AccessControlException, IOException {
        namesystem.renewLease(clientName);

    }

    @Override
    public boolean recoverLease(String src, String clientName) throws IOException {
        String clientMachine = getClientMachine();
        return namesystem.recoverLease(src, clientName, clientMachine);
    }

    @Override
    public long[] getStats() throws IOException {
        namesystem.checkOperation(NameNode.OperationCategory.READ);
        return namesystem.getStats();
    }

    @Override
    public DatanodeInfo[] getDatanodeReport(HdfsConstants.DatanodeReportType type) throws IOException {
        DatanodeInfo results[] = namesystem.datanodeReport(type);
        if (results == null) {
            throw new IOException("Cannot find datanode report");
        }
        return results;
    }

    @Override
    public long getPreferredBlockSize(String filename) throws IOException, UnresolvedLinkException {
        return namesystem.getPreferredBlockSize(filename);

    }

    @Override
    public boolean setSafeMode(HdfsConstants.SafeModeAction action, boolean isChecked) throws IOException {
        NameNode.OperationCategory opCategory = NameNode.OperationCategory.UNCHECKED;
        if (isChecked) {
            if (action == HdfsConstants.SafeModeAction.SAFEMODE_GET) {
                opCategory = NameNode.OperationCategory.READ;
            } else {
                opCategory = NameNode.OperationCategory.WRITE;
            }
        }
        namesystem.checkOperation(opCategory);
        return namesystem.setSafeMode(action);
    }

    @Override
    public void saveNamespace() throws AccessControlException, IOException {
        namesystem.saveNamespace();

    }

    @Override
    public long rollEdits() throws AccessControlException, IOException {
        CheckpointSignature sig = namesystem.rollEditLog();
        return sig.getCurSegmentTxId();
    }

    @Override
    public boolean restoreFailedStorage(String arg) throws AccessControlException, IOException {
        return namesystem.restoreFailedStorage(arg);

    }

    @Override
    public void refreshNodes() throws IOException {
        namesystem.refreshNodes();

    }

    @Override
    public void finalizeUpgrade() throws IOException {
        namesystem.finalizeUpgrade();

    }

    @Override
    public CorruptFileBlocks listCorruptFileBlocks(String path, String cookie) throws IOException {
        String[] cookieTab = new String[]{cookie};
        Collection<FileNamesystem.CorruptFileBlockInfo> fbs =
                namesystem.listCorruptFileBlocks(path, cookieTab);

        String[] files = new String[fbs.size()];
        int i = 0;
        for (FileNamesystem.CorruptFileBlockInfo fb : fbs) {
            files[i++] = fb.path;
        }
        return new CorruptFileBlocks(files, cookieTab[0]);
    }

    @Override
    public void metaSave(String filename) throws IOException {
        namesystem.metaSave(filename);

    }

    @Override
    public void setBalancerBandwidth(long bandwidth) throws IOException {
        namesystem.setBalancerBandwidth(bandwidth);

    }

    @Override
    public HdfsFileStatus getFileInfo(String src) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        metrics.incrFileInfoOps();
        return namesystem.getFileInfo(src, true);
    }

    @Override
    public boolean isFileClosed(String src) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        return namesystem.isFileClosed(src);
    }

    @Override
    public HdfsFileStatus getFileLinkInfo(String src) throws AccessControlException, UnresolvedLinkException, IOException {
        metrics.incrFileInfoOps();
        return namesystem.getFileInfo(src, false);
    }

    @Override
    public ContentSummary getContentSummary(String path) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        return namesystem.getContentSummary(path);

    }

    @Override
    public void setQuota(String path, long namespaceQuota, long diskspaceQuota) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, SnapshotAccessControlException, IOException {
        namesystem.setQuota(path, namespaceQuota, diskspaceQuota);

    }

    @Override
    public void fsync(String src, String client, long lastBlockLength) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
        namesystem.fsync(src, client, lastBlockLength);

    }

    @Override
    public void setTimes(String src, long mtime, long atime) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, SnapshotAccessControlException, IOException {
        namesystem.setTimes(src, mtime, atime);

    }

    @Override
    public void createSymlink(String target, String link, FsPermission dirPerms, boolean createParent) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, SafeModeException, UnresolvedLinkException, SnapshotAccessControlException, IOException {
        metrics.incrCreateSymlinkOps();
        /* We enforce the MAX_PATH_LENGTH limit even though a symlink target
         * URI may refer to a non-HDFS file system.
         */
        if (!checkPathLength(link)) {
            throw new IOException("Symlink path exceeds " + MAX_PATH_LENGTH +
                    " character limit");

        }
        if ("".equals(target)) {
            throw new IOException("Invalid symlink target");
        }
        final UserGroupInformation ugi = getRemoteUser();
        namesystem.createSymlink(target, link,
                new PermissionStatus(ugi.getShortUserName(), null, dirPerms), createParent);
    }

    @Override
    public String getLinkTarget(String path) throws AccessControlException, FileNotFoundException, IOException {
        metrics.incrGetLinkTargetOps();
        HdfsFileStatus stat = null;
        try {
            stat = namesystem.getFileInfo(path, false);
        } catch (UnresolvedPathException e) {
            return e.getResolvedPath().toString();
        } catch (UnresolvedLinkException e) {
            // The NameNode should only throw an UnresolvedPathException
            throw new AssertionError("UnresolvedLinkException thrown");
        }
        if (stat == null) {
            throw new FileNotFoundException("File does not exist: " + path);
        } else if (!stat.isSymlink()) {
            throw new IOException("Path " + path + " is not a symbolic link");
        }
        return stat.getSymlink();
    }

    @Override
    public LocatedBlock updateBlockForPipeline(ExtendedBlock block, String clientName) throws IOException {
        return namesystem.updateBlockForPipeline(block, clientName);

    }

    @Override
    public void updatePipeline(String clientName, ExtendedBlock oldBlock, ExtendedBlock newBlock, DatanodeID[] newNodes) throws IOException {
        namesystem.updatePipeline(clientName, oldBlock, newBlock, newNodes);

    }

    @Override
    public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer) throws IOException {
        return namesystem.getDelegationToken(renewer);

    }

    @Override
    public long renewDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException {
        return namesystem.renewDelegationToken(token);

    }

    @Override
    public void cancelDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException {
        namesystem.cancelDelegationToken(token);

    }

    @Override
    public DataEncryptionKey getDataEncryptionKey() throws IOException {
        return namesystem.getBlockManager().generateDataEncryptionKey();

    }

    @Override
    public String createSnapshot(String snapshotRoot, String snapshotName) throws IOException {
        if (!checkPathLength(snapshotRoot)) {
            throw new IOException("createSnapshot: Pathname too long.  Limit "
                    + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
        }
        metrics.incrCreateSnapshotOps();
        return namesystem.createSnapshot(snapshotRoot, snapshotName);
    }

    @Override
    public void deleteSnapshot(String snapshotRoot, String snapshotName) throws IOException {
        metrics.incrDeleteSnapshotOps();
        namesystem.deleteSnapshot(snapshotRoot, snapshotName);
    }

    @Override
    public void renameSnapshot(String snapshotRoot, String snapshotOldName, String snapshotNewName) throws IOException {
        if (snapshotNewName == null || snapshotNewName.isEmpty()) {
            throw new IOException("The new snapshot name is null or empty.");
        }
        metrics.incrRenameSnapshotOps();
        namesystem.renameSnapshot(snapshotRoot, snapshotOldName, snapshotNewName);
    }

    @Override
    public void allowSnapshot(String snapshotRoot) throws IOException {
        metrics.incrAllowSnapshotOps();
        namesystem.allowSnapshot(snapshotRoot);
    }

    @Override
    public void disallowSnapshot(String snapshotRoot) throws IOException {
        metrics.incrDisAllowSnapshotOps();
        namesystem.disallowSnapshot(snapshotRoot);
    }

    @Override
    public SnapshotDiffReport getSnapshotDiffReport(String snapshotRoot, String fromSnapshot, String toSnapshot) throws IOException {
        SnapshotDiffReport report = namesystem.getSnapshotDiffReport(snapshotRoot,
                fromSnapshot, toSnapshot);
        metrics.incrSnapshotDiffReportOps();
        return report;
    }

    @Override
    public void refreshUserToGroupsMappings() throws IOException {
        LOG.info("Refreshing all user-to-groups mappings. Requested by user: " +
                getRemoteUser().getShortUserName());
        Groups.getUserToGroupsMappingService().refresh();
    }

    @Override
    public void refreshSuperUserGroupsConfiguration() throws IOException {
        LOG.info("Refreshing SuperUser proxy group mapping list ");

        ProxyUsers.refreshSuperUserGroupsConfiguration();
    }

    @Override
    public void refreshServiceAcl() throws IOException {
        if (!serviceAuthEnabled) {
            throw new AuthorizationException("Service Level Authorization not enabled!");
        }

        this.clientRpcServer.refreshServiceAcl(new Configuration(), new HDFSPolicyProvider());
        if (this.serviceRpcServer != null) {
            this.serviceRpcServer.refreshServiceAcl(new Configuration(), new HDFSPolicyProvider());
        }
    }

    @Override
    public String[] getGroupsForUser(String user) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Getting groups for user " + user);
        }
        return UserGroupInformation.createRemoteUser(user).getGroupNames();
    }
    private static String getClientMachine() {
        String clientMachine = NamenodeWebHdfsMethods.getRemoteAddress();
        if (clientMachine == null) { //not a web client
            clientMachine = Server.getRemoteAddress();
        }
        if (clientMachine == null) { //not a RPC client
            clientMachine = "";
        }
        return clientMachine;
    }
  public   InetSocketAddress getRpcAddress() {
        return clientRpcAddress;
    }

    private boolean checkPathLength(String src) {
        Path srcPath = new Path(src);
        return (src.length() <= MAX_PATH_LENGTH &&
                srcPath.depth() <= MAX_PATH_DEPTH);
    }

    private static UserGroupInformation getRemoteUser() throws IOException {
        return NameNodeTest.getRemoteUser();
    }
}
