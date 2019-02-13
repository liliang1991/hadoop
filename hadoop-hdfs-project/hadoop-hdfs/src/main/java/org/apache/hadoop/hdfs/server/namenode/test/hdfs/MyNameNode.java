package org.apache.hadoop.hdfs.server.namenode.test.hdfs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.ha.HealthCheckFailedException;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ha.ActiveState;
import org.apache.hadoop.hdfs.server.namenode.ha.HAContext;
import org.apache.hadoop.hdfs.server.namenode.ha.HAState;
import org.apache.hadoop.hdfs.server.namenode.ha.StandbyState;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.fs.FileNamesystem;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.fs.FileSystenImage;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.rpc.FileNameNodeHttpServer;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.rpc.FileNameNodeRpcServer;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ServicePlugin;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.List;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.apache.hadoop.util.ExitUtil.terminate;


public class MyNameNode {
   static Configuration conf=new HdfsConfiguration();
    static {
        HdfsConfiguration.init();
        Configuration.dumpDeprecatedKeys();

    }
    FileNameNodeRpcServer rpcServer;
    protected NamenodeRegistration nodeRegistration;

    FileNameNodeHttpServer httpServer;
    NamenodeRole role;
    public static final HAState ACTIVE_STATE = new ActiveState();
    public static final HAState STANDBY_STATE = new StandbyState();
    static NameNodeMetrics metrics;

    private static final StartupProgress startupProgress = new StartupProgress();
    public static final Log stateChangeLog = LogFactory.getLog("org.apache.hadoop.hdfs.StateChange");
    public static final Log blockStateChangeLog = LogFactory.getLog("BlockStateChange");
    protected boolean allowStaleStandbyReads=conf.getBoolean("dfs.ha.allow.stale.reads", false);
    boolean haEnabled;
    public static final int DEFAULT_PORT = 8020;

    public   FileNamesystem namesystem;
     HAState state=new ActiveState();

    HAContext haContext=new NameNodeHAContext();
    private Thread emptier;
    private List<ServicePlugin> plugins;

    NamenodeRole namenodeRole= NamenodeRole.NAMENODE;
    public static final Log LOG = LogFactory.getLog(MyNameNode.class.getName());
    public MyNameNode()throws  Exception{
        this.namesystem=FileNamesystem.getLoadDisk(conf);
        metrics = NameNodeMetrics.create(conf, NamenodeRole.NAMENODE);

        state.prepareToEnterState(haContext);
        state.enterState(haContext);
        initialize(conf);

    }
    void checkOperation(NameNode.OperationCategory op) throws StandbyException{
        state.checkOperation(haContext, op);

    };
    public boolean isStandbyState() {
        return (state.equals(STANDBY_STATE));
    }

  public   NamenodeRegistration setRegistration() {
        nodeRegistration = new NamenodeRegistration(
                NetUtils.getHostPortString(rpcServer.getRpcAddress()),
                NetUtils.getHostPortString(getHttpAddress()),
                getFSImage().getStorage(), getRole());
        return nodeRegistration;
    }
  public   boolean isRole(NamenodeRole that) {
        return role.equals(that);
    }

    public static void main(String[] args) {
       try {
           System.out.println(conf.size());
           StartupOption startOpt = StartupOption.REGULAR;
           conf.set(DFSConfigKeys.DFS_NAMENODE_STARTUP_KEY, startOpt.toString());
           System.out.println(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY);
           MyNameNode namenode=new MyNameNode();
           if (namenode != null) {
               namenode.join();
           }
       }catch (Exception e){
           e.printStackTrace();
       }

    }
    protected void initialize(Configuration conf) throws IOException{

        rpcServer = createRpcServer(conf);

        startCommonServices(conf);


    }
    /**
     * Wait for service to finish.
     * (Normally, it runs forever.)
     */
    public void join() {
        try {
            rpcServer.join();
        } catch (InterruptedException ie) {
            LOG.info("Caught interrupted exception ", ie);
        }
    }
    private void startCommonServices(Configuration conf) throws IOException {
        namesystem.startCommonServices(conf, haContext);
    /*    if (NamenodeRole.NAMENODE != role) {
            startHttpServer(conf);
            httpServer.setNameNodeAddress(getNameNodeAddress());
            httpServer.setFSImage(getFSImage());
        }*/
        rpcServer.start();
        plugins = conf.getInstances(DFS_NAMENODE_PLUGINS_KEY,
                ServicePlugin.class);
        for (ServicePlugin p: plugins) {
            try {
                p.start(this);
            } catch (Throwable t) {
                LOG.warn("ServicePlugin " + p + " could not be started", t);
            }
        }
        LOG.info(getRole() + " RPC up at: " + rpcServer.getRpcAddress());
        if (rpcServer.getServiceRpcAddress() != null) {
            LOG.info(getRole() + " service RPC up at: "
                    + rpcServer.getServiceRpcAddress());
        }
    }
    private void startHttpServer(final Configuration conf) throws IOException {
        httpServer = new FileNameNodeHttpServer(conf, this, getHttpServerAddress(conf));
        httpServer.start();
        httpServer.setStartupProgress(startupProgress);
        setHttpServerAddress(conf);
    }
    protected InetSocketAddress getHttpServerAddress(Configuration conf) {
        return getHttpAddress(conf);
    }
    /** @return the NameNode HTTP address set in the conf. */
    public static InetSocketAddress getHttpAddress(Configuration conf) {
        return  NetUtils.createSocketAddr(
                conf.get(DFS_NAMENODE_HTTP_ADDRESS_KEY, DFS_NAMENODE_HTTP_ADDRESS_DEFAULT));
    }
    protected void setHttpServerAddress(Configuration conf) {
        String hostPort = NetUtils.getHostPortString(getHttpAddress());
        conf.set(DFS_NAMENODE_HTTP_ADDRESS_KEY, hostPort);
        LOG.info("Web-server up at: " + hostPort);
    }
    public InetSocketAddress getHttpAddress() {
        return httpServer.getHttpAddress();
    }

    public FileSystenImage getFSImage() {
        return namesystem.dir.fsImage;
    }
    public FileNamesystem getNamesystem() {
        return namesystem;
    }
    protected FileNameNodeRpcServer createRpcServer(Configuration conf)
            throws IOException {
        return new FileNameNodeRpcServer(conf, this);
    }
   public static StartupOption getStartupOption(Configuration conf) {
        return StartupOption.valueOf(conf.get(DFS_NAMENODE_STARTUP_KEY,
                StartupOption.REGULAR.toString()));
    }
    public static StartupProgress getStartupProgress() {
        return startupProgress;
    }
    //
    // Common NameNode methods implementation for the active name-node role.
    //
    public NamenodeRole getRole() {
        return role;
    }

    public class NameNodeHAContext implements HAContext {
        @Override
        public void setState(HAState s) {
            state = s;
        }

        @Override
        public HAState getState() {
            return state;
        }

        @Override
        public void startActiveServices() throws IOException {
            try {
              //  namesystem.startActiveServices();
                startTrashEmptier(conf);
            } catch (Throwable t) {
                doImmediateShutdown(t);
            }
        }

        @Override
        public void stopActiveServices() throws IOException {
            try {
                if (namesystem != null) {
             //       namesystem.stopActiveServices();
                }
                stopTrashEmptier();
            } catch (Throwable t) {
                doImmediateShutdown(t);
            }
        }

        @Override
        public void startStandbyServices() throws IOException {
            try {
          //      namesystem.startStandbyServices(conf);
            } catch (Throwable t) {
                doImmediateShutdown(t);
            }
        }

        @Override
        public void prepareToStopStandbyServices() throws ServiceFailedException {
            try {
       //         namesystem.prepareToStopStandbyServices();
            } catch (Throwable t) {
                doImmediateShutdown(t);
            }
        }

        @Override
        public void stopStandbyServices() throws IOException {
            try {
                if (namesystem != null) {
         //           namesystem.stopStandbyServices();
                }
            } catch (Throwable t) {
                doImmediateShutdown(t);
            }
        }

        @Override
        public void writeLock() {
            namesystem.writeLock();
        }

        @Override
        public void writeUnlock() {
            namesystem.writeUnlock();
        }

        /** Check if an operation of given category is allowed */
        @Override
        public void checkOperation(final NameNode.OperationCategory op)
                throws StandbyException {
            state.checkOperation(haContext, op);
        }

        @Override
        public boolean allowStaleReads() {
            return allowStaleStandbyReads;
        }

    }
    private void startTrashEmptier(final Configuration conf) throws IOException {
        long trashInterval =
                conf.getLong(FS_TRASH_INTERVAL_KEY, FS_TRASH_INTERVAL_DEFAULT);
        if (trashInterval == 0) {
            return;
        } else if (trashInterval < 0) {
            throw new IOException("Cannot start trash emptier with negative interval."
                    + " Set " + FS_TRASH_INTERVAL_KEY + " to a positive value.");
        }

        // This may be called from the transitionToActive code path, in which
        // case the current user is the administrator, not the NN. The trash
        // emptier needs to run as the NN. See HDFS-3972.
        FileSystem fs = SecurityUtil.doAsLoginUser(
                new PrivilegedExceptionAction<FileSystem>() {
                    @Override
                    public FileSystem run() throws IOException {
                        return FileSystem.get(conf);
                    }
                });
        this.emptier = new Thread(new Trash(fs, conf).getEmptier(), "Trash Emptier");
        this.emptier.setDaemon(true);
        this.emptier.start();
    }
    protected synchronized void doImmediateShutdown(Throwable t)
            throws ExitUtil.ExitException {
        String message = "Error encountered requiring NN shutdown. " +
                "Shutting down immediately.";
        try {
            LOG.fatal(message, t);
        } catch (Throwable ignored) {
            // This is unlikely to happen, but there's nothing we can do if it does.
        }
        terminate(1, t);
    }
    private void stopTrashEmptier() {
        if (this.emptier != null) {
            emptier.interrupt();
            emptier = null;
        }
    }
    public InetSocketAddress getServiceRpcServerAddress(Configuration conf) {
        return MyNameNode.getServiceAddress(conf, false);
    }
    public static InetSocketAddress getServiceAddress(Configuration conf,
                                                      boolean fallback) {
        String addr = conf.get(DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY);
        if (addr == null || addr.isEmpty()) {
            return fallback ? getAddress(conf) : null;
        }
        return getAddress(addr);
    }
    public static InetSocketAddress getAddress(String address) {
        return NetUtils.createSocketAddr(address, DEFAULT_PORT);
    }
    public static InetSocketAddress getAddress(Configuration conf) {
        URI filesystemURI = FileSystem.getDefaultUri(conf);
        return getAddress(filesystemURI);
    }
    public static InetSocketAddress getAddress(URI filesystemURI) {
        String authority = filesystemURI.getAuthority();
        if (authority == null) {
            throw new IllegalArgumentException(String.format(
                    "Invalid URI for NameNode address (check %s): %s has no authority.",
                    FileSystem.FS_DEFAULT_NAME_KEY, filesystemURI.toString()));
        }
        if (!HdfsConstants.HDFS_URI_SCHEME.equalsIgnoreCase(
                filesystemURI.getScheme())) {
            throw new IllegalArgumentException(String.format(
                    "Invalid URI for NameNode address (check %s): %s is not of scheme '%s'.",
                    FileSystem.FS_DEFAULT_NAME_KEY, filesystemURI.toString(),
                    HdfsConstants.HDFS_URI_SCHEME));
        }
        return getAddress(authority);
    }
    public InetSocketAddress getRpcServerAddress(Configuration conf) {
        return getAddress(conf);
    }
    public String getRpcServerBindHost(Configuration conf) {
        String addr = conf.getTrimmed(DFS_NAMENODE_RPC_BIND_HOST_KEY);
        if (addr == null || addr.isEmpty()) {
            return null;
        }
        return addr;
    }
    public void setRpcServerAddress(Configuration conf,
                                       InetSocketAddress rpcAddress) {
        FileSystem.setDefaultUri(conf, getUri(rpcAddress));
    }
    public static URI getUri(InetSocketAddress namenode) {
        int port = namenode.getPort();
        String portString = port == DEFAULT_PORT ? "" : (":"+port);
        return URI.create(HdfsConstants.HDFS_URI_SCHEME + "://"
                + namenode.getHostName()+portString);
    }

    public InetSocketAddress getNameNodeAddress() {
        return rpcServer.getRpcAddress();
    }

    public static UserGroupInformation getRemoteUser() throws IOException {
        UserGroupInformation ugi = Server.getRemoteUser();
        return (ugi != null) ? ugi : UserGroupInformation.getCurrentUser();
    }
    //rpc
   public synchronized void monitorHealth()
            throws HealthCheckFailedException, AccessControlException {
        namesystem.checkSuperuserPrivilege();
        if (!haEnabled) {
            return; // no-op, if HA is not enabled
        }
        getNamesystem().checkAvailableResources();
        if (!getNamesystem().nameNodeHasResourcesAvailable()) {
            throw new HealthCheckFailedException(
                    "The NameNode has no resources available");
        }
    }

    /**
     * Check that a request to change this node's HA state is valid.
     * In particular, verifies that, if auto failover is enabled, non-forced
     * requests from the HAAdmin CLI are rejected, and vice versa.
     *
     * @param req the request to check
     * @throws AccessControlException if the request is disallowed
     */
  public   void checkHaStateChange(HAServiceProtocol.StateChangeRequestInfo req)
            throws AccessControlException {
        boolean autoHaEnabled = conf.getBoolean(DFS_HA_AUTO_FAILOVER_ENABLED_KEY,
                DFS_HA_AUTO_FAILOVER_ENABLED_DEFAULT);
        switch (req.getSource()) {
            case REQUEST_BY_USER:
                if (autoHaEnabled) {
                    throw new AccessControlException(
                            "Manual HA control for this NameNode is disallowed, because " +
                                    "automatic HA is enabled.");
                }
                break;
            case REQUEST_BY_USER_FORCED:
                if (autoHaEnabled) {
                    LOG.warn("Allowing manual HA control from " +
                            Server.getRemoteAddress() +
                            " even though automatic HA is enabled, because the user " +
                            "specified the force flag");
                }
                break;
            case REQUEST_BY_ZKFC:
                if (!autoHaEnabled) {
                    throw new AccessControlException(
                            "Request from ZK failover controller at " +
                                    Server.getRemoteAddress() + " denied since automatic HA " +
                                    "is not enabled");
                }
                break;
        }
    }

  public   synchronized void transitionToActive()
            throws ServiceFailedException, AccessControlException {
        namesystem.checkSuperuserPrivilege();
        if (!haEnabled) {
            throw new ServiceFailedException("HA for namenode is not enabled");
        }
        state.setState(haContext, ACTIVE_STATE);
    }

  public   synchronized void transitionToStandby()
            throws ServiceFailedException, AccessControlException {
        namesystem.checkSuperuserPrivilege();
        if (!haEnabled) {
            throw new ServiceFailedException("HA for namenode is not enabled");
        }
        state.setState(haContext, STANDBY_STATE);
    }

  public   synchronized HAServiceStatus getServiceStatus()
            throws ServiceFailedException, AccessControlException {
        namesystem.checkSuperuserPrivilege();
        if (!haEnabled) {
            throw new ServiceFailedException("HA for namenode is not enabled");
        }
        if (state == null) {
            return new HAServiceStatus(HAServiceProtocol.HAServiceState.INITIALIZING);
        }
        HAServiceProtocol.HAServiceState retState = state.getServiceState();
        HAServiceStatus ret = new HAServiceStatus(retState);
        if (retState == HAServiceProtocol.HAServiceState.STANDBY) {
            String safemodeTip = namesystem.getSafeModeTip();
            if (!safemodeTip.isEmpty()) {
                ret.setNotReadyToBecomeActive(
                        "The NameNode is in safemode. " +
                                safemodeTip);
            } else {
                ret.setReadyToBecomeActive();
            }
        } else if (retState == HAServiceProtocol.HAServiceState.ACTIVE) {
            ret.setReadyToBecomeActive();
        } else {
            ret.setNotReadyToBecomeActive("State is " + state);
        }
        return ret;
    }
    public static NameNodeMetrics getNameNodeMetrics() {
        return metrics;
    }
    public String getServiceRpcServerBindHost(Configuration conf) {
        String addr = conf.getTrimmed(DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY);
        if (addr == null || addr.isEmpty()) {
            return null;
        }
        return addr;
    }
    public void setRpcServiceServerAddress(Configuration conf,
                                              InetSocketAddress serviceRPCAddress) {
        setServiceAddress(conf, NetUtils.getHostPortString(serviceRPCAddress));
    }
    public static void setServiceAddress(Configuration conf,
                                         String address) {
        LOG.info("Setting ADDRESS " + address);
        conf.set(DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, address);
    }
}
