package org.apache.hadoop.hdfs.server.namenode.test.hdfs.fs;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocol.datatransfer.ReplaceDatanodeOnFailure;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.blockmanagement.*;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.namenode.ha.HAContext;
import org.apache.hadoop.hdfs.server.namenode.ha.HAState;
import org.apache.hadoop.hdfs.server.namenode.metrics.FSNamesystemMBean;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectorySnapshottable;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeFileWithSnapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.*;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.NameNodeTest;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.block.FileINodeDirectorySnapshottable;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.block.FileINodeId;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.block.FileINodesInPath;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.block.FileSequentialBlockIdGenerator;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.manager.FileBlockManager;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.manager.FileDatanodeManager;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.manager.FileLeaseManager;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.manager.FileSnapshotManager;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.store.NameNodeStorage;
import org.apache.hadoop.hdfs.server.namenode.web.resources.NamenodeWebHdfsMethods;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RetryCache;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.VersionInfo;
import org.mortbay.util.ajax.JSON;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.apache.hadoop.util.Time.now;

public class FileNamesystem implements Namesystem, FSClusterStats,
        FSNamesystemMBean, NameNodeMXBean {
    public static final Log LOG = LogFactory.getLog(FileNamesystem.class);
    DelegationTokenSecretManager dtSecretManager;
    private static final Step STEP_AWAITING_REPORTED_BLOCKS =
            new Step(StepType.AWAITING_REPORTED_BLOCKS);
    FileLeaseManager leaseManager = new FileLeaseManager(this);
    ReentrantReadWriteLock fsLock = new ReentrantReadWriteLock(true);
    private final long resourceRecheckInterval;
    FileBlockManager blockManager;
    DatanodeStatistics datanodeStatistics;
    FileSequentialBlockIdGenerator blockIdGenerator;
    FsServerDefaults serverDefaults;
    FileINodeId inodeId;
    NameNodeStorage storage;
    long accessTimePrecision;
    long maxFsObjects;          // maximum number of fs objects
    private  String supergroup;
    long minBlockSize;         // minimum block size
    long maxBlocksPerFile;
    boolean supportAppends;
    boolean standbyShouldCheckpoint;
    boolean alwaysUseDelegationTokensForTests;
    Daemon smmthread = null;
    long startTime = now();
    ReplaceDatanodeOnFailure dtpReplaceDatanodeOnFailure;
    FileSnapshotManager snapshotManager;
    private String blockPoolId;
    FileNameNodeResourceChecker nnResourceChecker;
    boolean hasResourcesAvailable = false;
    public FileSystemDirectory dir;
    HAContext haContext;
    List<AuditLogger> auditLoggers;
    GenerationStamp generationStampV1 = new GenerationStamp();
    GenerationStamp generationStampV2 = new GenerationStamp();
    static int BLOCK_DELETION_INCREMENT = 1000;
    static final int DEFAULT_MAX_CORRUPT_FILEBLOCKS_RETURNED = 100;

    boolean haEnabled;
    boolean isPermissionEnabled;
  boolean fsRunning = true;
    private final UserGroupInformation fsOwner;
    private final String fsOwnerShortUserName;
    /** Total number of blocks. */
    int blockTotal;
    /** Number of safe blocks. */
    int blockSafe;
    /** Number of blocks needed to satisfy safe mode threshold condition */
    private int blockThreshold;
    /** Number of blocks needed before populating replication queues */
    private int blockReplQueueThreshold;
    private static final ThreadLocal<StringBuilder> auditBuffer =
            new ThreadLocal<StringBuilder>() {
                @Override
                protected StringBuilder initialValue() {
                    return new StringBuilder();
                }
            };
    public static final Log auditLog = LogFactory.getLog(
            FileNamesystem.class.getName() + ".audit");
    private double threshold;
    private long generationStampV1Limit =
            GenerationStamp.GRANDFATHER_GENERATION_STAMP;
    /**
     * Safe mode minimum number of datanodes alive
     */
    private int datanodeThreshold;
    /**
     * Safe mode extension after the threshold.
     */
    private int extension;
    /**
     * Min replication required by safe mode.
     */
    private int safeReplication;
    /**
     * threshold for populating needed replication queues
     */
    private double replQueueThreshold;
    private volatile SafeModeInfo safeMode;
    boolean isDefaultAuditLogger;
    RetryCache retryCache;
    public   boolean persistBlocks;

    private static final long DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL =
            TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
    FileNamesystem(Configuration conf, FileSystenImage fsImage, boolean ignoreRetryCache)
            throws IOException {
        this.blockManager = new FileBlockManager(this, this, conf);
        this.dtSecretManager = createDelegationTokenSecretManager(conf);

        String checksumTypeStr = conf.get(DFS_CHECKSUM_TYPE_KEY, DFS_CHECKSUM_TYPE_DEFAULT);
        DataChecksum.Type checksumType = DataChecksum.Type.valueOf(checksumTypeStr);
        this.resourceRecheckInterval = conf.getLong(
                DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY,
                DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_DEFAULT);
        this.serverDefaults = new FsServerDefaults(
                conf.getLongBytes(DFS_BLOCK_SIZE_KEY, DFS_BLOCK_SIZE_DEFAULT),
                conf.getInt(DFS_BYTES_PER_CHECKSUM_KEY, DFS_BYTES_PER_CHECKSUM_DEFAULT),
                conf.getInt(DFS_CLIENT_WRITE_PACKET_SIZE_KEY, DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT),
                (short) conf.getInt(DFS_REPLICATION_KEY, DFS_REPLICATION_DEFAULT),
                conf.getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT),
                conf.getBoolean(DFS_ENCRYPT_DATA_TRANSFER_KEY, DFS_ENCRYPT_DATA_TRANSFER_DEFAULT),
                conf.getLong(FS_TRASH_INTERVAL_KEY, FS_TRASH_INTERVAL_DEFAULT),
                checksumType);
        this.maxFsObjects = conf.getLong(DFS_NAMENODE_MAX_OBJECTS_KEY,
                DFS_NAMENODE_MAX_OBJECTS_DEFAULT);

        this.minBlockSize = conf.getLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY,
                DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_DEFAULT);
        this.maxBlocksPerFile = conf.getLong(DFSConfigKeys.DFS_NAMENODE_MAX_BLOCKS_PER_FILE_KEY,
                DFSConfigKeys.DFS_NAMENODE_MAX_BLOCKS_PER_FILE_DEFAULT);
        this.accessTimePrecision = conf.getLong(DFS_NAMENODE_ACCESSTIME_PRECISION_KEY,
                DFS_NAMENODE_ACCESSTIME_PRECISION_DEFAULT);
        this.supportAppends = conf.getBoolean(DFS_SUPPORT_APPEND_KEY, DFS_SUPPORT_APPEND_DEFAULT);

        this.dtpReplaceDatanodeOnFailure = ReplaceDatanodeOnFailure.get(conf);

        this.standbyShouldCheckpoint = conf.getBoolean(
                DFS_HA_STANDBY_CHECKPOINTS_KEY, DFS_HA_STANDBY_CHECKPOINTS_DEFAULT);
        this.inodeId = new FileINodeId();
        this.datanodeStatistics = blockManager.getDatanodeManager().getDatanodeStatistics();
        this.blockIdGenerator = new FileSequentialBlockIdGenerator(this.blockManager);
        alwaysUseDelegationTokensForTests = conf.getBoolean(
                DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY,
                DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_DEFAULT);
        this.fsOwner = UserGroupInformation.getCurrentUser();
        this.fsOwnerShortUserName = fsOwner.getShortUserName();
        this.dir = new FileSystemDirectory(fsImage, this, conf);
        this.snapshotManager = new FileSnapshotManager(dir);
        this.safeMode = new SafeModeInfo(conf);
        this.auditLoggers = initAuditLoggers(conf);
        this.isDefaultAuditLogger = auditLoggers.size() == 1 &&
                auditLoggers.get(0) instanceof DefaultAuditLogger;
        this.retryCache = null;
    }
    /**
     * Create delegation token secret manager
     */
    private DelegationTokenSecretManager createDelegationTokenSecretManager(
            Configuration conf) {
        return new DelegationTokenSecretManager(conf.getLong(
                DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_KEY,
                DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT),
                conf.getLong(DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_KEY,
                        DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT),
                conf.getLong(DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_KEY,
                        DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT),
                DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL,
                conf.getBoolean(DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_KEY,
                        DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_DEFAULT),
                this);
    }
    public FileSystenImage getFSImage() {
        return dir.fsImage;
    }

    public static FileSystenImage getFSImage(Configuration conf) throws IOException {
        FileSystenImage fsImage = new FileSystenImage(conf,
                getimageDirs(conf, DFS_NAMENODE_NAME_DIR_KEY),
                getNamespaceEditsDirs(conf, true));
        return fsImage;
    }

    /*
    获取image 目录
     */
    public static Collection<URI> getimageDirs(Configuration conf, String propertyName) {
        Collection<String> dirNames = conf.getTrimmedStringCollection(propertyName);
        return Util.stringCollectionAsURIs(dirNames);
    }

    /*
    获取 Edits 目录
     */
    public static List<URI> getNamespaceEditsDirs(Configuration conf,
                                                  boolean includeShared) {
        LinkedHashSet<URI> editsDirs = new LinkedHashSet<URI>();
        for (URI dir : getimageDirs(conf, DFS_NAMENODE_EDITS_DIR_KEY)) {
            editsDirs.add(dir);
        }
        return Lists.newArrayList(editsDirs);
    }

    public static   FileNamesystem getLoadDisk(Configuration conf) throws IOException {
        FileSystenImage fsImage = getFSImage(conf);
        FileNamesystem namesystem = new FileNamesystem(conf, fsImage, false);
        /*****************/
        HdfsServerConstants.StartupOption startOpt = NameNodeTest.getStartupOption(conf);
        long loadStart = now();
        String nameserviceId = DFSUtil.getNamenodeNameServiceId(conf);
        namesystem.loadFSImage(startOpt, fsImage,
                HAUtil.isHAEnabled(conf, nameserviceId));
        long timeTakenToLoadFSImage = now() - loadStart;
        LOG.info("Finished loading FSImage in " + timeTakenToLoadFSImage + " msecs");

        return namesystem;
    }

    void loadFSImage(HdfsServerConstants.StartupOption startOpt, FileSystenImage fsImage, boolean haEnabled) throws IOException {
        boolean success = false;
        try {


        this.fsLock.writeLock().lock();
        MetaRecoveryContext recovery = startOpt.createRecoveryContext();
        boolean needToSave = fsImage.recoverTransitionRead(startOpt, fsImage, false, this);
        if (needToSave)
            fsImage.saveNamespace(this,null);
        if (!haEnabled) {
            fsImage.openEditLogForWrite();
        }
            success = true;

        }finally {
            if (!success) {
                fsImage.close();
            }
            writeUnlock();
        }
        dir.imageLoadComplete();

    }

    PermissionStatus createFsOwnerPermissions(FsPermission permission) {
        return new PermissionStatus(fsOwner.getShortUserName(), supergroup, permission);
    }

  public   class SafeModeInfo {

        // configuration fields
        /**
         * Safe mode threshold condition %.
         */
        private double threshold;
        /**
         * Safe mode minimum number of datanodes alive
         */
        private int datanodeThreshold;
        /**
         * Safe mode extension after the threshold.
         */
        private int extension;
        /**
         * Min replication required by safe mode.
         */
        private int safeReplication;
        /**
         * threshold for populating needed replication queues
         */
        private double replQueueThreshold;
        // internal fields
        /**
         * Time when threshold was reached.
         * <br> -1 safe mode is off
         * <br> 0 safe mode is on, and threshold is not reached yet
         * <br> >0 safe mode is on, but we are in extension period
         */
        private long reached = -1;
        /**
         * Total number of blocks.
         */
        int blockTotal;
        /**
         * Number of safe blocks.
         */
        int blockSafe;
        /**
         * Number of blocks needed to satisfy safe mode threshold condition
         */
        private int blockThreshold;
        /**
         * Number of blocks needed before populating replication queues
         */
        private int blockReplQueueThreshold;
        /**
         * time of the last status printout
         */
        private long lastStatusReport = 0;
        /**
         * flag indicating whether replication queues have been initialized
         */
        boolean initializedReplQueues = false;
        /**
         * Was safemode entered automatically because available resources were low.
         */
        private boolean resourcesLow = false;
        /**
         * Should safemode adjust its block totals as blocks come in
         */
        public boolean shouldIncrementallyTrackBlocks = false;
        /**
         * counter for tracking startup progress of reported blocks
         */
        private StartupProgress.Counter awaitingReportedBlocksCounter;
      private synchronized void decrementSafeBlockCount(short replication) {
          if (replication == safeReplication-1) {
              this.blockSafe--;
              //blockSafe is set to -1 in manual / low resources safemode
              assert blockSafe >= 0 || isManual() || areResourcesLow();
              checkMode();
          }
      }

      private synchronized void incrementSafeBlockCount(short replication) {
          if (replication == safeReplication) {
              this.blockSafe++;

              // Report startup progress only if we haven't completed startup yet.
              StartupProgress prog = NameNode.getStartupProgress();
              if (prog.getStatus(Phase.SAFEMODE) != Status.COMPLETE) {
                  if (this.awaitingReportedBlocksCounter == null) {
                      this.awaitingReportedBlocksCounter = prog.getCounter(Phase.SAFEMODE,
                              STEP_AWAITING_REPORTED_BLOCKS);
                  }
                  this.awaitingReportedBlocksCounter.increment();
              }

              checkMode();
          }
      }
        private synchronized void setBlockTotal(int total) {
            this.blockTotal = total;
            this.blockThreshold = (int) (blockTotal * threshold);
            this.blockReplQueueThreshold =
                    (int) (blockTotal * replQueueThreshold);
            if (haEnabled) {
                // After we initialize the block count, any further namespace
                // modifications done while in safe mode need to keep track
                // of the number of total blocks in the system.
                this.shouldIncrementallyTrackBlocks = true;
            }
            if(blockSafe < 0)
                this.blockSafe = 0;
            checkMode();
        }
      private synchronized void adjustBlockTotals(int deltaSafe, int deltaTotal) {
          if (!shouldIncrementallyTrackBlocks) {
              return;
          }
          assert haEnabled;

          if (LOG.isDebugEnabled()) {
              LOG.debug("Adjusting block totals from " +
                      blockSafe + "/" + blockTotal + " to " +
                      (blockSafe + deltaSafe) + "/" + (blockTotal + deltaTotal));
          }
          assert blockSafe + deltaSafe >= 0 : "Can't reduce blockSafe " +
                  blockSafe + " by " + deltaSafe + ": would be negative";
          assert blockTotal + deltaTotal >= 0 : "Can't reduce blockTotal " +
                  blockTotal + " by " + deltaTotal + ": would be negative";

          blockSafe += deltaSafe;
          setBlockTotal(blockTotal + deltaTotal);
      }

        private synchronized boolean canLeave() {
            if (reached == 0)
                return false;
            if (now() - reached < extension) {
                reportStatus("STATE* Safe mode ON.", false);
                return false;
            }
            return !needEnter();
        }
        private void reportStatus(String msg, boolean rightNow) {
            long curTime = now();
            if(!rightNow && (curTime - lastStatusReport < 20 * 1000))
                return;
            //NameNodeTest.stateChangeLog.info(msg + " \n" + getTurnOffTip());
            lastStatusReport = curTime;
        }
      private boolean shouldIncrementallyTrackBlocks() {
          return shouldIncrementallyTrackBlocks;
      }
        /**
         * Check and trigger safe mode if needed.
         */
        private void checkMode() {
            // Have to have write-lock since leaving safemode initializes
            // repl queues, which requires write lock
            assert hasWriteLock();
            // if smmthread is already running, the block threshold must have been
            // reached before, there is no need to enter the safe mode again
            if (smmthread == null && needEnter()) {
                enter();
                // check if we are ready to initialize replication queues
              /*  if (canInitializeReplQueues() && !isPopulatingReplQueues()) {
                    initializeReplQueues();
                }
                reportStatus("STATE* Safe mode ON.", false);
                return;*/
            }
            // the threshold is reached or was reached before
            if (!isOn() ||                           // safe mode is off
                    extension <= 0 || threshold <= 0) {  // don't need to wait
                this.leave(); // leave safe mode
                return;
            }
            if (reached > 0) {  // threshold has already been reached before
                reportStatus("STATE* Safe mode ON.", false);
                return;
            }
            // start monitor
            reached = now();
            if (smmthread == null) {
                smmthread = new Daemon(new SafeModeMonitor());
                smmthread.start();
                reportStatus("STATE* Safe mode extension entered.", true);
            }

            // check if we are ready to initialize replication queues
            if (canInitializeReplQueues() && !isPopulatingReplQueues()) {
                initializeReplQueues();
            }
        }
        private boolean needEnter() {
            return (threshold != 0 && blockSafe < blockThreshold) ||
                    (getNumLiveDataNodes() < datanodeThreshold) ||
                    (!nameNodeHasResourcesAvailable());
        }
        boolean nameNodeHasResourcesAvailable() {
            return hasResourcesAvailable;
        }
        /**
         * Leave safe mode.
         * <p>
         * Check for invalid, under- & over-replicated blocks in the end of startup.
         */
        private synchronized void leave() {
            // if not done yet, initialize replication queues.
            // In the standby, do not populate repl queues
            if (!isPopulatingReplQueues() && shouldPopulateReplQueues()) {
                initializeReplQueues();
            }
            long timeInSafemode = now() - startTime;
            NameNodeTest.stateChangeLog.info("STATE* Leaving safe mode after "
                    + timeInSafemode/1000 + " secs");
          //  NameNodeTest.getNameNodeMetrics().setSafeModeTime((int) timeInSafemode);

            //Log the following only once (when transitioning from ON -> OFF)
            if (reached >= 0) {
                NameNode.stateChangeLog.info("STATE* Safe mode is OFF");
            }
            reached = -1;
            safeMode = null;
            final NetworkTopology nt = blockManager.getDatanodeManager().getNetworkTopology();


            startSecretManagerIfNecessary();

            // If startup has not yet completed, end safemode phase.
            StartupProgress prog = NameNode.getStartupProgress();
            if (prog.getStatus(Phase.SAFEMODE) != Status.COMPLETE) {
                prog.endStep(Phase.SAFEMODE, STEP_AWAITING_REPORTED_BLOCKS);
                prog.endPhase(Phase.SAFEMODE);
            }
        }
        private void startSecretManagerIfNecessary() {
            boolean shouldRun = shouldUseDelegationTokens() &&
                    !isInSafeMode() && getEditLog().isOpenForWrite();
            boolean running = dtSecretManager.isRunning();
            if (shouldRun && !running) {
              //  startSecretManager();
            }
        }
        private boolean shouldUseDelegationTokens() {
            return UserGroupInformation.isSecurityEnabled() ||
                    alwaysUseDelegationTokensForTests;
        }
        private synchronized void initializeReplQueues() {
            LOG.info("initializing replication queues");
            assert !isPopulatingReplQueues() : "Already initialized repl queues";
            long startTimeMisReplicatedScan = now();
            blockManager.processMisReplicatedBlocks();
            initializedReplQueues = true;
            NameNode.stateChangeLog.info("STATE* Replication Queue initialization "
                    + "scan for invalid, over- and under-replicated blocks "
                    + "completed in " + (now() - startTimeMisReplicatedScan)
                    + " msec");
        }
        /**
         * Creates SafeModeInfo when the name node enters
         * automatic safe mode at startup.
         *
         * @param conf configuration
         */

        private SafeModeInfo(Configuration conf) {
            this.threshold = conf.getFloat(DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY,
                    DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT);
            if (threshold > 1.0) {
            }
            this.datanodeThreshold = conf.getInt(
                    DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY,
                    DFS_NAMENODE_SAFEMODE_MIN_DATANODES_DEFAULT);
            this.extension = conf.getInt(DFS_NAMENODE_SAFEMODE_EXTENSION_KEY, 0);
            this.safeReplication = conf.getInt(DFS_NAMENODE_REPLICATION_MIN_KEY,
                    DFS_NAMENODE_REPLICATION_MIN_DEFAULT);


            // default to safe mode threshold (i.e., don't populate queues before leaving safe mode)
            this.replQueueThreshold =
                    conf.getFloat(DFS_NAMENODE_REPL_QUEUE_THRESHOLD_PCT_KEY,
                            (float) threshold);
            this.blockTotal = 0;
            this.blockSafe = 0;
        }
      private SafeModeInfo(boolean resourcesLow, boolean isReplQueuesInited) {
          this.threshold = 1.5f;  // this threshold can never be reached
          this.datanodeThreshold = Integer.MAX_VALUE;
          this.extension = Integer.MAX_VALUE;
          this.safeReplication = Short.MAX_VALUE + 1; // more than maxReplication
          this.replQueueThreshold = 1.5f; // can never be reached
          this.blockTotal = -1;
          this.blockSafe = -1;
          this.resourcesLow = resourcesLow;
          this.initializedReplQueues = isReplQueuesInited;
          enter();
          reportStatus("STATE* Safe mode is ON.", true);
      }
      private void setResourcesLow() {
          resourcesLow = true;
      }

        private synchronized boolean isOn() {
            return this.reached >= 0;
        }

        private synchronized boolean isPopulatingReplQueues() {
            return initializedReplQueues;
        }

        /**
         * Enter safe mode.
         */
        private void enter() {
            this.reached = 0;
        }


        private boolean isManual() {
            return extension == Integer.MAX_VALUE;
        }


        private synchronized void setManual() {
            extension = Integer.MAX_VALUE;
        }

        private boolean areResourcesLow() {
            return resourcesLow;
        }
          //rpc
        /**
         * A tip on how safe mode is to be turned off: manually or automatically.
         */
     public    String getTurnOffTip() {
            if(!isOn())
                return "Safe mode is OFF.";

            //Manual OR low-resource safemode. (Admin intervention required)
            String leaveMsg = "It was turned on manually. ";
            if (areResourcesLow()) {
                leaveMsg = "Resources are low on NN. Please add or free up more "
                        + "resources then turn off safe mode manually. NOTE:  If you turn off"
                        + " safe mode before adding resources, "
                        + "the NN will immediately return to safe mode. ";
            }
            if (isManual() || areResourcesLow()) {
                return leaveMsg
                        + "Use \"hdfs dfsadmin -safemode leave\" to turn safe mode off.";
            }

            //Automatic safemode. System will come out of safemode automatically.
            leaveMsg = "Safe mode will be turned off automatically";
            int numLive = getNumLiveDataNodes();
            String msg = "";
            if (reached == 0) {
                if (blockSafe < blockThreshold) {
                    msg += String.format(
                            "The reported blocks %d needs additional %d"
                                    + " blocks to reach the threshold %.4f of total blocks %d.\n",
                            blockSafe, (blockThreshold - blockSafe) + 1, threshold, blockTotal);
                }
                if (numLive < datanodeThreshold) {
                    msg += String.format(
                            "The number of live datanodes %d needs an additional %d live "
                                    + "datanodes to reach the minimum number %d.\n",
                            numLive, (datanodeThreshold - numLive), datanodeThreshold);
                }
            } else {
                msg = String.format("The reported blocks %d has reached the threshold"
                        + " %.4f of total blocks %d. ", blockSafe, threshold, blockTotal);

                msg += String.format("The number of live datanodes %d has reached "
                                + "the minimum number %d. ",
                        numLive, datanodeThreshold);
            }
            msg += leaveMsg;
            // threshold is not reached or manual or resources low
            if(reached == 0 || (isManual() && !areResourcesLow())) {
                return msg;
            }
            // extension period is in progress
            return msg + (reached + extension - now() > 0 ?
                    " in " + (reached + extension - now()) / 1000 + " seconds."
                    : " soon.");
        }

    }

    private List<AuditLogger> initAuditLoggers(Configuration conf) {
        // Initialize the custom access loggers if configured.
        Collection<String> alClasses = conf.getStringCollection(DFS_NAMENODE_AUDIT_LOGGERS_KEY);
        List<AuditLogger> auditLoggers = Lists.newArrayList();
        if (alClasses != null && !alClasses.isEmpty()) {
            for (String className : alClasses) {
                try {
                    AuditLogger logger;
                    if (DFS_NAMENODE_DEFAULT_AUDIT_LOGGER_NAME.equals(className)) {
                        logger = new DefaultAuditLogger();
                    } else {
                        logger = (AuditLogger) Class.forName(className).newInstance();
                    }
                    logger.initialize(conf);
                    auditLoggers.add(logger);
                } catch (RuntimeException re) {
                    throw re;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        // Make sure there is at least one logger installed.
        if (auditLoggers.isEmpty()) {
            auditLoggers.add(new DefaultAuditLogger());
        }
        return Collections.unmodifiableList(auditLoggers);
    }

    private boolean shouldPopulateReplQueues() {
        if (haContext == null || haContext.getState() == null)
            return false;
        return haContext.getState().shouldPopulateReplQueues();
    }

    private static class DefaultAuditLogger extends HdfsAuditLogger {

        private boolean logTokenTrackingId;

        @Override
        public void initialize(Configuration conf) {
            logTokenTrackingId = conf.getBoolean(
                    DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_KEY,
                    DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_DEFAULT);
        }

        @Override
        public void logAuditEvent(boolean succeeded, String userName,
                                  InetAddress addr, String cmd, String src, String dst,
                                  FileStatus status, UserGroupInformation ugi,
                                  DelegationTokenSecretManager dtSecretManager) {
            if (auditLog.isInfoEnabled()) {
                final StringBuilder sb = auditBuffer.get();
                sb.setLength(0);
                sb.append("allowed=").append(succeeded).append("\t");
                sb.append("ugi=").append(userName).append("\t");
                sb.append("ip=").append(addr).append("\t");
                sb.append("cmd=").append(cmd).append("\t");
                sb.append("src=").append(src).append("\t");
                sb.append("dst=").append(dst).append("\t");
                if (null == status) {
                    sb.append("perm=null");
                } else {
                    sb.append("perm=");
                    sb.append(status.getOwner()).append(":");
                    sb.append(status.getGroup()).append(":");
                    sb.append(status.getPermission());
                }
                if (logTokenTrackingId) {
                    sb.append("\t").append("trackingId=");
                    String trackingId = null;
                    if (ugi != null && dtSecretManager != null
                            && ugi.getAuthenticationMethod() == UserGroupInformation.AuthenticationMethod.TOKEN) {
                        for (TokenIdentifier tid : ugi.getTokenIdentifiers()) {
                            if (tid instanceof DelegationTokenIdentifier) {
                                DelegationTokenIdentifier dtid =
                                        (DelegationTokenIdentifier) tid;
                                trackingId = dtSecretManager.getTokenTrackingId(dtid);
                                break;
                            }
                        }
                    }
                    sb.append(trackingId);
                }
                auditLog.info(sb);
            }
        }

    }

    void clear() {
        dir.reset();
        generationStampV1.setCurrentValue(GenerationStamp.LAST_RESERVED_STAMP);
        generationStampV2.setCurrentValue(GenerationStamp.LAST_RESERVED_STAMP);
        blockIdGenerator.setCurrentValue(
                SequentialBlockIdGenerator.LAST_RESERVED_BLOCK_ID);
        generationStampV1Limit = GenerationStamp.GRANDFATHER_GENERATION_STAMP;
        leaseManager.removeAllLeases();
        inodeId.setCurrentValue(INodeId.LAST_RESERVED_ID);
    }

  public   void setGenerationStampV1(long stamp) {
        generationStampV1.setCurrentValue(stamp);
    }


  public   void setGenerationStampV1Limit(long stamp) {
        Preconditions.checkState(generationStampV1Limit ==
                GenerationStamp.GRANDFATHER_GENERATION_STAMP);
        generationStampV1Limit = stamp;
    }

  public   void setLastAllocatedBlockId(long blockId) {
        blockIdGenerator.skipTo(blockId);
    }

    long upgradeGenerationStampToV2() {
        Preconditions.checkState(generationStampV2.getCurrentValue() ==
                GenerationStamp.LAST_RESERVED_STAMP);

        generationStampV2.skipTo(
                generationStampV1.getCurrentValue() +
                        HdfsConstants.RESERVED_GENERATION_STAMPS_V1);

        generationStampV1Limit = generationStampV2.getCurrentValue();
        return generationStampV2.getCurrentValue();
    }

    public void resetLastInodeId(long newValue) throws IOException {
        try {
            inodeId.skipTo(newValue);
        } catch (IllegalStateException ise) {
            throw new IOException(ise);
        }
    }

    public FileSnapshotManager getSnapshotManager() {
        return snapshotManager;
    }
    public long allocateNewInodeId() {
        return inodeId.nextValue();
    }

    void setBlockPoolId(String bpid) {
        blockPoolId = bpid;
        blockManager.setBlockPoolId(blockPoolId);
    }

    @Override
    public void writeLock() {
        this.fsLock.writeLock().lock();
    }

    @Override
    public void writeLockInterruptibly() throws InterruptedException {
        this.fsLock.writeLock().lockInterruptibly();
    }

    @Override
    public void writeUnlock() {
        this.fsLock.writeLock().unlock();
    }

    @Override
    public int getTotalLoad() {
        return datanodeStatistics.getXceiverCount();
    }

    @Override
    public boolean isAvoidingStaleDataNodesForWrite() {
        return this.blockManager.getDatanodeManager()
                .shouldAvoidStaleDataNodesForWrite();
    }

    @Override
    public String getVersion() {
        return VersionInfo.getVersion() + ", r" + VersionInfo.getRevision();
    }

    @Override
    public String getSoftwareVersion() {
        return VersionInfo.getVersion();
    }

    @Override
    public long getUsed() {
        return this.getCapacityUsed();
    }

    @Override
    public long getFree() {
        return this.getCapacityRemaining();
    }

    @Override
    public long getTotal() {
        return this.getCapacityTotal();
    }

    @Override
    public String getSafemode() {
        if (!this.isInSafeMode())
            return "";
        return "Safe mode is ON. " + this.getSafeModeTip();
    }

    @Override
    public boolean isUpgradeFinalized() {
        return this.getFSImage().isUpgradeFinalized();
    }

    @Override
    public long getNonDfsUsedSpace() {
        return datanodeStatistics.getCapacityUsedNonDFS();
    }

    @Override
    public float getPercentUsed() {
        return datanodeStatistics.getCapacityUsedPercent();
    }

    @Override
    public float getPercentRemaining() {
        return datanodeStatistics.getCapacityRemainingPercent();
    }

    @Override
    public long getBlockPoolUsedSpace() {
        return datanodeStatistics.getBlockPoolUsed();
    }

    @Override
    public float getPercentBlockPoolUsed() {
        return datanodeStatistics.getPercentBlockPoolUsed();
    }

    @Override
    public long getTotalBlocks() {
        return getBlocksTotal();
    }

    @Override
    public long getTotalFiles() {
        return getFilesTotal();
    }

    @Override
    public long getNumberOfMissingBlocks() {
        return getMissingBlocksCount();
    }

    @Override
    public int getThreads() {
        return ManagementFactory.getThreadMXBean().getThreadCount();
    }
    private long getLastContact(DatanodeDescriptor alivenode) {
        return (Time.now() - alivenode.getLastUpdate())/1000;
    }
    private long getDfsUsed(DatanodeDescriptor alivenode) {
        return alivenode.getDfsUsed();
    }

    @Override
    public String getLiveNodes() {
        final Map<String, Map<String,Object>> info =
                new HashMap<String, Map<String,Object>>();
        final List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
        blockManager.getDatanodeManager().fetchDatanodes(live, null, true);
        for (DatanodeDescriptor node : live) {
            final Map<String, Object> innerinfo = new HashMap<String, Object>();
            innerinfo.put("lastContact", getLastContact(node));
            innerinfo.put("usedSpace", getDfsUsed(node));
            innerinfo.put("adminState", node.getAdminState().toString());
            innerinfo.put("nonDfsUsedSpace", node.getNonDfsUsed());
            innerinfo.put("capacity", node.getCapacity());
            innerinfo.put("numBlocks", node.numBlocks());
            innerinfo.put("version", node.getSoftwareVersion());
            info.put(node.getHostName(), innerinfo);
        }
        return JSON.toString(info);
    }

    @Override
    public String getDeadNodes() {
        final Map<String, Map<String, Object>> info =
                new HashMap<String, Map<String, Object>>();
        final List<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
        blockManager.getDatanodeManager().fetchDatanodes(null, dead, true);
        for (DatanodeDescriptor node : dead) {
            final Map<String, Object> innerinfo = new HashMap<String, Object>();
            innerinfo.put("lastContact", getLastContact(node));
            innerinfo.put("decommissioned", node.isDecommissioned());
            info.put(node.getHostName(), innerinfo);
        }
        return JSON.toString(info);
    }

    @Override
    public String getDecomNodes() {
        final Map<String, Map<String, Object>> info =
                new HashMap<String, Map<String, Object>>();
        final List<DatanodeDescriptor> decomNodeList = blockManager.getDatanodeManager(
        ).getDecommissioningNodes();
        for (DatanodeDescriptor node : decomNodeList) {
            final Map<String, Object> innerinfo = new HashMap<String, Object>();
            innerinfo.put("underReplicatedBlocks", node.decommissioningStatus
                    .getUnderReplicatedBlocks());
            innerinfo.put("decommissionOnlyReplicas", node.decommissioningStatus
                    .getDecommissionOnlyReplicas());
            innerinfo.put("underReplicateInOpenFiles", node.decommissioningStatus
                    .getUnderReplicatedInOpenFiles());
            info.put(node.getHostName(), innerinfo);
        }
        return JSON.toString(info);
    }

    @Override
    public String getClusterId() {
        return dir.fsImage.getStorage().getClusterID();
    }

    @Override
    public String getNameDirStatuses() {
        Map<String, Map<File, Storage.StorageDirType>> statusMap =
                new HashMap<String, Map<File, Storage.StorageDirType>>();

        Map<File, Storage.StorageDirType> activeDirs = new HashMap<File, Storage.StorageDirType>();
        for (Iterator<Storage.StorageDirectory> it
             = getFSImage().getStorage().dirIterator(); it.hasNext();) {
            Storage.StorageDirectory st = it.next();
            activeDirs.put(st.getRoot(), st.getStorageDirType());
        }
        statusMap.put("active", activeDirs);

        List<Storage.StorageDirectory> removedStorageDirs
                = getFSImage().getStorage().getRemovedStorageDirs();
        Map<File, Storage.StorageDirType> failedDirs = new HashMap<File, Storage.StorageDirType>();
        for (Storage.StorageDirectory st : removedStorageDirs) {
            failedDirs.put(st.getRoot(), st.getStorageDirType());
        }
        statusMap.put("failed", failedDirs);

        return JSON.toString(statusMap);
    }

    @Override
    public String getJournalTransactionInfo() {
        Map<String, String> txnIdMap = new HashMap<String, String>();
        txnIdMap.put("LastAppliedOrWrittenTxId",
                Long.toString(this.getFSImage().getLastAppliedOrWrittenTxId()));
        txnIdMap.put("MostRecentCheckpointTxId",
                Long.toString(this.getFSImage().getMostRecentCheckpointTxId()));
        return JSON.toString(txnIdMap);
    }

    @Override
    public int getDistinctVersionCount() {
        return blockManager.getDatanodeManager().getDatanodesSoftwareVersions()
                .size();
    }

    @Override
    public Map<String, Integer> getDistinctVersions() {
        return blockManager.getDatanodeManager().getDatanodesSoftwareVersions();

    }

    @Override
    public boolean isRunning() {
        return fsRunning;
    }

    @Override
    public void checkSuperuserPrivilege() throws AccessControlException {
        if (isPermissionEnabled) {
            FSPermissionChecker pc = getPermissionChecker();
            pc.checkSuperuserPrivilege();
        }
    }
    private FSPermissionChecker getPermissionChecker()
            throws AccessControlException {
        try {
            return new FSPermissionChecker(fsOwnerShortUserName, supergroup, getRemoteUser());
        } catch (IOException ioe) {
            throw new AccessControlException(ioe);
        }
    }
    private static UserGroupInformation getRemoteUser() throws IOException {
        return NameNodeTest.getRemoteUser();
    }

    public NamespaceInfo getNamespaceInfo() {
        readLock();
        try {
            return unprotectedGetNamespaceInfo();
        } finally {
            readUnlock();
        }
    }

    @Override
    public String getBlockPoolId() {
        return blockPoolId;
    }

    @Override
    public boolean isInStandbyState() {
        if (haContext == null || haContext.getState() == null) {
            // We're still starting up. In this case, if HA is
            // on for the cluster, we always start in standby. Otherwise
            // start in active.
            return haEnabled;
        }

        return HAServiceProtocol.HAServiceState.STANDBY == haContext.getState().getServiceState();
    }

    @Override
    public boolean isGenStampInFuture(Block block) {
        if (isLegacyBlock(block)) {
            return block.getGenerationStamp() > getGenerationStampV1();
        } else {
            return block.getGenerationStamp() > getGenerationStampV2();
        }
    }

    @Override
    public void adjustSafeModeBlockTotals(int deltaSafe, int deltaTotal) {
        SafeModeInfo safeMode = this.safeMode;
        if (safeMode == null)
            return;
        safeMode.adjustBlockTotals(deltaSafe, deltaTotal);
    }

    @Override
    public void checkOperation(NameNode.OperationCategory read) throws StandbyException {
        if (haContext != null) {
            // null in some unit tests
            haContext.checkOperation(read);
        }
    }

    @Override
    public void checkSafeMode() {
        SafeModeInfo safeMode = this.safeMode;
        if (safeMode != null) {
            safeMode.checkMode();
        }
    }

    @Override
    public boolean isInSafeMode() {
        SafeModeInfo safeMode = this.safeMode;
        if (safeMode == null)
            return false;
        return safeMode.isOn();
    }

    @Override
    public boolean isInStartupSafeMode() {
        SafeModeInfo safeMode = this.safeMode;
        if (safeMode == null)
            return false;
        // If the NN is in safemode, and not due to manual / low resources, we
        // assume it must be because of startup. If the NN had low resources during
        // startup, we assume it came out of startup safemode and it is now in low
        // resources safemode
        return !safeMode.isManual() && !safeMode.areResourcesLow()
                && safeMode.isOn();
    }

    @Override
    public boolean isPopulatingReplQueues() {
        if (!shouldPopulateReplQueues()) {
            return false;
        }
        // safeMode is volatile, and may be set to null at any time
        SafeModeInfo safeMode = this.safeMode;
        if (safeMode == null)
            return true;
        return safeMode.isPopulatingReplQueues();
    }

    @Override
    public void incrementSafeBlockCount(int replication) {
        SafeModeInfo safeMode = this.safeMode;
        if (safeMode == null)
            return;
        safeMode.incrementSafeBlockCount((short)replication);
    }

    @Override
    public void decrementSafeBlockCount(Block b) {
   SafeModeInfo safeMode = this.safeMode;
        if (safeMode == null) // mostly true
            return;
        BlockInfo storedBlock = getStoredBlock(b);
        if (storedBlock.isComplete()) {
            safeMode.decrementSafeBlockCount((short)blockManager.countNodes(b).liveReplicas());
        }
    }

    @Override
    public String getFSState() {
        return isInSafeMode() ? "safeMode" : "Operational";
    }

    @Override
    public long getBlocksTotal() {
        return getBlocksTotal();
    }

    @Override
    public long getCapacityTotal() {
        return datanodeStatistics.getCapacityTotal();
    }

    @Override
    public long getCapacityRemaining() {
        return datanodeStatistics.getCapacityRemaining();
    }

    @Override
    public long getCapacityUsed() {
        return datanodeStatistics.getCapacityUsed();
    }

    @Override
    public long getFilesTotal() {
        readLock();
        try {
            return this.dir.totalInodes();
        } finally {
            readUnlock();
        }
    }

    @Override
    public long getPendingReplicationBlocks() {
        return blockManager.getPendingReplicationBlocksCount();
    }

    @Override
    public long getUnderReplicatedBlocks() {
        return blockManager.getUnderReplicatedBlocksCount();
    }

    @Override
    public long getScheduledReplicationBlocks() {
        return blockManager.getScheduledReplicationBlocksCount();
    }

    @Override
    public int getNumLiveDataNodes() {
        return getBlockManager().getDatanodeManager().getNumLiveDataNodes();
    }

    @Override
    public int getNumDeadDataNodes() {
        return getBlockManager().getDatanodeManager().getNumDeadDataNodes();
    }

    @Override
    public int getNumStaleDataNodes() {
        return getBlockManager().getDatanodeManager().getNumStaleNodes();
    }

    @Override
    public void readLock() {
        this.fsLock.readLock().lock();

    }

    @Override
    public void readUnlock() {
        this.fsLock.readLock().unlock();
    }

    @Override
    public boolean hasReadLock() {
        return this.fsLock.getReadHoldCount() > 0;
    }

    @Override
    public boolean hasWriteLock() {
        return this.fsLock.isWriteLockedByCurrentThread();
    }

    @Override
    public boolean hasReadOrWriteLock() {
        return hasReadLock() || hasWriteLock();
    }

    public long getLastInodeId() {
        return inodeId.getCurrentValue();
    }

    boolean hasRetryCache() {
        return retryCache != null;
    }

    public FileBlockManager getBlockManager() {
        return blockManager;
    }

    void addCacheEntryWithPayload(byte[] clientId, int callId, Object payload) {
        if (retryCache != null) {
            retryCache.addCacheEntryWithPayload(clientId, callId, payload);
        }
    }

    LocatedBlock prepareFileForWrite(String src, INodeFile file,
                                     String leaseHolder, String clientMachine, DatanodeDescriptor clientNode,
                                     boolean writeToEditLog, Snapshot latestSnapshot, boolean logRetryCache)
            throws IOException {
        file = file.recordFileModification(latestSnapshot, dir.getINodeMap());
        final INodeFileUnderConstruction cons = file.toUnderConstruction(
                leaseHolder, clientMachine, clientNode);

        dir.replaceINodeFile(src, file, cons);
        leaseManager.addLease(cons.getClientName(), src);

        LocatedBlock ret = blockManager.convertLastBlockToUnderConstruction(cons);
        if (writeToEditLog) {
            getEditLog().logOpenFile(src, cons, logRetryCache);
        }
        return ret;
    }

    public FileSystemEditLog getEditLog() {
        return getFSImage().getEditLog();
    }

    public void addCacheEntry(byte[] clientId, int callId) {
        if (retryCache != null) {
            retryCache.addCacheEntry(clientId, callId);
        }
    }
 public    DelegationTokenSecretManager getDelegationTokenSecretManager() {
        return dtSecretManager;
    }
 public    void startCommonServices(Configuration conf, HAContext haContext) throws IOException {
     //   this.registerMBean(); // register the MBean for the FSNamesystemState
        writeLock();
        this.haContext = haContext;
        try {
            nnResourceChecker = new FileNameNodeResourceChecker(conf);
            checkAvailableResources();
            assert safeMode != null &&
                    !safeMode.isPopulatingReplQueues();
            StartupProgress prog = NameNode.getStartupProgress();
            prog.beginPhase(Phase.SAFEMODE);
            prog.setTotal(Phase.SAFEMODE, STEP_AWAITING_REPORTED_BLOCKS,
                    getCompleteBlocksTotal());
            setBlockTotal();
            blockManager.activate(conf);
        } finally {
            writeUnlock();
        }

     //   registerMXBean();
//        DefaultMetricsSystem.instance().register(this);
    }
    /**
     * Perform resource checks and cache the results.
     * @throws IOException
     */
  public   void checkAvailableResources() {
        Preconditions.checkState(nnResourceChecker != null,
                "nnResourceChecker not initialized");
        hasResourcesAvailable = nnResourceChecker.hasAvailableDiskSpace();
    }
    public void setBlockTotal() {
        // safeMode is volatile, and may be set to null at any time
        SafeModeInfo safeMode = this.safeMode;
        if (safeMode == null)
            return;
        safeMode.setBlockTotal((int)getCompleteBlocksTotal());
    }


    private long getCompleteBlocksTotal() {
        // Calculate number of blocks under construction
        long numUCBlocks = 0;
        readLock();
        try {
            for (FileLeaseManager.Lease lease : leaseManager.getSortedLeases()) {
                for (String path : lease.getPaths()) {
                    final INodeFileUnderConstruction cons;
                    try {
                        cons = INodeFileUnderConstruction.valueOf(dir.getINode(path), path);
                    } catch (UnresolvedLinkException e) {
                        throw new AssertionError("Lease files should reside on this FS");
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    BlockInfo[] blocks = cons.getBlocks();
                    if(blocks == null)
                        continue;
                    for(BlockInfo b : blocks) {
                        if(!b.isComplete())
                            numUCBlocks++;
                    }
                }
            }
            LOG.info("Number of blocks under construction: " + numUCBlocks);
            return getBlocksTotal() - numUCBlocks;
        } finally {
            readUnlock();
        }
    }
    /**
     * Periodically check whether it is time to leave safe mode.
     * This thread starts when the threshold level is reached.
     *
     */
    public class SafeModeMonitor implements Runnable {
        /** interval in msec for checking safe mode: {@value} */
        private static final long recheckInterval = 1000;

        /**
         */
        @Override
        public void run() {
            while (fsRunning) {
                writeLock();
                try {
                    if (safeMode == null) { // Not in safe mode.
                        break;
                    }
                    if (safeMode.canLeave()) {
                        // Leave safe mode.
                        safeMode.leave();
                        smmthread = null;
                        break;
                    }
                } finally {
                    writeUnlock();
                }

                try {
                    Thread.sleep(recheckInterval);
                } catch (InterruptedException ie) {
                    // Ignored
                }
            }
            if (!fsRunning) {
                LOG.info("NameNode is being shutdown, exit SafeModeMonitor thread");
            }
        }
    }

    /**
     * Check whether we have reached the threshold for
     * initializing replication queues.
     */
    private synchronized boolean canInitializeReplQueues() {
        return shouldPopulateReplQueues()
                && blockSafe >= blockReplQueueThreshold;
    }

    public FileSystemDirectory getFSDirectory() {
        return dir;
    }
  public   void loadSecretManagerState(DataInput in) throws IOException {
        dtSecretManager.loadSecretManagerState(in);
    }
  public   NamespaceInfo unprotectedGetNamespaceInfo() {
        return new NamespaceInfo(dir.fsImage.getStorage().getNamespaceID(),
                getClusterId(), getBlockPoolId(),
                dir.fsImage.getStorage().getCTime());
    }
 public    long getGenerationStampV1() {
        return generationStampV1.getCurrentValue();
    }
  public   void setGenerationStampV2(long stamp) {
        generationStampV2.setCurrentValue(stamp);
    }

   public long getGenerationStampV2() {
        return generationStampV2.getCurrentValue();
    }
    public  long getGenerationStampAtblockIdSwitch() {
        return generationStampV1Limit;
    }
    public long getLastAllocatedBlockId() {
        return blockIdGenerator.getCurrentValue();
    }
  public   void saveFilesUnderConstruction(DataOutputStream out) throws IOException {
        // This is run by an inferior thread of saveNamespace, which holds a read
        // lock on our behalf. If we took the read lock here, we could block
        // for fairness if a writer is waiting on the lock.
        synchronized (leaseManager) {
            Map<String, INodeFileUnderConstruction> nodes =
                    leaseManager.getINodesUnderConstruction();
            out.writeInt(nodes.size()); // write the size
            for (Map.Entry<String, INodeFileUnderConstruction> entry
                    : nodes.entrySet()) {
                FSImageSerialization.writeINodeUnderConstruction(
                        out, entry.getValue(), entry.getKey());
            }
        }
    }
   public void saveSecretManagerState(DataOutputStream out, String sdPath)
            throws IOException {
        dtSecretManager.saveSecretManagerState(out, sdPath);
    }

    //rpc
  public   boolean nameNodeHasResourcesAvailable() {
        return hasResourcesAvailable;
    }

  public   String getSafeModeTip() {
        readLock();
        try {
            if (!isInSafeMode()) {
                return "";
            }
            return safeMode.getTurnOffTip();
        } finally {
            readUnlock();
        }
    }

    /**
     * Get block locations within the specified range.
     * @see ClientProtocol#getBlockLocations(String, long, long)
     */
  public   LocatedBlocks getBlockLocations(String clientMachine, String src,
                                    long offset, long length) throws AccessControlException,
            FileNotFoundException, UnresolvedLinkException, IOException {
        LocatedBlocks blocks = getBlockLocations(src, offset, length, true, true,
                true);
        if (blocks != null) {
            blockManager.getDatanodeManager().sortLocatedBlocks(
                    clientMachine, blocks.getLocatedBlocks());

            LocatedBlock lastBlock = blocks.getLastLocatedBlock();
            if (lastBlock != null) {
                ArrayList<LocatedBlock> lastBlockList = new ArrayList<LocatedBlock>();
                lastBlockList.add(lastBlock);
                blockManager.getDatanodeManager().sortLocatedBlocks(
                        clientMachine, lastBlockList);
            }
        }
        return blocks;
    }
    /**
     * Get block locations within the specified range.
     * @see ClientProtocol#getBlockLocations(String, long, long)
     * @throws FileNotFoundException, UnresolvedLinkException, IOException
     */
    LocatedBlocks getBlockLocations(String src, long offset, long length,
                                    boolean doAccessTime, boolean needBlockToken, boolean checkSafeMode)
            throws FileNotFoundException, UnresolvedLinkException, IOException {
        try {
            return getBlockLocationsInt(src, offset, length, doAccessTime,
                    needBlockToken, checkSafeMode);
        } catch (AccessControlException e) {
            logAuditEvent(false, "open", src);
            throw e;
        }
    }

    private LocatedBlocks getBlockLocationsInt(String src, long offset,
                                               long length, boolean doAccessTime, boolean needBlockToken,
                                               boolean checkSafeMode)
            throws FileNotFoundException, UnresolvedLinkException, IOException {
        if (offset < 0) {
            throw new HadoopIllegalArgumentException(
                    "Negative offset is not supported. File: " + src);
        }
        if (length < 0) {
            throw new HadoopIllegalArgumentException(
                    "Negative length is not supported. File: " + src);
        }
        final LocatedBlocks ret = getBlockLocationsUpdateTimes(src,
                offset, length, doAccessTime, needBlockToken);
        logAuditEvent(true, "open", src);
        if (checkSafeMode && isInSafeMode()) {
            for (LocatedBlock b : ret.getLocatedBlocks()) {
                // if safemode & no block locations yet then throw safemodeException
                if ((b.getLocations() == null) || (b.getLocations().length == 0)) {
               /*     throw new SafeModeException("Zero blocklocations for " + src,
                            safeMode);*/
                }
            }
        }
        return ret;
    }
    private void logAuditEvent(boolean succeeded, String cmd, String src)
            throws IOException {
        logAuditEvent(succeeded, cmd, src, null, null);
    }
    private void logAuditEvent(boolean succeeded, String cmd, String src,
                               String dst, HdfsFileStatus stat) throws IOException {
        if (isAuditEnabled() && isExternalInvocation()) {
            logAuditEvent(succeeded, getRemoteUser(), getRemoteIp(),
                    cmd, src, dst, stat);
        }
    }

    private void logAuditEvent(boolean succeeded,
                               UserGroupInformation ugi, InetAddress addr, String cmd, String src,
                               String dst, HdfsFileStatus stat) {
        FileStatus status = null;
        if (stat != null) {
            Path symlink = stat.isSymlink() ? new Path(stat.getSymlink()) : null;
            Path path = dst != null ? new Path(dst) : new Path(src);
            status = new FileStatus(stat.getLen(), stat.isDir(),
                    stat.getReplication(), stat.getBlockSize(), stat.getModificationTime(),
                    stat.getAccessTime(), stat.getPermission(), stat.getOwner(),
                    stat.getGroup(), symlink, path);
        }
        for (AuditLogger logger : auditLoggers) {
            if (logger instanceof HdfsAuditLogger) {
                HdfsAuditLogger hdfsLogger = (HdfsAuditLogger) logger;
                hdfsLogger.logAuditEvent(succeeded, ugi.toString(), addr, cmd, src, dst,
                        status, ugi, dtSecretManager);
            } else {
                logger.logAuditEvent(succeeded, ugi.toString(), addr,
                        cmd, src, dst, status);
            }
        }
    }
    private static InetAddress getRemoteIp() {
        InetAddress ip = Server.getRemoteIp();
        if (ip != null) {
            return ip;
        }
        return NamenodeWebHdfsMethods.getRemoteIp();
    }
    /**
     * Client invoked methods are invoked over RPC and will be in
     * RPC call context even if the client exits.
     */
    private boolean isExternalInvocation() {
        return Server.isRpcInvocation() || NamenodeWebHdfsMethods.isWebHdfsInvocation();
    }
    public boolean isAuditEnabled() {
        return !isDefaultAuditLogger || auditLog.isInfoEnabled();
    }
    /*
     * Get block locations within the specified range, updating the
     * access times if necessary.
     */
    private LocatedBlocks getBlockLocationsUpdateTimes(String src, long offset,
                                                       long length, boolean doAccessTime, boolean needBlockToken)
            throws FileNotFoundException,
            UnresolvedLinkException, IOException {
        FSPermissionChecker pc = getPermissionChecker();
        byte[][] pathComponents = FileSystemDirectory.getPathComponentsForReservedPath(src);
        for (int attempt = 0; attempt < 2; attempt++) {
            boolean isReadOp = (attempt == 0);
            if (isReadOp) { // first attempt is with readlock
                checkOperation(NameNode.OperationCategory.READ);
                readLock();
            }  else { // second attempt is with  write lock
                checkOperation(NameNode.OperationCategory.WRITE);
                writeLock(); // writelock is needed to set accesstime
            }
            src = FileSystemDirectory.resolvePath(src, pathComponents, dir);
            try {
                if (isReadOp) {
                    checkOperation(NameNode.OperationCategory.READ);
                } else {
                    checkOperation(NameNode.OperationCategory.WRITE);
                }
                if (isPermissionEnabled) {
                    checkPathAccess(pc, src, FsAction.READ);
                }

                // if the namenode is in safemode, then do not update access time
                if (isInSafeMode()) {
                    doAccessTime = false;
                }

                final FileINodesInPath iip = dir.getLastINodeInPath(src);
                final INodeFile inode = INodeFile.valueOf(iip.getLastINode(), src);
                if (!iip.isSnapshot() //snapshots are readonly, so don't update atime.
                        && doAccessTime && isAccessTimeSupported()) {
                    final long now = now();
                    if (now > inode.getAccessTime() + getAccessTimePrecision()) {
                        // if we have to set access time but we only have the readlock, then
                        // restart this entire operation with the writeLock.
                        if (isReadOp) {
                            continue;
                        }
                        dir.setTimes(src, inode, -1, now, false, iip.getLatestSnapshot());
                    }
                }
                final long fileSize = iip.isSnapshot() ?
                        inode.computeFileSize(iip.getPathSnapshot())
                        : inode.computeFileSizeNotIncludingLastUcBlock();
                boolean isUc = inode.isUnderConstruction();
                if (iip.isSnapshot()) {
                    // if src indicates a snapshot file, we need to make sure the returned
                    // blocks do not exceed the size of the snapshot file.
                    length = Math.min(length, fileSize - offset);
                    isUc = false;
                }
                return blockManager.createLocatedBlocks(inode.getBlocks(), fileSize,
                        isUc, offset, length, needBlockToken, iip.isSnapshot());
            } finally {
                if (isReadOp) {
                    readUnlock();
                } else {
                    writeUnlock();
                }
            }
        }
        return null; // can never reach here
    }
    private boolean isAccessTimeSupported() {
        return accessTimePrecision > 0;
    }

    private void checkPathAccess(FSPermissionChecker pc,
                                 String path, FsAction access) throws AccessControlException,
            UnresolvedLinkException {
        checkPermission(pc, path, false, null, null, access, null);
    }
    private void checkPermission(FSPermissionChecker pc,
                                 String path, boolean doCheckOwner, FsAction ancestorAccess,
                                 FsAction parentAccess, FsAction access, FsAction subAccess)
            throws AccessControlException, UnresolvedLinkException {
        checkPermission(pc, path, doCheckOwner, ancestorAccess,
                parentAccess, access, subAccess, true);
    }
    private void checkPermission(FSPermissionChecker pc,
                                 String path, boolean doCheckOwner, FsAction ancestorAccess,
                                 FsAction parentAccess, FsAction access, FsAction subAccess,
                                 boolean resolveLink)
            throws AccessControlException, UnresolvedLinkException {
        if (!pc.isSuperUser()) {
            dir.waitForReady();
            readLock();
            try {
                pc.checkPermission(path, dir.rootDir, doCheckOwner, ancestorAccess,
                        parentAccess, access, subAccess, resolveLink);
            } finally {
                readUnlock();
            }
        }
    }
    long getAccessTimePrecision() {
        return accessTimePrecision;
    }

   public FsServerDefaults getServerDefaults() throws StandbyException {
        checkOperation(NameNode.OperationCategory.READ);
        return serverDefaults;
    }
    /**
     * Create a new file entry in the namespace.
     *
     * For description of parameters and exceptions thrown see
     * {@link ClientProtocol()}, except it returns valid file status upon
     * success
     *
     * For retryCache handling details see -
     * {@link boolean, RetryCache.CacheEntryWithPayload)}
     *
     */
  public   HdfsFileStatus startFile(String src, PermissionStatus permissions,
                             String holder, String clientMachine, EnumSet<CreateFlag> flag,
                             boolean createParent, short replication, long blockSize)
            throws AccessControlException, SafeModeException,
            FileAlreadyExistsException, UnresolvedLinkException,
            FileNotFoundException, ParentNotDirectoryException, IOException {
        HdfsFileStatus status = null;
        RetryCache.CacheEntryWithPayload cacheEntry = RetryCache.waitForCompletion(retryCache,
                null);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return (HdfsFileStatus) cacheEntry.getPayload();
        }

        try {
            status = startFileInt(src, permissions, holder, clientMachine, flag,
                    createParent, replication, blockSize, cacheEntry != null);
        } catch (AccessControlException e) {
            logAuditEvent(false, "create", src);
            throw e;
        } finally {
            RetryCache.setState(cacheEntry, status != null, status);
        }
        return status;
    }

    private HdfsFileStatus startFileInt(String src, PermissionStatus permissions,
                                        String holder, String clientMachine, EnumSet<CreateFlag> flag,
                                        boolean createParent, short replication, long blockSize,
                                        boolean logRetryCache) throws AccessControlException, SafeModeException,
            FileAlreadyExistsException, UnresolvedLinkException,
            FileNotFoundException, ParentNotDirectoryException, IOException {
        if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* NameSystem.startFile: src=" + src
                    + ", holder=" + holder
                    + ", clientMachine=" + clientMachine
                    + ", createParent=" + createParent
                    + ", replication=" + replication
                    + ", createFlag=" + flag.toString());
        }
        if (!DFSUtil.isValidName(src)) {
            throw new InvalidPathException(src);
        }
        blockManager.verifyReplication(src, replication, clientMachine);

        boolean skipSync = false;
        HdfsFileStatus stat = null;
        FSPermissionChecker pc = getPermissionChecker();
        checkOperation(NameNode.OperationCategory.WRITE);
        if (blockSize < minBlockSize) {
            throw new IOException("Specified block size is less than configured" +
                    " minimum value (" + DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY
                    + "): " + blockSize + " < " + minBlockSize);
        }
        byte[][] pathComponents = FileSystemDirectory.getPathComponentsForReservedPath(src);
        boolean create = flag.contains(CreateFlag.CREATE);
        boolean overwrite = flag.contains(CreateFlag.OVERWRITE);
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.WRITE);
            if (isInSafeMode()) {
                //throw new SafeModeException("Cannot create file" + src, safeMode);
            }
            src = FileSystemDirectory.resolvePath(src, pathComponents, dir);
            startFileInternal(pc, src, permissions, holder, clientMachine, create,
                    overwrite, createParent, replication, blockSize, logRetryCache);
            stat = dir.getFileInfo(src, false);
        } catch (StandbyException se) {
            skipSync = true;
            throw se;
        } finally {
            writeUnlock();
            // There might be transactions logged while trying to recover the lease.
            // They need to be sync'ed even when an exception was thrown.
            if (!skipSync) {
                getEditLog().logSync();
            }
        }
        logAuditEvent(true, "create", src, null, stat);
        return stat;
    }

    /**
     * Create a new file or overwrite an existing file<br>
     *
     * Once the file is create the client then allocates a new block with the next
     * call using {@link NameNode()}.
     * <p>
     * For description of parameters and exceptions thrown see
     * {@link ClientProtocol()}
     */
    private void startFileInternal(FSPermissionChecker pc, String src,
                                   PermissionStatus permissions, String holder, String clientMachine,
                                   boolean create, boolean overwrite, boolean createParent,
                                   short replication, long blockSize, boolean logRetryEntry)
            throws FileAlreadyExistsException, AccessControlException,
            UnresolvedLinkException, FileNotFoundException,
            ParentNotDirectoryException, IOException {
        assert hasWriteLock();
        // Verify that the destination does not exist as a directory already.
        final FileINodesInPath iip = dir.getINodesInPath4Write(src);
        final INode inode = iip.getLastINode();
        if (inode != null && inode.isDirectory()) {
            throw new FileAlreadyExistsException("Cannot create file " + src
                    + "; already exists as a directory.");
        }
        final INodeFile myFile = INodeFile.valueOf(inode, src, true);
        if (isPermissionEnabled) {
            if (overwrite && myFile != null) {
                checkPathAccess(pc, src, FsAction.WRITE);
            } else {
                checkAncestorAccess(pc, src, FsAction.WRITE);
            }
        }

        if (!createParent) {
            verifyParentDir(src);
        }

        try {
            if (myFile == null) {
                if (!create) {
                    throw new FileNotFoundException("failed to overwrite non-existent file "
                            + src + " on client " + clientMachine);
                }
            } else {
                if (overwrite) {
                    try {
                        deleteInt(src, true, false); // File exists - delete if overwrite
                    } catch (AccessControlException e) {
                        logAuditEvent(false, "delete", src);
                        throw e;
                    }
                } else {
                    // If lease soft limit time is expired, recover the lease
                    recoverLeaseInternal(myFile, src, holder, clientMachine, false);
                    throw new FileAlreadyExistsException("failed to create file " + src
                            + " on client " + clientMachine + " because the file exists");
                }
            }

            checkFsObjectLimit();
            final DatanodeDescriptor clientNode =
                    blockManager.getDatanodeManager().getDatanodeByHost(clientMachine);

            INodeFileUnderConstruction newNode = dir.addFile(src, permissions,
                    replication, blockSize, holder, clientMachine, clientNode);
            if (newNode == null) {
                throw new IOException("DIR* NameSystem.startFile: " +
                        "Unable to add file to namespace.");
            }
            leaseManager.addLease(newNode.getClientName(), src);

            // record file record in log, record new generation stamp
            getEditLog().logOpenFile(src, newNode, logRetryEntry);
            if (NameNode.stateChangeLog.isDebugEnabled()) {
                NameNode.stateChangeLog.debug("DIR* NameSystem.startFile: "
                        +"add "+src+" to namespace for "+holder);
            }
        } catch (IOException ie) {
            NameNode.stateChangeLog.warn("DIR* NameSystem.startFile: "
                    +ie.getMessage());
            throw ie;
        }
    }

    private void checkAncestorAccess(FSPermissionChecker pc,
                                     String path, FsAction access) throws AccessControlException,
            UnresolvedLinkException {
        checkPermission(pc, path, false, access, null, null, null);
    }

    /**
     * Verify that parent directory of src exists.
     */
    private void verifyParentDir(String src) throws FileNotFoundException,
            ParentNotDirectoryException, UnresolvedLinkException {
        assert hasReadOrWriteLock();
        Path parent = new Path(src).getParent();
        if (parent != null) {
            final INode parentNode = dir.getINode(parent.toString());
            if (parentNode == null) {
                throw new FileNotFoundException("Parent directory doesn't exist: "
                        + parent);
            } else if (!parentNode.isDirectory() && !parentNode.isSymlink()) {
                throw new ParentNotDirectoryException("Parent path is not a directory: "
                        + parent);
            }
        }
    }

    private boolean deleteInt(String src, boolean recursive, boolean logRetryCache)
            throws AccessControlException, SafeModeException,
            UnresolvedLinkException, IOException {
        if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* NameSystem.delete: " + src);
        }
        boolean status = deleteInternal(src, recursive, true, logRetryCache);
        if (status) {
            logAuditEvent(true, "delete", src);
        }
        return status;
    }

    private void recoverLeaseInternal(INodeFile fileInode,
                                      String src, String holder, String clientMachine, boolean force)
            throws IOException {
        assert hasWriteLock();
        if (fileInode != null && fileInode.isUnderConstruction()) {
            INodeFileUnderConstruction pendingFile = (INodeFileUnderConstruction) fileInode;
            //
            // If the file is under construction , then it must be in our
            // leases. Find the appropriate lease record.
            //
            FileLeaseManager.Lease lease = leaseManager.getLease(holder);
            //
            // We found the lease for this file. And surprisingly the original
            // holder is trying to recreate this file. This should never occur.
            //
            if (!force && lease != null) {
                FileLeaseManager.Lease leaseFile = leaseManager.getLeaseByPath(src);
                if ((leaseFile != null && leaseFile.equals(lease)) ||
                        lease.getHolder().equals(holder)) {
                    throw new AlreadyBeingCreatedException(
                            "failed to create file " + src + " for " + holder +
                                    " on client " + clientMachine +
                                    " because current leaseholder is trying to recreate file.");
                }
            }
            //
            // Find the original holder.
            //
            lease = leaseManager.getLease(pendingFile.getClientName());
            if (lease == null) {
                throw new AlreadyBeingCreatedException(
                        "failed to create file " + src + " for " + holder +
                                " on client " + clientMachine +
                                " because pendingCreates is non-null but no leases found.");
            }
            if (force) {
                // close now: no need to wait for soft lease expiration and
                // close only the file src
                LOG.info("recoverLease: " + lease + ", src=" + src +
                        " from client " + pendingFile.getClientName());
                internalReleaseLease(lease, src, holder);
            } else {
                assert lease.getHolder().equals(pendingFile.getClientName()) :
                        "Current lease holder " + lease.getHolder() +
                                " does not match file creator " + pendingFile.getClientName();
                //
                // If the original holder has not renewed in the last SOFTLIMIT
                // period, then start lease recovery.
                //
                if (lease.expiredSoftLimit()) {
                    LOG.info("startFile: recover " + lease + ", src=" + src + " client "
                            + pendingFile.getClientName());
                    boolean isClosed = internalReleaseLease(lease, src, null);
                    if(!isClosed)
                        throw new RecoveryInProgressException(
                                "Failed to close file " + src +
                                        ". Lease recovery is in progress. Try again later.");
                } else {
                    final BlockInfo lastBlock = pendingFile.getLastBlock();
                    if (lastBlock != null
                            && lastBlock.getBlockUCState() == HdfsServerConstants.BlockUCState.UNDER_RECOVERY) {
                        throw new RecoveryInProgressException("Recovery in progress, file ["
                                + src + "], " + "lease owner [" + lease.getHolder() + "]");
                    } else {
                        throw new AlreadyBeingCreatedException("Failed to create file ["
                                + src + "] for [" + holder + "] on client [" + clientMachine
                                + "], because this file is already being created by ["
                                + pendingFile.getClientName() + "] on ["
                                + pendingFile.getClientMachine() + "]");
                    }
                }
            }
        }
    }
    /**
     * Check to see if we have exceeded the limit on the number
     * of inodes.
     */
    void checkFsObjectLimit() throws IOException {
        if (maxFsObjects != 0 &&
                maxFsObjects <= dir.totalInodes() + getBlocksTotal()) {
            throw new IOException("Exceeded the configured number of objects " +
                    maxFsObjects + " in the filesystem.");
        }
    }

    private boolean deleteInternal(String src, boolean recursive,
                                   boolean enforcePermission, boolean logRetryCache)
            throws AccessControlException, SafeModeException, UnresolvedLinkException,
            IOException {
        INode.BlocksMapUpdateInfo collectedBlocks = new INode.BlocksMapUpdateInfo();
        List<INode> removedINodes = new ArrayList<INode>();
        FSPermissionChecker pc = getPermissionChecker();
        checkOperation(NameNode.OperationCategory.WRITE);
        byte[][] pathComponents = FileSystemDirectory.getPathComponentsForReservedPath(src);
        boolean ret = false;
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.WRITE);
            if (isInSafeMode()) {
               // throw new SafeModeException("Cannot delete " + src, safeMode);
            }
            src = FileSystemDirectory.resolvePath(src, pathComponents, dir);
            if (!recursive && dir.isNonEmptyDirectory(src)) {
                throw new IOException(src + " is non empty");
            }
            if (enforcePermission && isPermissionEnabled) {
                checkPermission(pc, src, false, null, FsAction.WRITE, null,
                        FsAction.ALL, false);
            }
            // Unlink the target directory from directory tree
            if (!dir.delete(src, collectedBlocks, removedINodes, logRetryCache)) {
                return false;
            }
            ret = true;
        } finally {
            writeUnlock();
        }
        getEditLog().logSync();
        removeBlocks(collectedBlocks); // Incremental deletion of blocks
        collectedBlocks.clear();
        dir.writeLock();
        try {
            dir.removeFromInodeMap(removedINodes);
        } finally {
            dir.writeUnlock();
        }
        removedINodes.clear();
        if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* Namesystem.delete: "
                    + src +" is removed");
        }
        return ret;
    }

    /**
     * Move a file that is being written to be immutable.
     * @param src The filename
     * @param lease The lease for the client creating the file
     * @param recoveryLeaseHolder reassign lease to this holder if the last block
     *        needs recovery; keep current holder if null.
     * @throws AlreadyBeingCreatedException if file is waiting to achieve minimal
     *         replication;<br>
     *         RecoveryInProgressException if lease recovery is in progress.<br>
     *         IOException in case of an error.
     * @return true  if file has been successfully finalized and closed or
     *         false if block recovery has been initiated. Since the lease owner
     *         has been changed and logged, caller should call logSync().
     */
    boolean internalReleaseLease(FileLeaseManager.Lease lease, String src,
                                 String recoveryLeaseHolder) throws AlreadyBeingCreatedException,
            IOException, UnresolvedLinkException {
        LOG.info("Recovering " + lease + ", src=" + src);
        assert !isInSafeMode();
        assert hasWriteLock();

        final FileINodesInPath iip = dir.getLastINodeInPath(src);
        final INodeFileUnderConstruction pendingFile
                = INodeFileUnderConstruction.valueOf(iip.getINode(0), src);
        int nrBlocks = pendingFile.numBlocks();
        BlockInfo[] blocks = pendingFile.getBlocks();

        int nrCompleteBlocks;
        BlockInfo curBlock = null;
        for(nrCompleteBlocks = 0; nrCompleteBlocks < nrBlocks; nrCompleteBlocks++) {
            curBlock = blocks[nrCompleteBlocks];
            if(!curBlock.isComplete())
                break;
            assert blockManager.checkMinReplication(curBlock) :
                    "A COMPLETE block is not minimally replicated in " + src;
        }

        // If there are no incomplete blocks associated with this file,
        // then reap lease immediately and close the file.
        if(nrCompleteBlocks == nrBlocks) {
            finalizeINodeFileUnderConstruction(src, pendingFile,
                    iip.getLatestSnapshot());
            NameNode.stateChangeLog.warn("BLOCK*"
                    + " internalReleaseLease: All existing blocks are COMPLETE,"
                    + " lease removed, file closed.");
            return true;  // closed!
        }

        // Only the last and the penultimate blocks may be in non COMPLETE state.
        // If the penultimate block is not COMPLETE, then it must be COMMITTED.
        if(nrCompleteBlocks < nrBlocks - 2 ||
                nrCompleteBlocks == nrBlocks - 2 &&
                        curBlock != null &&
                        curBlock.getBlockUCState() != HdfsServerConstants.BlockUCState.COMMITTED) {
            final String message = "DIR* NameSystem.internalReleaseLease: "
                    + "attempt to release a create lock on "
                    + src + " but file is already closed.";
            NameNode.stateChangeLog.warn(message);
            throw new IOException(message);
        }

        // The last block is not COMPLETE, and
        // that the penultimate block if exists is either COMPLETE or COMMITTED
        final BlockInfo lastBlock = pendingFile.getLastBlock();
        HdfsServerConstants.BlockUCState lastBlockState = lastBlock.getBlockUCState();
        BlockInfo penultimateBlock = pendingFile.getPenultimateBlock();
        boolean penultimateBlockMinReplication;
        HdfsServerConstants.BlockUCState penultimateBlockState;
        if (penultimateBlock == null) {
            penultimateBlockState = HdfsServerConstants.BlockUCState.COMPLETE;
            // If penultimate block doesn't exist then its minReplication is met
            penultimateBlockMinReplication = true;
        } else {
            penultimateBlockState = HdfsServerConstants.BlockUCState.COMMITTED;
            penultimateBlockMinReplication =
                    blockManager.checkMinReplication(penultimateBlock);
        }
        assert penultimateBlockState == HdfsServerConstants.BlockUCState.COMPLETE ||
                penultimateBlockState == HdfsServerConstants.BlockUCState.COMMITTED :
                "Unexpected state of penultimate block in " + src;

        switch(lastBlockState) {
            case COMPLETE:
                assert false : "Already checked that the last block is incomplete";
                break;
            case COMMITTED:
                // Close file if committed blocks are minimally replicated
                if(penultimateBlockMinReplication &&
                        blockManager.checkMinReplication(lastBlock)) {
                    finalizeINodeFileUnderConstruction(src, pendingFile,
                            iip.getLatestSnapshot());
                    NameNode.stateChangeLog.warn("BLOCK*"
                            + " internalReleaseLease: Committed blocks are minimally replicated,"
                            + " lease removed, file closed.");
                    return true;  // closed!
                }
                // Cannot close file right now, since some blocks
                // are not yet minimally replicated.
                // This may potentially cause infinite loop in lease recovery
                // if there are no valid replicas on data-nodes.
                String message = "DIR* NameSystem.internalReleaseLease: " +
                        "Failed to release lease for file " + src +
                        ". Committed blocks are waiting to be minimally replicated." +
                        " Try again later.";
                NameNode.stateChangeLog.warn(message);
                throw new AlreadyBeingCreatedException(message);
            case UNDER_CONSTRUCTION:
            case UNDER_RECOVERY:
                final BlockInfoUnderConstruction uc = (BlockInfoUnderConstruction)lastBlock;
                // setup the last block locations from the blockManager if not known
                if (uc.getNumExpectedLocations() == 0) {
                    uc.setExpectedLocations(blockManager.getNodes(lastBlock));
                }
                // start recovery of the last block for this file
                long blockRecoveryId = nextGenerationStamp(isLegacyBlock(uc));
                lease = reassignLease(lease, src, recoveryLeaseHolder, pendingFile);
                uc.initializeBlockRecovery(blockRecoveryId);
                leaseManager.renewLease(lease);
                // Cannot close file right now, since the last block requires recovery.
                // This may potentially cause infinite loop in lease recovery
                // if there are no valid replicas on data-nodes.
                NameNode.stateChangeLog.warn(
                        "DIR* NameSystem.internalReleaseLease: " +
                                "File " + src + " has not been closed." +
                                " Lease recovery is in progress. " +
                                "RecoveryId = " + blockRecoveryId + " for block " + lastBlock);
                break;
        }
        return false;
    }
    /**
     * Remove a list of INodeDirectorySnapshottable from the SnapshotManager
     * @param toRemove the list of INodeDirectorySnapshottable to be removed
     */
    void removeSnapshottableDirs(List<FileINodeDirectorySnapshottable> toRemove) {
        if (snapshotManager != null) {
            snapshotManager.removeSnapshottable(toRemove);
        }
    }
    boolean isLegacyBlock(Block block) {
        return block.getGenerationStamp() < getGenerationStampV1Limit();
    }
    long getGenerationStampV1Limit() {
        return generationStampV1Limit;
    }
    private FileLeaseManager.Lease reassignLease(FileLeaseManager.Lease lease, String src, String newHolder,
                                                 INodeFileUnderConstruction pendingFile) {
        assert hasWriteLock();
        if(newHolder == null)
            return lease;
        // The following transaction is not synced. Make sure it's sync'ed later.
        logReassignLease(lease.getHolder(), src, newHolder);
        return reassignLeaseInternal(lease, src, newHolder, pendingFile);
    }
    FileLeaseManager.Lease reassignLeaseInternal(FileLeaseManager.Lease lease, String src, String newHolder,
                                                 INodeFileUnderConstruction pendingFile) {
        assert hasWriteLock();
        pendingFile.setClientName(newHolder);
        return leaseManager.reassignLease(lease, src, newHolder);
    }
    private void logReassignLease(String leaseHolder, String src,
                                  String newHolder) {
        assert hasWriteLock();
        getEditLog().logReassignLease(leaseHolder, src, newHolder);
    }

    /**
     * Remove leases, inodes and blocks related to a given path
     * @param src The given path
     * @param blocks Containing the list of blocks to be deleted from blocksMap
     * @param removedINodes Containing the list of inodes to be removed from
     *                      inodesMap
     */
    void removePathAndBlocks(String src, INode.BlocksMapUpdateInfo blocks,
                             List<INode> removedINodes) {
        assert hasWriteLock();
        leaseManager.removeLeaseWithPrefixPath(src);
        // remove inodes from inodesMap
        if (removedINodes != null) {
            dir.removeFromInodeMap(removedINodes);
            removedINodes.clear();
        }
        if (blocks == null) {
            return;
        }

        // In the case that we are a Standby tailing edits from the
        // active while in safe-mode, we need to track the total number
        // of blocks and safe blocks in the system.
        boolean trackBlockCounts = isSafeModeTrackingBlocks();
        int numRemovedComplete = 0, numRemovedSafe = 0;

        for (Block b : blocks.getToDeleteList()) {
            if (trackBlockCounts) {
                BlockInfo bi = getStoredBlock(b);
                if (bi.isComplete()) {
                    numRemovedComplete++;
                    if (bi.numNodes() >= blockManager.minReplication) {
                        numRemovedSafe++;
                    }
                }
            }
            blockManager.removeBlock(b);
        }
        if (trackBlockCounts) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Adjusting safe-mode totals for deletion of " + src + ":" +
                        "decreasing safeBlocks by " + numRemovedSafe +
                        ", totalBlocks by " + numRemovedComplete);
            }
            adjustSafeModeBlockTotals(-numRemovedSafe, -numRemovedComplete);
        }
    }
    private boolean isSafeModeTrackingBlocks() {
        if (!haEnabled) {
            // Never track blocks incrementally in non-HA code.
            return false;
        }
        SafeModeInfo sm = this.safeMode;
        return sm != null && sm.shouldIncrementallyTrackBlocks();
    }
    void removeBlocks(INode.BlocksMapUpdateInfo blocks) {
        int start = 0;
        int end = 0;
        List<Block> toDeleteList = blocks.getToDeleteList();
        while (start < toDeleteList.size()) {
            end = BLOCK_DELETION_INCREMENT + start;
            end = end > toDeleteList.size() ? toDeleteList.size() : end;
            writeLock();
            try {
                for (int i = start; i < end; i++) {
                    blockManager.removeBlock(toDeleteList.get(i));
                }
            } finally {
                writeUnlock();
            }
            start = end;
        }
    }
    private void finalizeINodeFileUnderConstruction(String src,
                                                    INodeFileUnderConstruction pendingFile, Snapshot latestSnapshot)
            throws IOException, UnresolvedLinkException {
        assert hasWriteLock();
        leaseManager.removeLease(pendingFile.getClientName(), src);

        pendingFile = pendingFile.recordFileModification(latestSnapshot,
                dir.getINodeMap());

        // The file is no longer pending.
        // Create permanent INode, update blocks
        final INodeFile newFile = pendingFile.toINodeFile(now());
        dir.replaceINodeFile(src, pendingFile, newFile);

        // close file and persist block allocations for this file
        dir.closeFile(src, newFile);

        blockManager.checkReplication(newFile);
    }
    /**
     * Increments, logs and then returns the stamp
     */
    long nextGenerationStamp(boolean legacyBlock)
            throws IOException, SafeModeException {
        assert hasWriteLock();
        if (isInSafeMode()) {
       /*     throw new SafeModeException(
                    "Cannot get next generation stamp", safeMode);*/
        }

        long gs;
        if (legacyBlock) {
            gs = getNextGenerationStampV1();
            getEditLog().logGenerationStampV1(gs);
        } else {
            gs = getNextGenerationStampV2();
            getEditLog().logGenerationStampV2(gs);
        }

        // NB: callers sync the log
        return gs;



    }
    long getNextGenerationStampV1() throws IOException {
        long genStampV1 = generationStampV1.nextValue();

        if (genStampV1 >= generationStampV1Limit) {
            // We ran out of generation stamps for legacy blocks. In practice, it
            // is extremely unlikely as we reserved 1T v1 generation stamps. The
            // result is that we can no longer append to the legacy blocks that
            // were created before the upgrade to sequential block IDs.
            throw new OutOfV1GenerationStampsException();
        }

        return genStampV1;
    }

    long getNextGenerationStampV2() {
        return generationStampV2.nextValue();
    }
    BlockInfo getStoredBlock(Block block) {
        return blockManager.getStoredBlock(block);
    }

    /**
     * Append to an existing file for append.
     * <p>
     *
     * The method returns the last block of the file if this is a partial block,
     * which can still be used for writing more data. The client uses the returned
     * block locations to form the data pipeline for this block.<br>
     * The method returns null if the last block is full. The client then
     * allocates a new block with the next call using .
     * <p>
     *
     * For description of parameters and exceptions thrown see
     * {@link ClientProtocol#append(String, String)}
     *
     * @return the last block locations if the block is partial or null otherwise
     */
    private LocatedBlock appendFileInternal(FSPermissionChecker pc, String src,
                                            String holder, String clientMachine, boolean logRetryCache)
            throws AccessControlException, UnresolvedLinkException,
            FileNotFoundException, IOException {
        assert hasWriteLock();
        // Verify that the destination does not exist as a directory already.
        final FileINodesInPath iip = dir.getINodesInPath4Write(src);
        final INode inode = iip.getLastINode();
        if (inode != null && inode.isDirectory()) {
            throw new FileAlreadyExistsException("Cannot append to directory " + src
                    + "; already exists as a directory.");
        }
        if (isPermissionEnabled) {
            checkPathAccess(pc, src, FsAction.WRITE);
        }

        try {
            if (inode == null) {
                throw new FileNotFoundException("failed to append to non-existent file "
                        + src + " on client " + clientMachine);
            }
            INodeFile myFile = INodeFile.valueOf(inode, src, true);
            // Opening an existing file for write - may need to recover lease.
            recoverLeaseInternal(myFile, src, holder, clientMachine, false);

            // recoverLeaseInternal may create a new InodeFile via
            // finalizeINodeFileUnderConstruction so we need to refresh
            // the referenced file.
            myFile = INodeFile.valueOf(dir.getINode(src), src, true);

            final DatanodeDescriptor clientNode =
                    blockManager.getDatanodeManager().getDatanodeByHost(clientMachine);
            return prepareFileForWrite(src, myFile, holder, clientMachine, clientNode,
                    true, iip.getLatestSnapshot(), logRetryCache);
        } catch (IOException ie) {
            NameNodeTest.stateChangeLog.warn("DIR* NameSystem.append: " +ie.getMessage());
            throw ie;
        }
    }

    /**
     * Append to an existing file in the namespace.
     */
  public   LocatedBlock appendFile(String src, String holder, String clientMachine)
            throws AccessControlException, SafeModeException,
            FileAlreadyExistsException, FileNotFoundException,
            ParentNotDirectoryException, IOException {
        LocatedBlock lb = null;
        RetryCache.CacheEntryWithPayload cacheEntry = RetryCache.waitForCompletion(retryCache,
                null);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return (LocatedBlock) cacheEntry.getPayload();
        }

        boolean success = false;
        try {
            lb = appendFileInt(src, holder, clientMachine, cacheEntry != null);
            success = true;
            return lb;
        } catch (AccessControlException e) {
            logAuditEvent(false, "append", src);
            throw e;
        } finally {
            RetryCache.setState(cacheEntry, success, lb);
        }
    }

    private LocatedBlock appendFileInt(String src, String holder,
                                       String clientMachine, boolean logRetryCache)
            throws AccessControlException, SafeModeException,
            FileAlreadyExistsException, FileNotFoundException,
            ParentNotDirectoryException, IOException {
        if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* NameSystem.appendFile: src=" + src
                    + ", holder=" + holder
                    + ", clientMachine=" + clientMachine);
        }
        boolean skipSync = false;
        if (!supportAppends) {
            throw new UnsupportedOperationException(
                    "Append is not enabled on this NameNode. Use the " +
                            DFS_SUPPORT_APPEND_KEY + " configuration option to enable it.");
        }

        LocatedBlock lb = null;
        FSPermissionChecker pc = getPermissionChecker();
        checkOperation(NameNode.OperationCategory.WRITE);
        byte[][] pathComponents = FileSystemDirectory.getPathComponentsForReservedPath(src);
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.WRITE);
            if (isInSafeMode()) {
               // throw new SafeModeException("Cannot append to file" + src, safeMode);
            }
            src = FileSystemDirectory.resolvePath(src, pathComponents, dir);
            lb = appendFileInternal(pc, src, holder, clientMachine, logRetryCache);
        } catch (StandbyException se) {
            skipSync = true;
            throw se;
        } finally {
            writeUnlock();
            // There might be transactions logged while trying to recover the lease.
            // They need to be sync'ed even when an exception was thrown.
            if (!skipSync) {
                getEditLog().logSync();
            }
        }
        if (lb != null) {
            if (NameNodeTest.stateChangeLog.isDebugEnabled()) {
                NameNodeTest.stateChangeLog.debug("DIR* NameSystem.appendFile: file "
                        +src+" for "+holder+" at "+clientMachine
                        +" block " + lb.getBlock()
                        +" block size " + lb.getBlock().getNumBytes());
            }
        }
        logAuditEvent(true, "append", src);
        return lb;
    }

    /**
     * Set replication for an existing file.
     *
     * The NameNode sets new replication and schedules either replication of
     * under-replicated data blocks or removal of the excessive block copies
     * if the blocks are over-replicated.
     *
     * @see ClientProtocol#setReplication(String, short)
     * @param src file name
     * @param replication new replication
     * @return true if successful;
     *         false if file does not exist or is a directory
     */
   public boolean setReplication(final String src, final short replication)
            throws IOException {
        try {
            return setReplicationInt(src, replication);
        } catch (AccessControlException e) {
            logAuditEvent(false, "setReplication", src);
            throw e;
        }
    }

    private boolean setReplicationInt(String src, final short replication)
            throws IOException {
        blockManager.verifyReplication(src, replication, null);
        final boolean isFile;
        FSPermissionChecker pc = getPermissionChecker();
        checkOperation(NameNode.OperationCategory.WRITE);
        byte[][] pathComponents = FileSystemDirectory.getPathComponentsForReservedPath(src);
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.WRITE);
            if (isInSafeMode()) {
              //  throw new SafeModeException("Cannot set replication for " + src, safeMode);
            }
            src = FileSystemDirectory.resolvePath(src, pathComponents, dir);
            if (isPermissionEnabled) {
                checkPathAccess(pc, src, FsAction.WRITE);
            }

            final short[] blockRepls = new short[2]; // 0: old, 1: new
            final Block[] blocks = dir.setReplication(src, replication, blockRepls);
            isFile = blocks != null;
            if (isFile) {
                blockManager.setReplication(blockRepls[0], blockRepls[1], src, blocks);
            }
        } finally {
            writeUnlock();
        }

        getEditLog().logSync();
        if (isFile) {
            logAuditEvent(true, "setReplication", src);
        }
        return isFile;
    }

    /**
     * Set permissions for an existing file.
     * @throws IOException
     */
   public void setPermission(String src, FsPermission permission)
            throws AccessControlException, FileNotFoundException, SafeModeException,
            UnresolvedLinkException, IOException {
        try {
            setPermissionInt(src, permission);
        } catch (AccessControlException e) {
            logAuditEvent(false, "setPermission", src);
            throw e;
        }
    }

    private void setPermissionInt(String src, FsPermission permission)
            throws AccessControlException, FileNotFoundException, SafeModeException,
            UnresolvedLinkException, IOException {
        HdfsFileStatus resultingStat = null;
        FSPermissionChecker pc = getPermissionChecker();
        checkOperation(NameNode.OperationCategory.WRITE);
        byte[][] pathComponents = FileSystemDirectory.getPathComponentsForReservedPath(src);
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.WRITE);
            if (isInSafeMode()) {
                //throw new SafeModeException("Cannot set permission for " + src, safeMode);
            }
            src = FileSystemDirectory.resolvePath(src, pathComponents, dir);
            checkOwner(pc, src);
            dir.setPermission(src, permission);
            resultingStat = getAuditFileInfo(src, false);
        } finally {
            writeUnlock();
        }
        getEditLog().logSync();
        logAuditEvent(true, "setPermission", src, null, resultingStat);
    }

    private void checkOwner(FSPermissionChecker pc, String path)
            throws AccessControlException, UnresolvedLinkException {
        checkPermission(pc, path, true, null, null, null, null);
    }
    private HdfsFileStatus getAuditFileInfo(String path, boolean resolveSymlink)
            throws IOException {
        return (isAuditEnabled() && isExternalInvocation())
                ? dir.getFileInfo(path, resolveSymlink) : null;
    }
  public   void setOwner(String src, String username, String group)
            throws AccessControlException, FileNotFoundException, SafeModeException,
            UnresolvedLinkException, IOException {
        try {
            setOwnerInt(src, username, group);
        } catch (AccessControlException e) {
            logAuditEvent(false, "setOwner", src);
            throw e;
        }
    }

    private void setOwnerInt(String src, String username, String group)
            throws AccessControlException, FileNotFoundException, SafeModeException,
            UnresolvedLinkException, IOException {
        HdfsFileStatus resultingStat = null;
        FSPermissionChecker pc = getPermissionChecker();
        checkOperation(NameNode.OperationCategory.WRITE);
        byte[][] pathComponents = FileSystemDirectory.getPathComponentsForReservedPath(src);
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.WRITE);
            if (isInSafeMode()) {
               // throw new SafeModeException("Cannot set owner for " + src, safeMode);
            }
            src = FileSystemDirectory.resolvePath(src, pathComponents, dir);
            checkOwner(pc, src);
            if (!pc.isSuperUser()) {
                if (username != null && !pc.getUser().equals(username)) {
                    throw new AccessControlException("Non-super user cannot change owner");
                }
                if (group != null && !pc.containsGroup(group)) {
                    throw new AccessControlException("User does not belong to " + group);
                }
            }
            dir.setOwner(src, username, group);
            resultingStat = getAuditFileInfo(src, false);
        } finally {
            writeUnlock();
        }
        getEditLog().logSync();
        logAuditEvent(true, "setOwner", src, null, resultingStat);
    }

    /**
     * The client would like to let go of the given block
     */
  public   boolean abandonBlock(ExtendedBlock b, String src, String holder)
            throws LeaseExpiredException, FileNotFoundException,
            UnresolvedLinkException, IOException {
        if(NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("BLOCK* NameSystem.abandonBlock: " + b
                    + "of file " + src);
        }
        checkOperation(NameNode.OperationCategory.WRITE);
        byte[][] pathComponents = FileSystemDirectory.getPathComponentsForReservedPath(src);
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.WRITE);
            if (isInSafeMode()) {
              /*  throw new SafeModeException("Cannot abandon block " + b +
                        " for fle" + src, safeMode);*/
            }
            src = FileSystemDirectory.resolvePath(src, pathComponents, dir);

            //
            // Remove the block from the pending creates list
            //
            INodeFileUnderConstruction file = checkLease(src, holder);
            boolean removed = dir.removeBlock(src, file,
                    ExtendedBlock.getLocalBlock(b));
            if (!removed) {
                return true;
            }
            if(NameNode.stateChangeLog.isDebugEnabled()) {
                NameNode.stateChangeLog.debug("BLOCK* NameSystem.abandonBlock: "
                        + b + " is removed from pendingCreates");
            }
            dir.persistBlocks(src, file, false);
        } finally {
            writeUnlock();
        }
        if (persistBlocks) {
            getEditLog().logSync();
        }

        return true;
    }



    /** make sure that we still have the lease on this file. */
    private INodeFileUnderConstruction checkLease(String src, String holder)
            throws LeaseExpiredException, UnresolvedLinkException,
            FileNotFoundException {
        return checkLease(src, INodeId.GRANDFATHER_INODE_ID, holder,
                dir.getINode(src));
    }

    private INodeFileUnderConstruction checkLease(String src, long fileId,
                                                  String holder, INode inode) throws LeaseExpiredException,
            FileNotFoundException {
        assert hasReadOrWriteLock();
        if (inode == null || !inode.isFile()) {
            FileLeaseManager.Lease lease = leaseManager.getLease(holder);
            throw new LeaseExpiredException(
                    "No lease on " + src + ": File does not exist. "
                            + (lease != null ? lease.toString()
                            : "Holder " + holder + " does not have any open files."));
        }
        final INodeFile file = inode.asFile();
        if (!file.isUnderConstruction()) {
            FileLeaseManager.Lease lease = leaseManager.getLease(holder);
            throw new LeaseExpiredException(
                    "No lease on " + src + ": File is not open for writing. "
                            + (lease != null ? lease.toString()
                            : "Holder " + holder + " does not have any open files."));
        }
        INodeFileUnderConstruction pendingFile = (INodeFileUnderConstruction)file;
        if (holder != null && !pendingFile.getClientName().equals(holder)) {
            throw new LeaseExpiredException("Lease mismatch on " + src + " owned by "
                    + pendingFile.getClientName() + " but is accessed by " + holder);
        }
        INodeId.checkId(fileId, pendingFile);
        return pendingFile;
    }
    /**
     * The client would like to obtain an additional block for the indicated
     * filename (which is being written-to).  Return an array that consists
     * of the block, plus a set of machines.  The first on this list should
     * be where the client writes data.  Subsequent items in the list must
     * be provided in the connection to the first datanode.
     *
     * Make sure the previous blocks have been reported by datanodes and
     * are replicated.  Will return an empty 2-elt array if we want the
     * client to "try again later".
     */
   public LocatedBlock getAdditionalBlock(String src, long fileId, String clientName,
                                    ExtendedBlock previous, HashMap<Node, Node> excludedNodes,
                                    List<String> favoredNodes)
            throws LeaseExpiredException, NotReplicatedYetException,
            QuotaExceededException, SafeModeException, UnresolvedLinkException,
            IOException {
        long blockSize;
        int replication;
        DatanodeDescriptor clientNode = null;

        if(NameNodeTest.stateChangeLog.isDebugEnabled()) {
            NameNodeTest.stateChangeLog.debug(
                    "BLOCK* NameSystem.getAdditionalBlock: file "
                            +src+" for "+clientName);
        }

        // Part I. Analyze the state of the file with respect to the input data.
        checkOperation(NameNode.OperationCategory.READ);
        byte[][] pathComponents = FileSystemDirectory.getPathComponentsForReservedPath(src);
        readLock();
        try {
            checkOperation(NameNode.OperationCategory.READ);
            src = FileSystemDirectory.resolvePath(src, pathComponents, dir);
            LocatedBlock[] onRetryBlock = new LocatedBlock[1];
            final INode[] inodes = analyzeFileState(
                    src, fileId, clientName, previous, onRetryBlock).getINodes();
            final INodeFileUnderConstruction pendingFile =
                    (INodeFileUnderConstruction) inodes[inodes.length - 1];

            if(onRetryBlock[0] != null) {
                // This is a retry. Just return the last block.
                return onRetryBlock[0];
            }
            if (pendingFile.getBlocks().length >= maxBlocksPerFile) {
                throw new IOException("File has reached the limit on maximum number of"
                        + " blocks (" + DFSConfigKeys.DFS_NAMENODE_MAX_BLOCKS_PER_FILE_KEY
                        + "): " + pendingFile.getBlocks().length + " >= "
                        + maxBlocksPerFile);
            }
            blockSize = pendingFile.getPreferredBlockSize();
            clientNode = pendingFile.getClientNode();
            replication = pendingFile.getFileReplication();
        } finally {
            readUnlock();
        }

        // choose targets for the new block to be allocated.
        final DatanodeDescriptor targets[] = getBlockManager().chooseTarget(
                src, replication, clientNode, excludedNodes, blockSize, favoredNodes);

        // Part II.
        // Allocate a new block, add it to the INode and the BlocksMap.
        Block newBlock = null;
        long offset;
        checkOperation(NameNode.OperationCategory.WRITE);
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.WRITE);
            // Run the full analysis again, since things could have changed
            // while chooseTarget() was executing.
            LocatedBlock[] onRetryBlock = new LocatedBlock[1];
            FileINodesInPath inodesInPath =
                    analyzeFileState(src, fileId, clientName, previous, onRetryBlock);
            final INode[] inodes = inodesInPath.getINodes();
            final INodeFileUnderConstruction pendingFile =
                    (INodeFileUnderConstruction) inodes[inodes.length - 1];

            if(onRetryBlock[0] != null) {
                // This is a retry. Just return the last block.
                return onRetryBlock[0];
            }

            // commit the last block and complete it if it has minimum replicas
            commitOrCompleteLastBlock(pendingFile,
                    ExtendedBlock.getLocalBlock(previous));

            // allocate new block, record block locations in INode.
            newBlock = createNewBlock();
            saveAllocatedBlock(src, inodesInPath, newBlock, targets);

            dir.persistBlocks(src, pendingFile, false);
            offset = pendingFile.computeFileSize();
        } finally {
            writeUnlock();
        }
        if (persistBlocks) {
            getEditLog().logSync();
        }

        // Return located block
        return makeLocatedBlock(newBlock, targets, offset);
    }

    LocatedBlock makeLocatedBlock(Block blk,
                                  DatanodeInfo[] locs,
                                  long offset) throws IOException {
        LocatedBlock lBlk = new LocatedBlock(
                getExtendedBlock(blk), locs, offset);
        getBlockManager().setBlockToken(
                lBlk, BlockTokenSecretManager.AccessMode.WRITE);
        return lBlk;
    }

    ExtendedBlock getExtendedBlock(Block blk) {
        return new ExtendedBlock(blockPoolId, blk);
    }
    /**
     * Save allocated block at the given pending filename
     *
     * @param src path to the file
     * @throws QuotaExceededException If addition of block exceeds space quota
     */
    BlockInfo saveAllocatedBlock(String src, FileINodesInPath inodes,
                                 Block newBlock, DatanodeDescriptor targets[]) throws IOException {
        assert hasWriteLock();
        BlockInfo b = dir.addBlock(src, inodes, newBlock, targets);
        NameNode.stateChangeLog.info("BLOCK* allocateBlock: " + src + ". "
                + getBlockPoolId() + " " + b);
        for (DatanodeDescriptor dn : targets) {
            dn.incBlocksScheduled();
        }
        return b;
    }


    /**
     * Increments, logs and then returns the block ID
     */
    private long nextBlockId() throws SafeModeException {
        assert hasWriteLock();
        if (isInSafeMode()) {
           /* throw new SafeModeException(
                    "Cannot get next block ID", safeMode);*/
        }
        final long blockId = blockIdGenerator.nextValue();
        getEditLog().logAllocateBlockId(blockId);
        // NB: callers sync the log
        return blockId;
    }

    Block createNewBlock() throws IOException {
        assert hasWriteLock();
        Block b = new Block(nextBlockId(), 0, 0);
        // Increment the generation stamp for every new block.
        b.setGenerationStamp(nextGenerationStamp(false));
        return b;
    }
    private void commitOrCompleteLastBlock(final INodeFileUnderConstruction fileINode,
                                           final Block commitBlock) throws IOException {
        assert hasWriteLock();
        if (!blockManager.commitOrCompleteLastBlock(fileINode, commitBlock)) {
            return;
        }

        // Adjust disk space consumption if required
        final long diff = fileINode.getPreferredBlockSize() - commitBlock.getNumBytes();
        if (diff > 0) {
            try {
                String path = leaseManager.findPath(fileINode);
                dir.updateSpaceConsumed(path, 0, -diff*fileINode.getFileReplication());
            } catch (IOException e) {
                LOG.warn("Unexpected exception while updating disk space.", e);
            }
        }
    }
    FileINodesInPath analyzeFileState(String src,
                                  long fileId,
                                  String clientName,
                                  ExtendedBlock previous,
                                  LocatedBlock[] onRetryBlock)
            throws IOException  {
        assert hasReadOrWriteLock();

        checkBlock(previous);
        onRetryBlock[0] = null;
        checkOperation(NameNode.OperationCategory.WRITE);
        if (isInSafeMode()) {
            //throw new SafeModeException("Cannot add block to " + src, safeMode);
        }

        // have we exceeded the configured limit of fs objects.
        checkFsObjectLimit();

        Block previousBlock = ExtendedBlock.getLocalBlock(previous);
        final FileINodesInPath iip = dir.getINodesInPath4Write(src);
        final INodeFileUnderConstruction pendingFile
                = checkLease(src, fileId, clientName, iip.getLastINode());
        BlockInfo lastBlockInFile = pendingFile.getLastBlock();
        if (!Block.matchingIdAndGenStamp(previousBlock, lastBlockInFile)) {
            // The block that the client claims is the current last block
            // doesn't match up with what we think is the last block. There are
            // four possibilities:
            // 1) This is the first block allocation of an append() pipeline
            //    which started appending exactly at a block boundary.
            //    In this case, the client isn't passed the previous block,
            //    so it makes the allocateBlock() call with previous=null.
            //    We can distinguish this since the last block of the file
            //    will be exactly a full block.
            // 2) This is a retry from a client that missed the response of a
            //    prior getAdditionalBlock() call, perhaps because of a network
            //    timeout, or because of an HA failover. In that case, we know
            //    by the fact that the client is re-issuing the RPC that it
            //    never began to write to the old block. Hence it is safe to
            //    to return the existing block.
            // 3) This is an entirely bogus request/bug -- we should error out
            //    rather than potentially appending a new block with an empty
            //    one in the middle, etc
            // 4) This is a retry from a client that timed out while
            //    the prior getAdditionalBlock() is still being processed,
            //    currently working on chooseTarget().
            //    There are no means to distinguish between the first and
            //    the second attempts in Part I, because the first one hasn't
            //    changed the namesystem state yet.
            //    We run this analysis again in Part II where case 4 is impossible.

            BlockInfo penultimateBlock = pendingFile.getPenultimateBlock();
            if (previous == null &&
                    lastBlockInFile != null &&
                    lastBlockInFile.getNumBytes() == pendingFile.getPreferredBlockSize() &&
                    lastBlockInFile.isComplete()) {
                // Case 1
                if (NameNode.stateChangeLog.isDebugEnabled()) {
                    NameNode.stateChangeLog.debug(
                            "BLOCK* NameSystem.allocateBlock: handling block allocation" +
                                    " writing to a file with a complete previous block: src=" +
                                    src + " lastBlock=" + lastBlockInFile);
                }
            } else if (Block.matchingIdAndGenStamp(penultimateBlock, previousBlock)) {
                if (lastBlockInFile.getNumBytes() != 0) {
                    throw new IOException(
                            "Request looked like a retry to allocate block " +
                                    lastBlockInFile + " but it already contains " +
                                    lastBlockInFile.getNumBytes() + " bytes");
                }

                // Case 2
                // Return the last block.
                NameNode.stateChangeLog.info("BLOCK* allocateBlock: " +
                        "caught retry for allocation of a new block in " +
                        src + ". Returning previously allocated block " + lastBlockInFile);
                long offset = pendingFile.computeFileSize();
                onRetryBlock[0] = makeLocatedBlock(lastBlockInFile,
                        ((BlockInfoUnderConstruction)lastBlockInFile).getExpectedLocations(),
                        offset);
                return iip;
            } else {
                // Case 3
                throw new IOException("Cannot allocate block in " + src + ": " +
                        "passed 'previous' block " + previous + " does not match actual " +
                        "last block in file " + lastBlockInFile);
            }
        }

        // Check if the penultimate block is minimally replicated
        if (!checkFileProgress(pendingFile, false)) {
            throw new NotReplicatedYetException("Not replicated yet: " + src);
        }
        return iip;
    }

    private void checkBlock(ExtendedBlock block) throws IOException {
        if (block != null && !this.blockPoolId.equals(block.getBlockPoolId())) {
            throw new IOException("Unexpected BlockPoolId " + block.getBlockPoolId()
                    + " - expected " + blockPoolId);
        }
    }

    /**
     * Check that the indicated file's blocks are present and
     * replicated.  If not, return false. If checkall is true, then check
     * all blocks, otherwise check only penultimate block.
     */
    boolean checkFileProgress(INodeFile v, boolean checkall) {
        readLock();
        try {
            if (checkall) {
                //
                // check all blocks of the file.
                //
                for (BlockInfo block: v.getBlocks()) {
                    if (!block.isComplete()) {
                        LOG.info("BLOCK* checkFileProgress: " + block
                                + " has not reached minimal replication "
                                + blockManager.minReplication);
                        return false;
                    }
                }
            } else {
                //
                // check the penultimate block of this file
                //
                BlockInfo b = v.getPenultimateBlock();
                if (b != null && !b.isComplete()) {
                    LOG.info("BLOCK* checkFileProgress: " + b
                            + " has not reached minimal replication "
                            + blockManager.minReplication);
                    return false;
                }
            }
            return true;
        } finally {
            readUnlock();
        }
    }

  public   LocatedBlock getAdditionalDatanode(String src, final ExtendedBlock blk,
                                       final DatanodeInfo[] existings,  final HashMap<Node, Node> excludes,
                                       final int numAdditionalNodes, final String clientName
    ) throws IOException {
        //check if the feature is enabled
        dtpReplaceDatanodeOnFailure.checkEnabled();

        final DatanodeDescriptor clientnode;
        final long preferredblocksize;
        final List<DatanodeDescriptor> chosen;
        checkOperation(NameNode.OperationCategory.READ);
        byte[][] pathComponents = FileSystemDirectory.getPathComponentsForReservedPath(src);
        readLock();
        try {
            checkOperation(NameNode.OperationCategory.READ);
            //check safe mode
            if (isInSafeMode()) {
         /*       throw new SafeModeException("Cannot add datanode; src=" + src
                        + ", blk=" + blk, safeMode);*/
            }
            src = FileSystemDirectory.resolvePath(src, pathComponents, dir);

            //check lease
            final INodeFileUnderConstruction file = checkLease(src, clientName);
            clientnode = file.getClientNode();
            preferredblocksize = file.getPreferredBlockSize();

            //find datanode descriptors
            chosen = new ArrayList<DatanodeDescriptor>();
            for(DatanodeInfo d : existings) {
                final DatanodeDescriptor descriptor = blockManager.getDatanodeManager(
                ).getDatanode(d);
                if (descriptor != null) {
                    chosen.add(descriptor);
                }
            }
        } finally {
            readUnlock();
        }

        // choose new datanodes.
        final DatanodeInfo[] targets = blockManager.getBlockPlacementPolicy(
        ).chooseTarget(src, numAdditionalNodes, clientnode, chosen, true,
                excludes, preferredblocksize);
        final LocatedBlock lb = new LocatedBlock(blk, targets);
        blockManager.setBlockToken(lb, BlockTokenSecretManager.AccessMode.COPY);
        return lb;
    }

    /**
     * Complete in-progress write to the given file.
     * @return true if successful, false if the client should continue to retry
     *         (e.g if not all blocks have reached minimum replication yet)
     * @throws IOException on error (eg lease mismatch, file not open, file deleted)
     */
   public boolean completeFile(String src, String holder,
                         ExtendedBlock last, long fileId)
            throws SafeModeException, UnresolvedLinkException, IOException {
        if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* NameSystem.completeFile: " +
                    src + " for " + holder);
        }
        checkBlock(last);
        boolean success = false;
        checkOperation(NameNode.OperationCategory.WRITE);
        byte[][] pathComponents = FileSystemDirectory.getPathComponentsForReservedPath(src);
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.WRITE);
            if (isInSafeMode()) {
               // throw new SafeModeException("Cannot complete file " + src, safeMode);
            }
            src = FileSystemDirectory.resolvePath(src, pathComponents, dir);
            success = completeFileInternal(src, holder,
                    ExtendedBlock.getLocalBlock(last), fileId);
        } finally {
            writeUnlock();
        }
        getEditLog().logSync();
        NameNode.stateChangeLog.info("DIR* completeFile: " + src + " is closed by "
                + holder);
        return success;
    }


    private boolean completeFileInternal(String src,
                                         String holder, Block last, long fileId) throws SafeModeException,
            UnresolvedLinkException, IOException {
        assert hasWriteLock();
        final FileINodesInPath iip = dir.getLastINodeInPath(src);
        final INodeFileUnderConstruction pendingFile;
        try {
            pendingFile = checkLease(src, fileId, holder, iip.getINode(0));
        } catch (LeaseExpiredException lee) {
            final INode inode = dir.getINode(src);
            if (inode != null
                    && inode.isFile()
                    && !inode.asFile().isUnderConstruction()) {
                // This could be a retry RPC - i.e the client tried to close
                // the file, but missed the RPC response. Thus, it is trying
                // again to close the file. If the file still exists and
                // the client's view of the last block matches the actual
                // last block, then we'll treat it as a successful close.
                // See HDFS-3031.
                final Block realLastBlock = inode.asFile().getLastBlock();
                if (Block.matchingIdAndGenStamp(last, realLastBlock)) {
                    NameNode.stateChangeLog.info("DIR* completeFile: " +
                            "request from " + holder + " to complete " + src +
                            " which is already closed. But, it appears to be an RPC " +
                            "retry. Returning success");
                    return true;
                }
            }
            throw lee;
        }
        // commit the last block and complete it if it has minimum replicas
        commitOrCompleteLastBlock(pendingFile, last);

        if (!checkFileProgress(pendingFile, true)) {
            return false;
        }

        finalizeINodeFileUnderConstruction(src, pendingFile,
                iip.getLatestSnapshot());
        return true;
    }

   public void registerDatanode(DatanodeRegistration nodeReg) throws IOException {
        writeLock();
        try {
            getBlockManager().getDatanodeManager().registerDatanode(nodeReg);
            checkSafeMode();
        } finally {
            writeUnlock();
        }
    }
  public   HeartbeatResponse handleHeartbeat(DatanodeRegistration nodeReg,
                                      long capacity, long dfsUsed, long remaining, long blockPoolUsed,
                                      int xceiverCount, int xmitsInProgress, int failedVolumes)
            throws IOException {
        readLock();
        try {
            final int maxTransfer = blockManager.getMaxReplicationStreams()
                    - xmitsInProgress;
            DatanodeCommand[] cmds = blockManager.getDatanodeManager().handleHeartbeat(
                    nodeReg, blockPoolId, capacity, dfsUsed, remaining, blockPoolUsed,
                    xceiverCount, maxTransfer, failedVolumes);
            return new HeartbeatResponse(cmds, createHaStatusHeartbeat());
        } finally {
            readUnlock();
        }
    }


    private NNHAStatusHeartbeat createHaStatusHeartbeat() {
        HAState state = haContext.getState();
        return new NNHAStatusHeartbeat(state.getServiceState(),
                getFSImage().getLastAppliedOrWrittenTxId());
    }
   public String getRegistrationID() {
        return Storage.getRegistrationID(dir.fsImage.getStorage());
    }
    public void processIncrementalBlockReport(final DatanodeID nodeID,
                                              final String poolId, final ReceivedDeletedBlockInfo blockInfos[])
            throws IOException {
        writeLock();
        try {
            blockManager.processIncrementalBlockReport(nodeID, poolId, blockInfos);
        } finally {
            writeUnlock();
        }
    }

   public CheckpointSignature rollEditLog() throws IOException {
        checkSuperuserPrivilege();
        checkOperation(NameNode.OperationCategory.JOURNAL);
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.JOURNAL);
            if (isInSafeMode()) {
              //  throw new SafeModeException("Log not rolled", safeMode);
            }
            LOG.info("Roll Edit Log from " + Server.getRemoteAddress());
            return getFSImage().rollEditLog();
        } finally {
            writeUnlock();
        }
    }
    /**
     * Register a Backup name-node, verifying that it belongs
     * to the correct namespace, and adding it to the set of
     * active journals if necessary.
     *
     * @param bnReg registration of the new BackupNode
     * @param nnReg registration of this NameNode
     * @throws IOException if the namespace IDs do not match
     */
  public   void registerBackupNode(NamenodeRegistration bnReg,
                            NamenodeRegistration nnReg) throws IOException {
        writeLock();
        try {
            if(getFSImage().getStorage().getNamespaceID()
                    != bnReg.getNamespaceID())
                throw new IOException("Incompatible namespaceIDs: "
                        + " Namenode namespaceID = "
                        + getFSImage().getStorage().getNamespaceID() + "; "
                        + bnReg.getRole() +
                        " node namespaceID = " + bnReg.getNamespaceID());
            if (bnReg.getRole() == HdfsServerConstants.NamenodeRole.BACKUP) {
                getFSImage().getEditLog().registerBackupNode(
                        bnReg, nnReg);
            }
        } finally {
            writeUnlock();
        }
    }


 public    NamenodeCommand startCheckpoint(NamenodeRegistration backupNode,
                                    NamenodeRegistration activeNamenode) throws IOException {
        checkOperation(NameNode.OperationCategory.CHECKPOINT);
        RetryCache.CacheEntryWithPayload cacheEntry = RetryCache.waitForCompletion(retryCache,
                null);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return (NamenodeCommand) cacheEntry.getPayload();
        }
        writeLock();
        NamenodeCommand cmd = null;
        try {
            checkOperation(NameNode.OperationCategory.CHECKPOINT);

            if (isInSafeMode()) {
               // throw new SafeModeException("Checkpoint not started", safeMode);
            }
            LOG.info("Start checkpoint for " + backupNode.getAddress());
            cmd = getFSImage().startCheckpoint(backupNode, activeNamenode);
            getEditLog().logSync();
            return cmd;
        } finally {
            writeUnlock();
            RetryCache.setState(cacheEntry, cmd != null, cmd);
        }
    }

   public void endCheckpoint(NamenodeRegistration registration,
                       CheckpointSignature sig) throws IOException {
        checkOperation(NameNode.OperationCategory.CHECKPOINT);
        RetryCache.CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return; // Return previous response
        }
        boolean success = false;
        readLock();
        try {
            checkOperation(NameNode.OperationCategory.CHECKPOINT);

            if (isInSafeMode()) {
                //throw new SafeModeException("Checkpoint not ended", safeMode);
            }
            LOG.info("End checkpoint for " + registration.getAddress());
            getFSImage().endCheckpoint(sig);
            success = true;
        } finally {
            readUnlock();
            RetryCache.setState(cacheEntry, success);
        }
    }

    /**
     * Client is reporting some bad block locations.
     */
  public   void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
        checkOperation(NameNode.OperationCategory.WRITE);
        NameNode.stateChangeLog.info("*DIR* reportBadBlocks");
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.WRITE);
            for (int i = 0; i < blocks.length; i++) {
                ExtendedBlock blk = blocks[i].getBlock();
                DatanodeInfo[] nodes = blocks[i].getLocations();
                for (int j = 0; j < nodes.length; j++) {
                    DatanodeInfo dn = nodes[j];
                    blockManager.findAndMarkBlockAsCorrupt(blk, dn,
                            "client machine reported it");
                }
            }
        } finally {
            writeUnlock();
        }
    }

  public   void commitBlockSynchronization(ExtendedBlock lastblock,
                                    long newgenerationstamp, long newlength,
                                    boolean closeFile, boolean deleteblock, DatanodeID[] newtargets,
                                    String[] newtargetstorages)
            throws IOException, UnresolvedLinkException {
        LOG.info("commitBlockSynchronization(lastblock=" + lastblock
                + ", newgenerationstamp=" + newgenerationstamp
                + ", newlength=" + newlength
                + ", newtargets=" + Arrays.asList(newtargets)
                + ", closeFile=" + closeFile
                + ", deleteBlock=" + deleteblock
                + ")");
        checkOperation(NameNode.OperationCategory.WRITE);
        String src = "";
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.WRITE);
            // If a DN tries to commit to the standby, the recovery will
            // fail, and the next retry will succeed on the new NN.

            if (isInSafeMode()) {
            /*    throw new SafeModeException(
                        "Cannot commitBlockSynchronization while in safe mode",
                        safeMode);*/
            }
            final BlockInfo storedBlock = getStoredBlock(
                    ExtendedBlock.getLocalBlock(lastblock));
            if (storedBlock == null) {
                if (deleteblock) {
                    // This may be a retry attempt so ignore the failure
                    // to locate the block.
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Block (=" + lastblock + ") not found");
                    }
                    return;
                } else {
                    throw new IOException("Block (=" + lastblock + ") not found");
                }
            }
            INodeFile iFile = ((INode)storedBlock.getBlockCollection()).asFile();
            if (!iFile.isUnderConstruction() || storedBlock.isComplete()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Unexpected block (=" + lastblock
                            + ") since the file (=" + iFile.getLocalName()
                            + ") is not under construction");
                }
                return;
            }

            long recoveryId =
                    ((BlockInfoUnderConstruction)storedBlock).getBlockRecoveryId();
            if(recoveryId != newgenerationstamp) {
                throw new IOException("The recovery id " + newgenerationstamp
                        + " does not match current recovery id "
                        + recoveryId + " for block " + lastblock);
            }

            INodeFileUnderConstruction pendingFile = (INodeFileUnderConstruction)iFile;

            if (deleteblock) {
                Block blockToDel = ExtendedBlock.getLocalBlock(lastblock);
                boolean remove = pendingFile.removeLastBlock(blockToDel);
                if (remove) {
                    blockManager.removeBlockFromMap(storedBlock);
                }
            }
            else {
                // update last block
                storedBlock.setGenerationStamp(newgenerationstamp);
                storedBlock.setNumBytes(newlength);

                // find the DatanodeDescriptor objects
                // There should be no locations in the blockManager till now because the
                // file is underConstruction
                List<DatanodeDescriptor> targetList =
                        new ArrayList<DatanodeDescriptor>(newtargets.length);
                if (newtargets.length > 0) {
                    for (DatanodeID newtarget : newtargets) {
                        // try to get targetNode
                        DatanodeDescriptor targetNode =
                                blockManager.getDatanodeManager().getDatanode(newtarget);
                        if (targetNode != null)
                            targetList.add(targetNode);
                        else if (LOG.isDebugEnabled()) {
                            LOG.debug("DatanodeDescriptor (=" + newtarget + ") not found");
                        }
                    }
                }
                if ((closeFile) && !targetList.isEmpty()) {
                    // the file is getting closed. Insert block locations into blockManager.
                    // Otherwise fsck will report these blocks as MISSING, especially if the
                    // blocksReceived from Datanodes take a long time to arrive.
                    for (DatanodeDescriptor targetNode : targetList) {
                        targetNode.addBlock(storedBlock);
                    }
                }
                // add pipeline locations into the INodeUnderConstruction
                DatanodeDescriptor[] targetArray =
                        new DatanodeDescriptor[targetList.size()];
                pendingFile.setLastBlock(storedBlock, targetList.toArray(targetArray));
            }

            if (closeFile) {
                src = closeFileCommitBlocks(pendingFile, storedBlock);
            } else {
                // If this commit does not want to close the file, persist blocks
                src = persistBlocks(pendingFile, false);
            }
        } finally {
            writeUnlock();
        }
        getEditLog().logSync();
        if (closeFile) {
            LOG.info("commitBlockSynchronization(newblock=" + lastblock
                    + ", file=" + src
                    + ", newgenerationstamp=" + newgenerationstamp
                    + ", newlength=" + newlength
                    + ", newtargets=" + Arrays.asList(newtargets) + ") successful");
        } else {
            LOG.info("commitBlockSynchronization(" + lastblock + ") successful");
        }
    }
    String persistBlocks(INodeFileUnderConstruction pendingFile,
                         boolean logRetryCache) throws IOException {
        String src = leaseManager.findPath(pendingFile);
        dir.persistBlocks(src, pendingFile, logRetryCache);
        return src;
    }
    String closeFileCommitBlocks(INodeFileUnderConstruction pendingFile,
                                 BlockInfo storedBlock)
            throws IOException {

        String src = leaseManager.findPath(pendingFile);

        // commit the last block and complete it if it has minimum replicas
        commitOrCompleteLastBlock(pendingFile, storedBlock);

        //remove lease, close file
        finalizeINodeFileUnderConstruction(src, pendingFile,
                Snapshot.findLatestSnapshot(pendingFile, null));

        return src;
    }
  public   boolean renameTo(String src, String dst)
            throws IOException, UnresolvedLinkException {
        RetryCache.CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return true; // Return previous response
        }
        boolean ret = false;
        try {
            ret = renameToInt(src, dst, cacheEntry != null);
        } catch (AccessControlException e) {
            logAuditEvent(false, "rename", src, dst, null);
            throw e;
        } finally {
            RetryCache.setState(cacheEntry, ret);
        }
        return ret;
    }

    private boolean renameToInt(String src, String dst, boolean logRetryCache)
            throws IOException, UnresolvedLinkException {
        if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* NameSystem.renameTo: " + src +
                    " to " + dst);
        }
        if (!DFSUtil.isValidName(dst)) {
            throw new IOException("Invalid name: " + dst);
        }
        FSPermissionChecker pc = getPermissionChecker();
        checkOperation(NameNode.OperationCategory.WRITE);
        byte[][] srcComponents = FileSystemDirectory.getPathComponentsForReservedPath(src);
        byte[][] dstComponents = FileSystemDirectory.getPathComponentsForReservedPath(dst);
        boolean status = false;
        HdfsFileStatus resultingStat = null;
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.WRITE);
            if (isInSafeMode()) {
               // throw new SafeModeException("Cannot rename " + src, safeMode);
            }
            src = FileSystemDirectory.resolvePath(src, srcComponents, dir);
            dst = FileSystemDirectory.resolvePath(dst, dstComponents, dir);
            checkOperation(NameNode.OperationCategory.WRITE);
            status = renameToInternal(pc, src, dst, logRetryCache);
            if (status) {
                resultingStat = getAuditFileInfo(dst, false);
            }
        } finally {
            writeUnlock();
        }
        getEditLog().logSync();
        if (status) {
            logAuditEvent(true, "rename", src, dst, resultingStat);
        }
        return status;
    }

    private boolean renameToInternal(FSPermissionChecker pc, String src,
                                     String dst, boolean logRetryCache) throws IOException,
            UnresolvedLinkException {
        assert hasWriteLock();
        if (isPermissionEnabled) {
            //We should not be doing this.  This is move() not renameTo().
            //but for now,
            //NOTE: yes, this is bad!  it's assuming much lower level behavior
            //      of rewriting the dst
            String actualdst = dir.isDir(dst)?
                    dst + Path.SEPARATOR + new Path(src).getName(): dst;
            checkParentAccess(pc, src, FsAction.WRITE);
            checkAncestorAccess(pc, actualdst, FsAction.WRITE);
        }

        if (dir.renameTo(src, dst, logRetryCache)) {
            return true;
        }
        return false;
    }

    private void checkParentAccess(FSPermissionChecker pc,
                                   String path, FsAction access) throws AccessControlException,
            UnresolvedLinkException {
        checkPermission(pc, path, false, null, access, null, null);
    }
    void unprotectedChangeLease(String src, String dst) {
        assert hasWriteLock();
        leaseManager.changeLease(src, dst);
    }
   public void concat(String target, String [] srcs)
            throws IOException, UnresolvedLinkException {
        RetryCache.CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return; // Return previous response
        }

        // Either there is no previous request in progres or it has failed
        if(FSNamesystem.LOG.isDebugEnabled()) {
            FSNamesystem.LOG.debug("concat " + Arrays.toString(srcs) +
                    " to " + target);
        }

        boolean success = false;
        try {
            concatInt(target, srcs, cacheEntry != null);
            success = true;
        } catch (AccessControlException e) {
            logAuditEvent(false, "concat", Arrays.toString(srcs), target, null);
            throw e;
        } finally {
            RetryCache.setState(cacheEntry, success);
        }
    }


    private void concatInt(String target, String [] srcs,
                           boolean logRetryCache) throws IOException, UnresolvedLinkException {
        // verify args
        if(target.isEmpty()) {
            throw new IllegalArgumentException("Target file name is empty");
        }
        if(srcs == null || srcs.length == 0) {
            throw new IllegalArgumentException("No sources given");
        }

        // We require all files be in the same directory
        String trgParent =
                target.substring(0, target.lastIndexOf(Path.SEPARATOR_CHAR));
        for (String s : srcs) {
            String srcParent = s.substring(0, s.lastIndexOf(Path.SEPARATOR_CHAR));
            if (!srcParent.equals(trgParent)) {
                throw new IllegalArgumentException(
                        "Sources and target are not in the same directory");
            }
        }

        HdfsFileStatus resultingStat = null;
        FSPermissionChecker pc = getPermissionChecker();
        checkOperation(NameNode.OperationCategory.WRITE);
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.WRITE);
            if (isInSafeMode()) {
              //  throw new SafeModeException("Cannot concat " + target, safeMode);
            }
            concatInternal(pc, target, srcs, logRetryCache);
            resultingStat = getAuditFileInfo(target, false);
        } finally {
            writeUnlock();
        }
        getEditLog().logSync();
        logAuditEvent(true, "concat", Arrays.toString(srcs), target, resultingStat);
    }

    private void concatInternal(FSPermissionChecker pc, String target,
                                String[] srcs, boolean logRetryCache) throws IOException,
            UnresolvedLinkException {
        assert hasWriteLock();

        // write permission for the target
        if (isPermissionEnabled) {
            checkPathAccess(pc, target, FsAction.WRITE);

            // and srcs
            for(String aSrc: srcs) {
                checkPathAccess(pc, aSrc, FsAction.READ); // read the file
                checkParentAccess(pc, aSrc, FsAction.WRITE); // for delete
            }
        }

        // to make sure no two files are the same
        Set<INode> si = new HashSet<INode>();

        // we put the following prerequisite for the operation
        // replication and blocks sizes should be the same for ALL the blocks

        // check the target
        final INodeFile trgInode = INodeFile.valueOf(dir.getINode4Write(target),
                target);
        if(trgInode.isUnderConstruction()) {
            throw new HadoopIllegalArgumentException("concat: target file "
                    + target + " is under construction");
        }
        // per design target shouldn't be empty and all the blocks same size
        if(trgInode.numBlocks() == 0) {
            throw new HadoopIllegalArgumentException("concat: target file "
                    + target + " is empty");
        }
        if (trgInode instanceof INodeFileWithSnapshot) {
            throw new HadoopIllegalArgumentException("concat: target file "
                    + target + " is in a snapshot");
        }

        long blockSize = trgInode.getPreferredBlockSize();

        // check the end block to be full
        final BlockInfo last = trgInode.getLastBlock();
        if(blockSize != last.getNumBytes()) {
            throw new HadoopIllegalArgumentException("The last block in " + target
                    + " is not full; last block size = " + last.getNumBytes()
                    + " but file block size = " + blockSize);
        }

        si.add(trgInode);
        final short repl = trgInode.getFileReplication();

        // now check the srcs
        boolean endSrc = false; // final src file doesn't have to have full end block
        for(int i=0; i<srcs.length; i++) {
            String src = srcs[i];
            if(i==srcs.length-1)
                endSrc=true;

            final INodeFile srcInode = INodeFile.valueOf(dir.getINode4Write(src), src);
            if(src.isEmpty()
                    || srcInode.isUnderConstruction()
                    || srcInode.numBlocks() == 0) {
                throw new HadoopIllegalArgumentException("concat: source file " + src
                        + " is invalid or empty or underConstruction");
            }

            // check replication and blocks size
            if(repl != srcInode.getBlockReplication()) {
                throw new HadoopIllegalArgumentException("concat: the soruce file "
                        + src + " and the target file " + target
                        + " should have the same replication: source replication is "
                        + srcInode.getBlockReplication()
                        + " but target replication is " + repl);
            }

            //boolean endBlock=false;
            // verify that all the blocks are of the same length as target
            // should be enough to check the end blocks
            final BlockInfo[] srcBlocks = srcInode.getBlocks();
            int idx = srcBlocks.length-1;
            if(endSrc)
                idx = srcBlocks.length-2; // end block of endSrc is OK not to be full
            if(idx >= 0 && srcBlocks[idx].getNumBytes() != blockSize) {
                throw new HadoopIllegalArgumentException("concat: the soruce file "
                        + src + " and the target file " + target
                        + " should have the same blocks sizes: target block size is "
                        + blockSize + " but the size of source block " + idx + " is "
                        + srcBlocks[idx].getNumBytes());
            }

            si.add(srcInode);
        }

        // make sure no two files are the same
        if(si.size() < srcs.length+1) { // trg + srcs
            // it means at least two files are the same
            throw new HadoopIllegalArgumentException(
                    "concat: at least two of the source files are the same");
        }

        if(NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* NameSystem.concat: " +
                    Arrays.toString(srcs) + " to " + target);
        }

        dir.concat(target,srcs, logRetryCache);
    }

  public   void renameTo(String src, String dst, Options.Rename... options)
            throws IOException, UnresolvedLinkException {
        if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* NameSystem.renameTo: with options - "
                    + src + " to " + dst);
        }
        if (!DFSUtil.isValidName(dst)) {
            throw new InvalidPathException("Invalid name: " + dst);
        }
        final FSPermissionChecker pc = getPermissionChecker();

        checkOperation(NameNode.OperationCategory.WRITE);
        RetryCache.CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return; // Return previous response
        }
        byte[][] srcComponents = FileSystemDirectory.getPathComponentsForReservedPath(src);
        byte[][] dstComponents = FileSystemDirectory.getPathComponentsForReservedPath(dst);
        HdfsFileStatus resultingStat = null;
        boolean success = false;
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.WRITE);
            if (isInSafeMode()) {
               // throw new SafeModeException("Cannot rename " + src, safeMode);
            }
            src =FileSystemDirectory.resolvePath(src, srcComponents, dir);
            dst = FileSystemDirectory.resolvePath(dst, dstComponents, dir);
            renameToInternal(pc, src, dst, cacheEntry != null, options);
            resultingStat = getAuditFileInfo(dst, false);
            success = true;
        } finally {
            writeUnlock();
            RetryCache.setState(cacheEntry, success);
        }
        getEditLog().logSync();
        if (resultingStat != null) {
            StringBuilder cmd = new StringBuilder("rename options=");
            for (Options.Rename option : options) {
                cmd.append(option.value()).append(" ");
            }
            logAuditEvent(true, cmd.toString(), src, dst, resultingStat);
        }
    }
    private void renameToInternal(FSPermissionChecker pc, String src, String dst,
                                  boolean logRetryCache, Options.Rename... options) throws IOException {
        assert hasWriteLock();
        if (isPermissionEnabled) {
            checkParentAccess(pc, src, FsAction.WRITE);
            checkAncestorAccess(pc, dst, FsAction.WRITE);
        }

        dir.renameTo(src, dst, logRetryCache, options);
    }
    /**
     * Create all the necessary directories
     */
   public boolean mkdirs(String src, PermissionStatus permissions,
                   boolean createParent) throws IOException, UnresolvedLinkException {
        boolean ret = false;
        try {
            ret = mkdirsInt(src, permissions, createParent);
        } catch (AccessControlException e) {
            logAuditEvent(false, "mkdirs", src);
            throw e;
        }
        return ret;
    }

    private boolean mkdirsInt(String src, PermissionStatus permissions,
                              boolean createParent) throws IOException, UnresolvedLinkException {
        if(NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* NameSystem.mkdirs: " + src);
        }
        if (!DFSUtil.isValidName(src)) {
            throw new InvalidPathException(src);
        }
        FSPermissionChecker pc = getPermissionChecker();
        checkOperation(NameNode.OperationCategory.WRITE);
        byte[][] pathComponents = FileSystemDirectory.getPathComponentsForReservedPath(src);
        HdfsFileStatus resultingStat = null;
        boolean status = false;
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.WRITE);
            if (isInSafeMode()) {
                //throw new SafeModeException("Cannot create directory " + src, safeMode);
            }
            src =  FileSystemDirectory.resolvePath(src, pathComponents, dir);
            status = mkdirsInternal(pc, src, permissions, createParent);
            if (status) {
                resultingStat = dir.getFileInfo(src, false);
            }
        } finally {
            writeUnlock();
        }
        getEditLog().logSync();
        if (status) {
            logAuditEvent(true, "mkdirs", src, null, resultingStat);
        }
        return status;
    }


    /**
     * Create all the necessary directories
     */
    private boolean mkdirsInternal(FSPermissionChecker pc, String src,
                                   PermissionStatus permissions, boolean createParent)
            throws IOException, UnresolvedLinkException {
        assert hasWriteLock();
        if (isPermissionEnabled) {
            checkTraverse(pc, src);
        }
        if (dir.isDirMutable(src)) {
            // all the users of mkdirs() are used to expect 'true' even if
            // a new directory is not created.
            return true;
        }
        if (isPermissionEnabled) {
            checkAncestorAccess(pc, src, FsAction.WRITE);
        }
        if (!createParent) {
            verifyParentDir(src);
        }

        // validate that we have enough inodes. This is, at best, a
        // heuristic because the mkdirs() operation might need to
        // create multiple inodes.
        checkFsObjectLimit();

        if (!dir.mkdirs(src, permissions, false, now())) {
            throw new IOException("Failed to create directory: " + src);
        }
        return true;
    }
    private void checkTraverse(FSPermissionChecker pc, String path)
            throws AccessControlException, UnresolvedLinkException {
        checkPermission(pc, path, false, null, null, null, null);
    }

  public   DirectoryListing getListing(String src, byte[] startAfter,
                                boolean needLocation)
            throws AccessControlException, UnresolvedLinkException, IOException {
        try {
            return getListingInt(src, startAfter, needLocation);
        } catch (AccessControlException e) {
            logAuditEvent(false, "listStatus", src);
            throw e;
        }
    }
    private DirectoryListing getListingInt(String src, byte[] startAfter,
                                           boolean needLocation)
            throws AccessControlException, UnresolvedLinkException, IOException {
        DirectoryListing dl;
        FSPermissionChecker pc = getPermissionChecker();
        checkOperation(NameNode.OperationCategory.READ);
        byte[][] pathComponents = FileSystemDirectory.getPathComponentsForReservedPath(src);
        readLock();
        try {
            checkOperation(NameNode.OperationCategory.READ);
            src = FileSystemDirectory.resolvePath(src, pathComponents, dir);

            if (isPermissionEnabled) {
                if (dir.isDir(src)) {
                    checkPathAccess(pc, src, FsAction.READ_EXECUTE);
                } else {
                    checkTraverse(pc, src);
                }
            }
            logAuditEvent(true, "listStatus", src);
            dl = dir.getListing(src, startAfter, needLocation);
        } finally {
            readUnlock();
        }
        return dl;
    }
    public SnapshottableDirectoryStatus[] getSnapshottableDirListing()
            throws IOException {
        SnapshottableDirectoryStatus[] status = null;
        final FSPermissionChecker checker = getPermissionChecker();
        readLock();
        try {
            checkOperation(NameNode.OperationCategory.READ);
            final String user = checker.isSuperUser()? null : checker.getUser();
            status = snapshotManager.getSnapshottableDirListing(user);
        } finally {
            readUnlock();
        }
        if (auditLog.isInfoEnabled() && isExternalInvocation()) {
            logAuditEvent(true, "listSnapshottableDirectory", null, null, null);
        }
        return status;
    }
    public void renewLease(String holder) throws IOException {
        checkOperation(NameNode.OperationCategory.WRITE);
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.WRITE);
            if (isInSafeMode()) {
                //throw new SafeModeException("Cannot renew lease for " + holder, safeMode);
            }
            leaseManager.renewLease(holder);
        } finally {
            writeUnlock();
        }
    }


 public    boolean recoverLease(String src, String holder, String clientMachine)
            throws IOException {
        if (!DFSUtil.isValidName(src)) {
            throw new IOException("Invalid file name: " + src);
        }

        boolean skipSync = false;
        FSPermissionChecker pc = getPermissionChecker();
        checkOperation(NameNode.OperationCategory.WRITE);
        byte[][] pathComponents = FileSystemDirectory.getPathComponentsForReservedPath(src);
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.WRITE);
            if (isInSafeMode()) {
          /*      throw new SafeModeException(
                        "Cannot recover the lease of " + src, safeMode);*/
            }
            src = FileSystemDirectory.resolvePath(src, pathComponents, dir);
            final INodeFile inode = INodeFile.valueOf(dir.getINode(src), src);
            if (!inode.isUnderConstruction()) {
                return true;
            }
            if (isPermissionEnabled) {
                checkPathAccess(pc, src, FsAction.WRITE);
            }

            recoverLeaseInternal(inode, src, holder, clientMachine, true);
        } catch (StandbyException se) {
            skipSync = true;
            throw se;
        } finally {
            writeUnlock();
            // There might be transactions logged while trying to recover the lease.
            // They need to be sync'ed even when an exception was thrown.
            if (!skipSync) {
                getEditLog().logSync();
            }
        }
        return false;
    }
  public   long[] getStats() {
        final long[] stats = datanodeStatistics.getStats();
        stats[ClientProtocol.GET_STATS_UNDER_REPLICATED_IDX] = getUnderReplicatedBlocks();
        stats[ClientProtocol.GET_STATS_CORRUPT_BLOCKS_IDX] = getCorruptReplicaBlocks();
        stats[ClientProtocol.GET_STATS_MISSING_BLOCKS_IDX] = getMissingBlocksCount();
        return stats;
    }
    public long getCorruptReplicaBlocks() {
        return blockManager.getCorruptReplicaBlocksCount();
    }
    public long getMissingBlocksCount() {
        // not locking
        return blockManager.getMissingBlocksCount();
    }
    int getNumberOfDatanodes(HdfsConstants.DatanodeReportType type) {
        readLock();
        try {
            return getBlockManager().getDatanodeManager().getDatanodeListForReport(
                    type).size();
        } finally {
            readUnlock();
        }
    }
 public    DatanodeInfo[] datanodeReport(final HdfsConstants.DatanodeReportType type
    ) throws AccessControlException, StandbyException {
        checkSuperuserPrivilege();
        checkOperation(NameNode.OperationCategory.UNCHECKED);
        readLock();
        try {
            checkOperation(NameNode.OperationCategory.UNCHECKED);
            final FileDatanodeManager dm = getBlockManager().getDatanodeManager();
            final List<DatanodeDescriptor> results = dm.getDatanodeListForReport(type);

            DatanodeInfo[] arr = new DatanodeInfo[results.size()];
            for (int i=0; i<arr.length; i++) {
                arr[i] = new DatanodeInfo(results.get(i));
            }
            return arr;
        } finally {
            readUnlock();
        }
    }

  public   long getPreferredBlockSize(String filename)
            throws IOException, UnresolvedLinkException {
        FSPermissionChecker pc = getPermissionChecker();
        checkOperation(NameNode.OperationCategory.READ);
        byte[][] pathComponents = FileSystemDirectory.getPathComponentsForReservedPath(filename);
        readLock();
        try {
            checkOperation(NameNode.OperationCategory.READ);
            filename = FileSystemDirectory.resolvePath(filename, pathComponents, dir);
            if (isPermissionEnabled) {
                checkTraverse(pc, filename);
            }
            return dir.getPreferredBlockSize(filename);
        } finally {
            readUnlock();
        }
    }
  public   boolean setSafeMode(HdfsConstants.SafeModeAction action) throws IOException {
        if (action != HdfsConstants.SafeModeAction.SAFEMODE_GET) {
            checkSuperuserPrivilege();
            switch(action) {
                case SAFEMODE_LEAVE: // leave safe mode
                    leaveSafeMode();
                    break;
                case SAFEMODE_ENTER: // enter safe mode
                    enterSafeMode(false);
                    break;
                default:
                    LOG.error("Unexpected safe mode action");
            }
        }
        return isInSafeMode();
    }
    void leaveSafeMode() {
        writeLock();
        try {
            if (!isInSafeMode()) {
                NameNode.stateChangeLog.info("STATE* Safe mode is already OFF");
                return;
            }
            safeMode.leave();
        } finally {
            writeUnlock();
        }
    }

    void enterSafeMode(boolean resourcesLow) throws IOException {
        writeLock();
        try {
            // Stop the secret manager, since rolling the master key would
            // try to write to the edit log
            stopSecretManager();

            // Ensure that any concurrent operations have been fully synced
            // before entering safe mode. This ensures that the FSImage
            // is entirely stable on disk as soon as we're in safe mode.
            boolean isEditlogOpenForWrite = getEditLog().isOpenForWrite();
            // Before Editlog is in OpenForWrite mode, editLogStream will be null. So,
            // logSyncAll call can be called only when Edlitlog is in OpenForWrite mode
            if (isEditlogOpenForWrite) {
                getEditLog().logSyncAll();
            }
            if (!isInSafeMode()) {
                safeMode = new SafeModeInfo(resourcesLow, isPopulatingReplQueues());
                return;
            }
            if (resourcesLow) {
                safeMode.setResourcesLow();
            } else {
                safeMode.setManual();
            }
            if (isEditlogOpenForWrite) {
                getEditLog().logSyncAll();
            }
            NameNode.stateChangeLog.info("STATE* Safe mode is ON"
                    + safeMode.getTurnOffTip());
        } finally {
            writeUnlock();
        }
    }
    private void stopSecretManager() {
        if (dtSecretManager != null) {
            dtSecretManager.stopThreads();
        }
    }

  public   void saveNamespace() throws AccessControlException, IOException {


        checkOperation(NameNode.OperationCategory.UNCHECKED);
        checkSuperuserPrivilege();

        RetryCache.CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return; // Return previous response
        }
        boolean success = false;
        readLock();
        try {
            checkOperation(NameNode.OperationCategory.UNCHECKED);
            if (!isInSafeMode()) {
                throw new IOException("Safe mode should be turned ON " +
                        "in order to create namespace image.");
            }
            getFSImage().saveNamespace(this);
            success = true;
        } finally {
            readUnlock();
            RetryCache.setState(cacheEntry, success);
        }
        LOG.info("New namespace image has been created");
    }

  public   boolean restoreFailedStorage(String arg) throws AccessControlException,
            StandbyException {
        checkSuperuserPrivilege();
        checkOperation(NameNode.OperationCategory.UNCHECKED);
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.UNCHECKED);

            // if it is disabled - enable it and vice versa.
            if(arg.equals("check"))
                return getFSImage().getStorage().getRestoreFailedStorage();

            boolean val = arg.equals("true");  // false if not
            getFSImage().getStorage().setRestoreFailedStorage(val);

            return val;
        } finally {
            writeUnlock();
        }
    }
  public   void refreshNodes() throws IOException {
        checkOperation(NameNode.OperationCategory.UNCHECKED);
        checkSuperuserPrivilege();
        getBlockManager().getDatanodeManager().refreshNodes(new HdfsConfiguration());
    }
  public   void finalizeUpgrade() throws IOException {
        checkSuperuserPrivilege();
        checkOperation(NameNode.OperationCategory.WRITE);
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.WRITE);
            getFSImage().finalizeUpgrade();
        } finally {
            writeUnlock();
        }
    }

    public static class CorruptFileBlockInfo {
     public    String path;
        Block block;

        public CorruptFileBlockInfo(String p, Block b) {
            path = p;
            block = b;
        }

        @Override
        public String toString() {
            return block.getBlockName() + "\t" + path;
        }
    }
    /**
     * @param path Restrict corrupt files to this portion of namespace.
     *  back is ordered by blockid; startBlockAfter tells where to start from
     * @return a list in which each entry describes a corrupt file/block
     * @throws AccessControlException
     * @throws IOException
     */
  public   Collection<CorruptFileBlockInfo> listCorruptFileBlocks(String path,
                                                                        String[] cookieTab) throws IOException {
        checkSuperuserPrivilege();
        checkOperation(NameNode.OperationCategory.READ);
        readLock();
        try {
            checkOperation(NameNode.OperationCategory.READ);
            if (!isPopulatingReplQueues()) {
                throw new IOException("Cannot run listCorruptFileBlocks because " +
                        "replication queues have not been initialized.");
            }
            // print a limited # of corrupt files per call
            int count = 0;
            ArrayList<CorruptFileBlockInfo> corruptFiles = new ArrayList<CorruptFileBlockInfo>();

            final Iterator<Block> blkIterator = blockManager.getCorruptReplicaBlockIterator();

            if (cookieTab == null) {
                cookieTab = new String[] { null };
            }
            int skip = getIntCookie(cookieTab[0]);
            for (int i = 0; i < skip && blkIterator.hasNext(); i++) {
                blkIterator.next();
            }

            while (blkIterator.hasNext()) {
                Block blk = blkIterator.next();
                final INode inode = (INode)blockManager.getBlockCollection(blk);
                skip++;
                if (inode != null && blockManager.countNodes(blk).liveReplicas() == 0) {
                    String src = FileSystemDirectory.getFullPathName(inode);
                    if (src.startsWith(path)){
                        corruptFiles.add(new CorruptFileBlockInfo(src, blk));
                        count++;
                        if (count >= DEFAULT_MAX_CORRUPT_FILEBLOCKS_RETURNED)
                            break;
                    }
                }
            }
            cookieTab[0] = String.valueOf(skip);
            LOG.info("list corrupt file blocks returned: " + count);
            return corruptFiles;
        } finally {
            readUnlock();
        }
    }
    /**
     * Convert string cookie to integer.
     */
    private static int getIntCookie(String cookie){
        int c;
        if(cookie == null){
            c = 0;
        } else {
            try{
                c = Integer.parseInt(cookie);
            }catch (NumberFormatException e) {
                c = 0;
            }
        }
        c = Math.max(0, c);
        return c;
    }

    /**
     * Dump all metadata into specified file
     */
  public   void metaSave(String filename) throws IOException {
        checkSuperuserPrivilege();
        checkOperation(NameNode.OperationCategory.UNCHECKED);
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.UNCHECKED);
            File file = new File(System.getProperty("hadoop.log.dir"), filename);
            PrintWriter out = new PrintWriter(new BufferedWriter(
                    new OutputStreamWriter(new FileOutputStream(file), Charsets.UTF_8)));
            metaSave(out);
            out.flush();
            out.close();
        } finally {
            writeUnlock();
        }
    }

    private void metaSave(PrintWriter out) {
        assert hasWriteLock();
        long totalInodes = this.dir.totalInodes();
        long totalBlocks = this.getBlocksTotal();
        out.println(totalInodes + " files and directories, " + totalBlocks
                + " blocks = " + (totalInodes + totalBlocks) + " total");

        blockManager.metaSave(out);
    }
  public   void setBalancerBandwidth(long bandwidth) throws IOException {
        checkOperation(NameNode.OperationCategory.UNCHECKED);
        checkSuperuserPrivilege();
        getBlockManager().getDatanodeManager().setBalancerBandwidth(bandwidth);
    }

    /**
     * Get the file info for a specific file.
     *
     * @param src The string representation of the path to the file
     * @param resolveLink whether to throw UnresolvedLinkException
     *        if src refers to a symlink
     *
     * @throws AccessControlException if access is denied
     * @throws UnresolvedLinkException if a symlink is encountered.
     *
     * @return object containing information regarding the file
     *         or null if file not found
     * @throws StandbyException
     */
 public    HdfsFileStatus getFileInfo(String src, boolean resolveLink)
            throws AccessControlException, UnresolvedLinkException,
            StandbyException, IOException {
        if (!DFSUtil.isValidName(src)) {
            throw new InvalidPathException("Invalid file name: " + src);
        }
        HdfsFileStatus stat = null;
        FSPermissionChecker pc = getPermissionChecker();
        checkOperation(NameNode.OperationCategory.READ);
        if (!DFSUtil.isValidName(src)) {
            throw new InvalidPathException("Invalid file name: " + src);
        }
        byte[][] pathComponents = FileSystemDirectory.getPathComponentsForReservedPath(src);
        readLock();
        try {
            checkOperation(NameNode.OperationCategory.READ);
            src = FileSystemDirectory.resolvePath(src, pathComponents, dir);
            if (isPermissionEnabled) {
                checkTraverse(pc, src);
            }
            stat = dir.getFileInfo(src, resolveLink);
        } catch (AccessControlException e) {
            logAuditEvent(false, "getfileinfo", src);
            throw e;
        } finally {
            readUnlock();
        }
        logAuditEvent(true, "getfileinfo", src);
        return stat;
    }
    /**
     * Returns true if the file is closed
     */
  public   boolean isFileClosed(String src)
            throws AccessControlException, UnresolvedLinkException,
            StandbyException, IOException {
        FSPermissionChecker pc = getPermissionChecker();
        checkOperation(NameNode.OperationCategory.READ);
        readLock();
        try {
            checkOperation(NameNode.OperationCategory.READ);
            if (isPermissionEnabled) {
                checkTraverse(pc, src);
            }
            return !INodeFile.valueOf(dir.getINode(src), src).isUnderConstruction();
        } catch (AccessControlException e) {
            if (isAuditEnabled() && isExternalInvocation()) {
                logAuditEvent(false, "isFileClosed", src);
            }
            throw e;
        } finally {
            readUnlock();
        }
    }

  public   ContentSummary getContentSummary(String src) throws AccessControlException,
            FileNotFoundException, UnresolvedLinkException, StandbyException {
        FSPermissionChecker pc = getPermissionChecker();
        checkOperation(NameNode.OperationCategory.READ);
        byte[][] pathComponents = FileSystemDirectory.getPathComponentsForReservedPath(src);
        readLock();
        try {
            checkOperation(NameNode.OperationCategory.READ);
            src = FileSystemDirectory.resolvePath(src, pathComponents, dir);
            if (isPermissionEnabled) {
                checkPermission(pc, src, false, null, null, null, FsAction.READ_EXECUTE);
            }
            return dir.getContentSummary(src);
        } finally {
            readUnlock();
        }
    }

    /**
     * Set the namespace quota and diskspace quota for a directory.
     * See {@link ClientProtocol#setQuota(String, long, long)} for the
     * contract.
     *
     * Note: This does not support ".inodes" relative path.
     */
   public void setQuota(String path, long nsQuota, long dsQuota)
            throws IOException, UnresolvedLinkException {
        checkSuperuserPrivilege();
        checkOperation(NameNode.OperationCategory.WRITE);
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.WRITE);
            if (isInSafeMode()) {
                //throw new SafeModeException("Cannot set quota on " + path, safeMode);
            }
            dir.setQuota(path, nsQuota, dsQuota);
        } finally {
            writeUnlock();
        }
        getEditLog().logSync();
    }

    /** Persist all metadata about this file.
     * @param src The string representation of the path
     * @param clientName The string representation of the client
     * @param lastBlockLength The length of the last block
     *                        under construction reported from client.
     * @throws IOException if path does not exist
     */
   public void fsync(String src, String clientName, long lastBlockLength)
            throws IOException, UnresolvedLinkException {
        NameNode.stateChangeLog.info("BLOCK* fsync: " + src + " for " + clientName);
        checkOperation(NameNode.OperationCategory.WRITE);
        byte[][] pathComponents = FileSystemDirectory.getPathComponentsForReservedPath(src);
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.WRITE);
            if (isInSafeMode()) {
                //throw new SafeModeException("Cannot fsync file " + src, safeMode);
            }
            src = FileSystemDirectory.resolvePath(src, pathComponents, dir);
            INodeFileUnderConstruction pendingFile  = checkLease(src, clientName);
            if (lastBlockLength > 0) {
                pendingFile.updateLengthOfLastBlock(lastBlockLength);
            }
            dir.persistBlocks(src, pendingFile, false);
        } finally {
            writeUnlock();
        }
        getEditLog().logSync();
    }
    /**
     * stores the modification and access time for this inode.
     * The access time is precise upto an hour. The transaction, if needed, is
     * written to the edits log but is not flushed.
     */
   public void setTimes(String src, long mtime, long atime)
            throws IOException, UnresolvedLinkException {
        if (!isAccessTimeSupported() && atime != -1) {
            throw new IOException("Access time for hdfs is not configured. " +
                    " Please set " + DFS_NAMENODE_ACCESSTIME_PRECISION_KEY + " configuration parameter.");
        }
        try {
            setTimesInt(src, mtime, atime);
        } catch (AccessControlException e) {
            logAuditEvent(false, "setTimes", src);
            throw e;
        }
    }

    private void setTimesInt(String src, long mtime, long atime)
            throws IOException, UnresolvedLinkException {
        HdfsFileStatus resultingStat = null;
        FSPermissionChecker pc = getPermissionChecker();
        checkOperation(NameNode.OperationCategory.WRITE);
        byte[][] pathComponents = FileSystemDirectory.getPathComponentsForReservedPath(src);
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.WRITE);
            if (isInSafeMode()) {
               // throw new SafeModeException("Cannot set times " + src, safeMode);
            }
            src = FileSystemDirectory.resolvePath(src, pathComponents, dir);

            // Write access is required to set access and modification times
            if (isPermissionEnabled) {
                checkPathAccess(pc, src, FsAction.WRITE);
            }
            final FileINodesInPath iip = dir.getINodesInPath4Write(src);
            final INode inode = iip.getLastINode();
            if (inode != null) {
                dir.setTimes(src, inode, mtime, atime, true, iip.getLatestSnapshot());
                resultingStat = getAuditFileInfo(src, false);
            } else {
                throw new FileNotFoundException("File/Directory " + src + " does not exist.");
            }
        } finally {
            writeUnlock();
        }
        logAuditEvent(true, "setTimes", src, null, resultingStat);
    }

    /**
     * Create a symbolic link.
     */
    @SuppressWarnings("deprecation")
  public   void createSymlink(String target, String link,
                       PermissionStatus dirPerms, boolean createParent)
            throws IOException, UnresolvedLinkException {
        if (!FileSystem.isSymlinksEnabled()) {
            throw new UnsupportedOperationException("Symlinks not supported");
        }
        if (!DFSUtil.isValidName(link)) {
            throw new InvalidPathException("Invalid link name: " + link);
        }
        if (FSDirectory.isReservedName(target)) {
            throw new InvalidPathException("Invalid target name: " + target);
        }
        RetryCache.CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return; // Return previous response
        }
        boolean success = false;
        try {
            createSymlinkInt(target, link, dirPerms, createParent, cacheEntry != null);
            success = true;
        } catch (AccessControlException e) {
            logAuditEvent(false, "createSymlink", link, target, null);
            throw e;
        } finally {
            RetryCache.setState(cacheEntry, success);
        }
    }

    private void createSymlinkInt(String target, String link,
                                  PermissionStatus dirPerms, boolean createParent, boolean logRetryCache)
            throws IOException, UnresolvedLinkException {
        if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* NameSystem.createSymlink: target="
                    + target + " link=" + link);
        }
        HdfsFileStatus resultingStat = null;
        FSPermissionChecker pc = getPermissionChecker();
        checkOperation(NameNode.OperationCategory.WRITE);
        byte[][] pathComponents = FileSystemDirectory.getPathComponentsForReservedPath(link);
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.WRITE);
            if (isInSafeMode()) {
                //throw new SafeModeException("Cannot create symlink " + link, safeMode);
            }
            link = FileSystemDirectory.resolvePath(link, pathComponents, dir);
            if (!createParent) {
                verifyParentDir(link);
            }
            if (!dir.isValidToCreate(link)) {
                throw new IOException("failed to create link " + link
                        +" either because the filename is invalid or the file exists");
            }
            if (isPermissionEnabled) {
                checkAncestorAccess(pc, link, FsAction.WRITE);
            }
            // validate that we have enough inodes.
            checkFsObjectLimit();

            // add symbolic link to namespace
            dir.addSymlink(link, target, dirPerms, createParent, logRetryCache);
            resultingStat = getAuditFileInfo(link, false);
        } finally {
            writeUnlock();
        }
        getEditLog().logSync();
        logAuditEvent(true, "createSymlink", link, target, resultingStat);
    }
 public    LocatedBlock updateBlockForPipeline(ExtendedBlock block,
                                        String clientName) throws IOException {
        LocatedBlock locatedBlock;
        checkOperation(NameNode.OperationCategory.WRITE);
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.WRITE);

            // check vadility of parameters
            checkUCBlock(block, clientName);

            // get a new generation stamp and an access token
            block.setGenerationStamp(
                    nextGenerationStamp(isLegacyBlock(block.getLocalBlock())));
            locatedBlock = new LocatedBlock(block, new DatanodeInfo[0]);
            blockManager.setBlockToken(locatedBlock, BlockTokenSecretManager.AccessMode.WRITE);
        } finally {
            writeUnlock();
        }
        // Ensure we record the new generation stamp
        getEditLog().logSync();
        return locatedBlock;
    }
    private INodeFileUnderConstruction checkUCBlock(ExtendedBlock block,
                                                    String clientName) throws IOException {
        assert hasWriteLock();
        if (isInSafeMode()) {
/*            throw new SafeModeException("Cannot get a new generation stamp and an " +
                    "access token for block " + block, safeMode);*/
        }

        // check stored block state
        BlockInfo storedBlock = getStoredBlock(ExtendedBlock.getLocalBlock(block));
        if (storedBlock == null ||
                storedBlock.getBlockUCState() != HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION) {
            throw new IOException(block +
                    " does not exist or is not under Construction" + storedBlock);
        }

        // check file inode
        final INodeFile file = ((INode)storedBlock.getBlockCollection()).asFile();
        if (file==null || !file.isUnderConstruction()) {
            throw new IOException("The file " + storedBlock +
                    " belonged to does not exist or it is not under construction.");
        }

        // check lease
        INodeFileUnderConstruction pendingFile = (INodeFileUnderConstruction)file;
        if (clientName == null || !clientName.equals(pendingFile.getClientName())) {
            throw new LeaseExpiredException("Lease mismatch: " + block +
                    " is accessed by a non lease holder " + clientName);
        }

        return pendingFile;
    }
   public void updatePipeline(String clientName, ExtendedBlock oldBlock,
                        ExtendedBlock newBlock, DatanodeID[] newNodes)
            throws IOException {
        checkOperation(NameNode.OperationCategory.WRITE);
        RetryCache.CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return; // Return previous response
        }
        LOG.info("updatePipeline(block=" + oldBlock
                + ", newGenerationStamp=" + newBlock.getGenerationStamp()
                + ", newLength=" + newBlock.getNumBytes()
                + ", newNodes=" + Arrays.asList(newNodes)
                + ", clientName=" + clientName
                + ")");
        writeLock();
        boolean success = false;
        try {
            checkOperation(NameNode.OperationCategory.WRITE);
            if (isInSafeMode()) {
               // throw new SafeModeException("Pipeline not updated", safeMode);
            }
            assert newBlock.getBlockId()==oldBlock.getBlockId() : newBlock + " and "
                    + oldBlock + " has different block identifier";
            updatePipelineInternal(clientName, oldBlock, newBlock, newNodes,
                    cacheEntry != null);
            success = true;
        } finally {
            writeUnlock();
            RetryCache.setState(cacheEntry, success);
        }
        getEditLog().logSync();
        LOG.info("updatePipeline(" + oldBlock + ") successfully to " + newBlock);
    }
    private void updatePipelineInternal(String clientName, ExtendedBlock oldBlock,
                                        ExtendedBlock newBlock, DatanodeID[] newNodes, boolean logRetryCache)
            throws IOException {
        assert hasWriteLock();
        // check the vadility of the block and lease holder name
        final INodeFileUnderConstruction pendingFile
                = checkUCBlock(oldBlock, clientName);
        final BlockInfoUnderConstruction blockinfo
                = (BlockInfoUnderConstruction)pendingFile.getLastBlock();

        // check new GS & length: this is not expected
        if (newBlock.getGenerationStamp() <= blockinfo.getGenerationStamp() ||
                newBlock.getNumBytes() < blockinfo.getNumBytes()) {
            String msg = "Update " + oldBlock + " (len = " +
                    blockinfo.getNumBytes() + ") to an older state: " + newBlock +
                    " (len = " + newBlock.getNumBytes() +")";
            LOG.warn(msg);
            throw new IOException(msg);
        }

        // Update old block with the new generation stamp and new length
        blockinfo.setGenerationStamp(newBlock.getGenerationStamp());
        blockinfo.setNumBytes(newBlock.getNumBytes());

        // find the DatanodeDescriptor objects
        final FileDatanodeManager dm = getBlockManager().getDatanodeManager();
        DatanodeDescriptor[] descriptors = null;
        if (newNodes.length > 0) {
            descriptors = new DatanodeDescriptor[newNodes.length];
            for(int i = 0; i < newNodes.length; i++) {
                descriptors[i] = dm.getDatanode(newNodes[i]);
            }
        }
        blockinfo.setExpectedLocations(descriptors);

        String src = leaseManager.findPath(pendingFile);
        dir.persistBlocks(src, pendingFile, logRetryCache);
    }
  public   Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
            throws IOException {
        Token<DelegationTokenIdentifier> token;
        checkOperation(NameNode.OperationCategory.WRITE);
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.WRITE);
            if (isInSafeMode()) {
              //  throw new SafeModeException("Cannot issue delegation token", safeMode);
            }
            if (!isAllowedDelegationTokenOp()) {
                throw new IOException(
                        "Delegation Token can be issued only with kerberos or web authentication");
            }
            if (dtSecretManager == null || !dtSecretManager.isRunning()) {
                LOG.warn("trying to get DT with no secret manager running");
                return null;
            }

            UserGroupInformation ugi = getRemoteUser();
            String user = ugi.getUserName();
            Text owner = new Text(user);
            Text realUser = null;
            if (ugi.getRealUser() != null) {
                realUser = new Text(ugi.getRealUser().getUserName());
            }
            DelegationTokenIdentifier dtId = new DelegationTokenIdentifier(owner,
                    renewer, realUser);
            token = new Token<DelegationTokenIdentifier>(
                    dtId, dtSecretManager);
            long expiryTime = dtSecretManager.getTokenExpiryTime(dtId);
            getEditLog().logGetDelegationToken(dtId, expiryTime);
        } finally {
            writeUnlock();
        }
        getEditLog().logSync();
        return token;
    }

    private boolean isAllowedDelegationTokenOp() throws IOException {
        UserGroupInformation.AuthenticationMethod authMethod = getConnectionAuthenticationMethod();
        if (UserGroupInformation.isSecurityEnabled()
                && (authMethod != UserGroupInformation.AuthenticationMethod.KERBEROS)
                && (authMethod != UserGroupInformation.AuthenticationMethod.KERBEROS_SSL)
                && (authMethod != UserGroupInformation.AuthenticationMethod.CERTIFICATE)) {
            return false;
        }
        return true;
    }
    private UserGroupInformation.AuthenticationMethod getConnectionAuthenticationMethod()
            throws IOException {
        UserGroupInformation ugi = getRemoteUser();
        UserGroupInformation.AuthenticationMethod authMethod = ugi.getAuthenticationMethod();
        if (authMethod == UserGroupInformation.AuthenticationMethod.PROXY) {
            authMethod = ugi.getRealUser().getAuthenticationMethod();
        }
        return authMethod;
    }
   public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
            throws SecretManager.InvalidToken, IOException {
        long expiryTime;
        checkOperation(NameNode.OperationCategory.WRITE);
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.WRITE);

            if (isInSafeMode()) {
                //throw new SafeModeException("Cannot renew delegation token", safeMode);
            }
            if (!isAllowedDelegationTokenOp()) {
                throw new IOException(
                        "Delegation Token can be renewed only with kerberos or web authentication");
            }
            String renewer = getRemoteUser().getShortUserName();
            expiryTime = dtSecretManager.renewToken(token, renewer);
            DelegationTokenIdentifier id = new DelegationTokenIdentifier();
            ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
            DataInputStream in = new DataInputStream(buf);
            id.readFields(in);
            getEditLog().logRenewDelegationToken(id, expiryTime);
        } finally {
            writeUnlock();
        }
        getEditLog().logSync();
        return expiryTime;
    }

   public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
            throws IOException {
        checkOperation(NameNode.OperationCategory.WRITE);
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.WRITE);

            if (isInSafeMode()) {
             //   throw new SafeModeException("Cannot cancel delegation token", safeMode);
            }
            String canceller = getRemoteUser().getUserName();
            DelegationTokenIdentifier id = dtSecretManager
                    .cancelToken(token, canceller);
            getEditLog().logCancelDelegationToken(id);
        } finally {
            writeUnlock();
        }
        getEditLog().logSync();
    }
   public String createSnapshot(String snapshotRoot, String snapshotName)
            throws SafeModeException, IOException {
        checkOperation(NameNode.OperationCategory.WRITE);
        final FSPermissionChecker pc = getPermissionChecker();
        RetryCache.CacheEntryWithPayload cacheEntry = RetryCache.waitForCompletion(retryCache,
                null);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return (String) cacheEntry.getPayload();
        }
        writeLock();
        String snapshotPath = null;
        try {
            checkOperation(NameNode.OperationCategory.WRITE);
            if (isInSafeMode()) {
          /*      throw new SafeModeException("Cannot create snapshot for "
                        + snapshotRoot, safeMode);*/
            }
            if (isPermissionEnabled) {
                checkOwner(pc, snapshotRoot);
            }

            if (snapshotName == null || snapshotName.isEmpty()) {
                snapshotName = Snapshot.generateDefaultSnapshotName();
            }
            dir.verifySnapshotName(snapshotName, snapshotRoot);
            dir.writeLock();
            try {
                snapshotPath = snapshotManager.createSnapshot(snapshotRoot, snapshotName);
            } finally {
                dir.writeUnlock();
            }
            getEditLog().logCreateSnapshot(snapshotRoot, snapshotName,
                    cacheEntry != null);
        } finally {
            writeUnlock();
            RetryCache.setState(cacheEntry, snapshotPath != null, snapshotPath);
        }
        getEditLog().logSync();

        if (auditLog.isInfoEnabled() && isExternalInvocation()) {
            logAuditEvent(true, "createSnapshot", snapshotRoot, snapshotPath, null);
        }
        return snapshotPath;
    }
  public   void deleteSnapshot(String snapshotRoot, String snapshotName)
            throws SafeModeException, IOException {
        checkOperation(NameNode.OperationCategory.WRITE);
        final FSPermissionChecker pc = getPermissionChecker();

        RetryCache.CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return; // Return previous response
        }
        boolean success = false;
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.WRITE);
            if (isInSafeMode()) {
             /*   throw new SafeModeException(
                        "Cannot delete snapshot for " + snapshotRoot, safeMode);*/
            }
            if (isPermissionEnabled) {
                checkOwner(pc, snapshotRoot);
            }

            INode.BlocksMapUpdateInfo collectedBlocks = new INode.BlocksMapUpdateInfo();
            List<INode> removedINodes = new ArrayList<INode>();
            dir.writeLock();
            try {
                snapshotManager.deleteSnapshot(snapshotRoot, snapshotName,
                        collectedBlocks, removedINodes);
                dir.removeFromInodeMap(removedINodes);
            } finally {
                dir.writeUnlock();
            }
            removedINodes.clear();
            this.removeBlocks(collectedBlocks);
            collectedBlocks.clear();
            getEditLog().logDeleteSnapshot(snapshotRoot, snapshotName,
                    cacheEntry != null);
            success = true;
        } finally {
            writeUnlock();
            RetryCache.setState(cacheEntry, success);
        }
        getEditLog().logSync();

        if (auditLog.isInfoEnabled() && isExternalInvocation()) {
            String rootPath = Snapshot.getSnapshotPath(snapshotRoot, snapshotName);
            logAuditEvent(true, "deleteSnapshot", rootPath, null, null);
        }
    }


   public void renameSnapshot(String path, String snapshotOldName,
                        String snapshotNewName) throws SafeModeException, IOException {
        checkOperation(NameNode.OperationCategory.WRITE);
        final FSPermissionChecker pc = getPermissionChecker();
        RetryCache.CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return; // Return previous response
        }
        writeLock();
        boolean success = false;
        try {
            checkOperation(NameNode.OperationCategory.WRITE);
            if (isInSafeMode()) {
           /*     throw new SafeModeException("Cannot rename snapshot for " + path,
                        safeMode);*/
            }
            if (isPermissionEnabled) {
                checkOwner(pc, path);
            }
            dir.verifySnapshotName(snapshotNewName, path);

            snapshotManager.renameSnapshot(path, snapshotOldName, snapshotNewName);
            getEditLog().logRenameSnapshot(path, snapshotOldName, snapshotNewName,
                    cacheEntry != null);
            success = true;
        } finally {
            writeUnlock();
            RetryCache.setState(cacheEntry, success);
        }
        getEditLog().logSync();

        if (auditLog.isInfoEnabled() && isExternalInvocation()) {
            String oldSnapshotRoot = Snapshot.getSnapshotPath(path, snapshotOldName);
            String newSnapshotRoot = Snapshot.getSnapshotPath(path, snapshotNewName);
            logAuditEvent(true, "renameSnapshot", oldSnapshotRoot, newSnapshotRoot, null);
        }
    }

   public void allowSnapshot(String path) throws SafeModeException, IOException {
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.WRITE);
            if (isInSafeMode()) {
              /*  throw new SafeModeException("Cannot allow snapshot for " + path,
                        safeMode);*/
            }
            checkSuperuserPrivilege();

            dir.writeLock();
            try {
                snapshotManager.setSnapshottable(path, true);
            } finally {
                dir.writeUnlock();
            }
            getEditLog().logAllowSnapshot(path);
        } finally {
            writeUnlock();
        }
        getEditLog().logSync();

        if (auditLog.isInfoEnabled() && isExternalInvocation()) {
            logAuditEvent(true, "allowSnapshot", path, null, null);
        }
    }
   public void disallowSnapshot(String path) throws SafeModeException, IOException {
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.WRITE);
            if (isInSafeMode()) {
            /*    throw new SafeModeException("Cannot disallow snapshot for " + path,
                        safeMode);*/
            }
            checkSuperuserPrivilege();

            dir.writeLock();
            try {
                snapshotManager.resetSnapshottable(path);
            } finally {
                dir.writeUnlock();
            }
            getEditLog().logDisallowSnapshot(path);
        } finally {
            writeUnlock();
        }
        getEditLog().logSync();

        if (auditLog.isInfoEnabled() && isExternalInvocation()) {
            logAuditEvent(true, "disallowSnapshot", path, null, null);
        }
    }

   public SnapshotDiffReport getSnapshotDiffReport(String path,
                                             String fromSnapshot, String toSnapshot) throws IOException {
        FileINodeDirectorySnapshottable.SnapshotDiffInfo diffs = null;
        final FSPermissionChecker pc = getPermissionChecker();
        readLock();
        try {
            checkOperation(NameNode.OperationCategory.READ);
            if (isPermissionEnabled) {
          /*      checkSubtreeReadPermission(pc, path, fromSnapshot);
                checkSubtreeReadPermission(pc, path, toSnapshot);*/
            }
            diffs = snapshotManager.diff(path, fromSnapshot, toSnapshot);
        } finally {
            readUnlock();
        }

        if (auditLog.isInfoEnabled() && isExternalInvocation()) {
            logAuditEvent(true, "computeSnapshotDiff", null, null, null);
        }
        return diffs != null ? diffs.generateReport() : new SnapshotDiffReport(
                path, fromSnapshot, toSnapshot,
                Collections.<SnapshotDiffReport.DiffReportEntry> emptyList());
    }

   public void releaseBackupNode(NamenodeRegistration registration)
            throws IOException {
        checkOperation(NameNode.OperationCategory.WRITE);
        writeLock();
        try {
            checkOperation(NameNode.OperationCategory.WRITE);
            if(getFSImage().getStorage().getNamespaceID()
                    != registration.getNamespaceID())
                throw new IOException("Incompatible namespaceIDs: "
                        + " Namenode namespaceID = "
                        + getFSImage().getStorage().getNamespaceID() + "; "
                        + registration.getRole() +
                        " node namespaceID = " + registration.getNamespaceID());
            getEditLog().releaseBackupStream(registration);
        } finally {
            writeUnlock();
        }
    }
}
