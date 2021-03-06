package org.apache.hadoop.hdfs.server.namenode.test.hdfs.journal;

import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.fs.FileSystemEditLog;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.fs.RedundantFileEditLogInputStream;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;


import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.apache.hadoop.util.ExitUtil.terminate;

public class FileJournalSet implements JournalManager {
    int minimumRedundantJournals;
    Log LOG = LogFactory.getLog(FileSystemEditLog.class);
    private boolean disabled = false;
    private  JournalManager journal;

    private List<JournalAndStream> journals =
            new CopyOnWriteArrayList<JournalAndStream>();
    public static Comparator<EditLogInputStream>  EDIT_LOG_INPUT_STREAM_COMPARATOR = new Comparator<EditLogInputStream>() {
        @Override
        public int compare(EditLogInputStream a, EditLogInputStream b) {
            return ComparisonChain.start().
                    compare(a.getFirstTxId(), b.getFirstTxId()).
                    compare(b.getLastTxId(), a.getLastTxId()).
                    result();
        }
    };
   public FileJournalSet(int minimumRedundantResources) {
        this.minimumRedundantJournals = minimumRedundantResources;
    }
   public void add(JournalManager j, boolean required) {
        JournalAndStream jas = new JournalAndStream(j, required);
        journals.add(jas);
    }
    public boolean isEmpty() {
        return !NameNodeResourcePolicy.areResourcesAvailable(journals,
                minimumRedundantJournals);
    }
   public List<JournalManager> getJournalManagers() {
        List<JournalManager> jList = new ArrayList<JournalManager>();
        for (JournalAndStream j : journals) {
            jList.add(j.getManager());
        }
        return jList;
    }

    @Override
    public void format(NamespaceInfo ns) throws IOException {

    }
    public EditLogOutputStream startLogSegment(final long txId) throws IOException {
        mapJournalsAndReportErrors(new JournalClosure() {
            @Override
            public void apply(JournalAndStream jas) throws IOException {
                jas.startLogSegment(txId);
            }
        }, "starting log segment " + txId);
        return new FileJournalSet.JournalSetOutputStream();
    }

    @Override
    public void finalizeLogSegment(final long firstTxId, final long lastTxId) throws IOException {
        mapJournalsAndReportErrors(new JournalClosure() {
            @Override
            public void apply(JournalAndStream jas) throws IOException {
                if (jas.isActive()) {
                    jas.closeStream();
                    jas.getManager().finalizeLogSegment(firstTxId, lastTxId);
                }
            }
        }, "finalize log segment " + firstTxId + ", " + lastTxId);
    }

    @Override
    public void setOutputBufferCapacity(final int size) {
        try {
            mapJournalsAndReportErrors(new JournalClosure() {
                @Override
                public void apply(JournalAndStream jas) throws IOException {
                    jas.getManager().setOutputBufferCapacity(size);
                }
            }, "setOutputBufferCapacity");
        } catch (IOException e) {
            LOG.error("Error in setting outputbuffer capacity");
        }
    }

    @Override
    public void recoverUnfinalizedSegments() throws IOException {
        mapJournalsAndReportErrors(new JournalClosure() {
            @Override
            public void apply(JournalAndStream jas) throws IOException {
                jas.getManager().recoverUnfinalizedSegments();
            }
        }, "recoverUnfinalizedSegments");
    }
    private void mapJournalsAndReportErrors(
            JournalClosure closure, String status) throws IOException{

        List<JournalAndStream> badJAS = Lists.newLinkedList();
        for (JournalAndStream jas : journals) {
            try {
                closure.apply(jas);
            } catch (Throwable t) {
                if (jas.isRequired()) {
                    final String msg = "Error: " + status + " failed for required journal ("
                            + jas + ")";
                    LOG.fatal(msg, t);
                    // If we fail on *any* of the required journals, then we must not
                    // continue on any of the other journals. Abort them to ensure that
                    // retry behavior doesn't allow them to keep going in any way.
                    abortAllJournals();
                    // the current policy is to shutdown the NN on errors to shared edits
                    // dir. There are many code paths to shared edits failures - syncs,
                    // roll of edits etc. All of them go through this common function
                    // where the isRequired() check is made. Applying exit policy here
                    // to catch all code paths.
                    terminate(1, msg);
                } else {
                    LOG.error("Error: " + status + " failed for (journal " + jas + ")", t);
                    badJAS.add(jas);
                }
            }
        }
        disableAndReportErrorOnJournals(badJAS);
        if (!NameNodeResourcePolicy.areResourcesAvailable(journals,
                minimumRedundantJournals)) {
            String message = status + " failed for too many journals";
            LOG.error("Error: " + message);
            throw new IOException(message);
        }
    }
    /**
     * Called when some journals experience an error in some operation.
     */
    private void disableAndReportErrorOnJournals(List<JournalAndStream> badJournals) {
        if (badJournals == null || badJournals.isEmpty()) {
            return; // nothing to do
        }

        for (JournalAndStream j : badJournals) {
            LOG.error("Disabling journal " + j);
            j.abort();
            j.setDisabled(true);
        }
    }
    /**
     * Abort all of the underlying streams.
     */
    private void abortAllJournals() {
        for (JournalAndStream jas : journals) {
            if (jas.isActive()) {
                jas.abort();
            }
        }
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public boolean hasSomeData() throws IOException {
        return false;
    }
    private interface JournalClosure {
        /**
         * The operation on JournalAndStream.
         * @param jas Object on which operations are performed.
         * @throws IOException
         */
        public void apply(JournalAndStream jas) throws IOException;
    }
    @Override
    public void purgeLogsOlderThan(long minTxIdToKeep) throws IOException {

    }
   public void remove(JournalManager j) {
        JournalAndStream jasToRemove = null;
        for (JournalAndStream jas: journals) {
            if (jas.getManager().equals(j)) {
                jasToRemove = jas;
                break;
            }
        }
        if (jasToRemove != null) {
            jasToRemove.abort();
            journals.remove(jasToRemove);
        }
    }
    @Override
    public void selectInputStreams(Collection<EditLogInputStream> streams, long fromTxId, boolean inProgressOk, boolean forReading) throws IOException {
        PriorityQueue<EditLogInputStream> allStreams =
                new PriorityQueue<EditLogInputStream>(64,
                        EDIT_LOG_INPUT_STREAM_COMPARATOR);
        for (JournalAndStream jas : journals) {
            if (jas.isDisabled()) {
                LOG.info("Skipping jas " + jas + " since it's disabled");
                continue;
            }
            jas.getManager().selectInputStreams(allStreams, fromTxId, inProgressOk,
                    forReading);
        }
        chainAndMakeRedundantStreams(streams, allStreams, fromTxId);
    }
    public static void chainAndMakeRedundantStreams(
            Collection<EditLogInputStream> outStreams,
            PriorityQueue<EditLogInputStream> allStreams, long fromTxId) {
        // We want to group together all the streams that start on the same start
        // transaction ID.  To do this, we maintain an accumulator (acc) of all
        // the streams we've seen at a given start transaction ID.  When we see a
        // higher start transaction ID, we select a stream from the accumulator and
        // clear it.  Then we begin accumulating streams with the new, higher start
        // transaction ID.
        LinkedList<EditLogInputStream> acc =
                new LinkedList<EditLogInputStream>();
        EditLogInputStream elis;
        while ((elis = allStreams.poll()) != null) {
            if (acc.isEmpty()) {
                acc.add(elis);
            } else {
                long accFirstTxId = acc.get(0).getFirstTxId();
                if (accFirstTxId == elis.getFirstTxId()) {
                    acc.add(elis);
                } else if (accFirstTxId < elis.getFirstTxId()) {
                    outStreams.add(new RedundantFileEditLogInputStream(acc, fromTxId));
                    acc.clear();
                    acc.add(elis);
                } else if (accFirstTxId > elis.getFirstTxId()) {
                    throw new RuntimeException("sorted set invariants violated!  " +
                            "Got stream with first txid " + elis.getFirstTxId() +
                            ", but the last firstTxId was " + accFirstTxId);
                }
            }
        }
        if (!acc.isEmpty()) {
            outStreams.add(new RedundantFileEditLogInputStream(acc, fromTxId));
            acc.clear();
        }
    }
/*    public interface JournalClosure {
        *//**
         * The operation on JournalAndStream.
         * @param jas Object on which operations are performed.
         * @throws IOException
         *//*
        public void apply(JournalAndStream jas) throws IOException;
    }*/
    static class JournalAndStream implements CheckableNameNodeResource {
        private final JournalManager journal;
        private boolean disabled = false;
        private EditLogOutputStream stream;
        private boolean required = false;

        JournalAndStream(JournalManager manager, boolean required) {
            this.journal = manager;
            this.required = required;
        }

        public void startLogSegment(long txId) throws IOException {
            Preconditions.checkState(stream == null);
            disabled = false;
            stream = journal.startLogSegment(txId);
        }

        /**
         * Closes the stream, also sets it to null.
         */
        public void closeStream() throws IOException {
            if (stream == null) return;
            stream.close();
            stream = null;
        }

        /**
         * Close the Journal and Stream
         */
        public void close() throws IOException {
            closeStream();

            journal.close();
        }

        /**
         * Aborts the stream, also sets it to null.
         */
        public void abort() {
            if (stream == null) return;
            try {
                stream.abort();
            } catch (IOException ioe) {
            }
            stream = null;
        }

        boolean isActive() {
            return stream != null;
        }

        /**
         * Should be used outside JournalSet only for testing.
         */
        EditLogOutputStream getCurrentStream() {
            return stream;
        }

        @Override
        public String toString() {
            return "JournalAndStream(mgr=" + journal +
                    ", " + "stream=" + stream + ")";
        }

        void setCurrentStreamForTests(EditLogOutputStream stream) {
            this.stream = stream;
        }

      public   JournalManager getManager() {
            return journal;
        }

        boolean isDisabled() {
            return disabled;
        }

        private void setDisabled(boolean disabled) {
            this.disabled = disabled;
        }

        @Override
        public boolean isResourceAvailable() {
            return !isDisabled();
        }

        @Override
        public boolean isRequired() {
            return required;
        }
    }

   public String getSyncTimes() {
        StringBuilder buf = new StringBuilder();
        for (JournalAndStream jas : journals) {
            if (jas.isActive()) {
                buf.append(jas.getCurrentStream().getTotalSyncTime());
                buf.append(" ");
            }
        }
        return buf.toString();
    }
    /**
     * An implementation of EditLogOutputStream that applies a requested method on
     * all the journals that are currently active.
     */
    private class JournalSetOutputStream extends EditLogOutputStream {

        JournalSetOutputStream() throws IOException {
            super();
        }

        @Override
        public void write(final FSEditLogOp op)
                throws IOException {
            mapJournalsAndReportErrors(new JournalClosure() {
                @Override
                public void apply(JournalAndStream jas) throws IOException {
                    if (jas.isActive()) {
                        jas.getCurrentStream().write(op);
                    }
                }
            }, "write op");
        }

        @Override
        public void writeRaw(final byte[] data, final int offset, final int length)
                throws IOException {
            mapJournalsAndReportErrors(new JournalClosure() {
                @Override
                public void apply(JournalAndStream jas) throws IOException {
                    if (jas.isActive()) {
                        jas.getCurrentStream().writeRaw(data, offset, length);
                    }
                }
            }, "write bytes");
        }

        @Override
        public void create() throws IOException {
            mapJournalsAndReportErrors(new JournalClosure() {
                @Override
                public void apply(JournalAndStream jas) throws IOException {
                    if (jas.isActive()) {
                        jas.getCurrentStream().create();
                    }
                }
            }, "create");
        }

        @Override
        public void close() throws IOException {
            mapJournalsAndReportErrors(new JournalClosure() {
                @Override
                public void apply(JournalAndStream jas) throws IOException {
                    jas.closeStream();
                }
            }, "close");
        }

        @Override
        public void abort() throws IOException {
            mapJournalsAndReportErrors(new JournalClosure() {
                @Override
                public void apply(JournalAndStream jas) throws IOException {
                    jas.abort();
                }
            }, "abort");
        }

        @Override
        public void setReadyToFlush() throws IOException {
            mapJournalsAndReportErrors(new JournalClosure() {
                @Override
                public void apply(JournalAndStream jas) throws IOException {
                    if (jas.isActive()) {
                        jas.getCurrentStream().setReadyToFlush();
                    }
                }
            }, "setReadyToFlush");
        }

        @Override
        public void flushAndSync(final boolean durable) throws IOException {
            mapJournalsAndReportErrors(new JournalClosure() {
                @Override
                public void apply(JournalAndStream jas) throws IOException {
                    if (jas.isActive()) {
                        jas.getCurrentStream().flushAndSync(durable);
                    }
                }
            }, "flushAndSync");
        }

        @Override
        public void flush() throws IOException {
            mapJournalsAndReportErrors(new JournalClosure() {
                @Override
                public void apply(JournalAndStream jas) throws IOException {
                    if (jas.isActive()) {
                        jas.getCurrentStream().flush();
                    }
                }
            }, "flush");
        }

        @Override
        public boolean shouldForceSync() {
            for (JournalAndStream js : journals) {
                if (js.isActive() && js.getCurrentStream().shouldForceSync()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public long getNumSync() {
            for (JournalAndStream jas : journals) {
                if (jas.isActive()) {
                    return jas.getCurrentStream().getNumSync();
                }
            }
            return 0;
        }
    }


    /**
     * Return a manifest of what finalized edit logs are available. All available
     * edit logs are returned starting from the transaction id passed.
     *
     * @param fromTxId Starting transaction id to read the logs.
     * @return RemoteEditLogManifest object.
     */
    public synchronized RemoteEditLogManifest getEditLogManifest(long fromTxId,
                                                                 boolean forReading) {
        // Collect RemoteEditLogs available from each FileJournalManager
        List<RemoteEditLog> allLogs = Lists.newArrayList();
        for (JournalAndStream j : journals) {
            if (j.getManager() instanceof FileJournalManager) {
                FileJournalManager fjm = (FileJournalManager)j.getManager();
                try {
                    allLogs.addAll(fjm.getRemoteEditLogs(fromTxId, forReading, false));
                } catch (Throwable t) {
                    LOG.warn("Cannot list edit logs in " + fjm, t);
                }
            }
        }

        // Group logs by their starting txid
        ImmutableListMultimap<Long, RemoteEditLog> logsByStartTxId =
                Multimaps.index(allLogs, RemoteEditLog.GET_START_TXID);
        long curStartTxId = fromTxId;

        List<RemoteEditLog> logs = Lists.newArrayList();
        while (true) {
            ImmutableList<RemoteEditLog> logGroup = logsByStartTxId.get(curStartTxId);
            if (logGroup.isEmpty()) {
                // we have a gap in logs - for example because we recovered some old
                // storage directory with ancient logs. Clear out any logs we've
                // accumulated so far, and then skip to the next segment of logs
                // after the gap.
                SortedSet<Long> startTxIds = Sets.newTreeSet(logsByStartTxId.keySet());
                startTxIds = startTxIds.tailSet(curStartTxId);
                if (startTxIds.isEmpty()) {
                    break;
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Found gap in logs at " + curStartTxId + ": " +
                                "not returning previous logs in manifest.");
                    }
                    logs.clear();
                    curStartTxId = startTxIds.first();
                    continue;
                }
            }

            // Find the one that extends the farthest forward
            RemoteEditLog bestLog = Collections.max(logGroup);
            logs.add(bestLog);
            // And then start looking from after that point
            curStartTxId = bestLog.getEndTxId() + 1;
        }
        RemoteEditLogManifest ret = new RemoteEditLogManifest(logs);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Generated manifest for logs since " + fromTxId + ":"
                    + ret);
        }
        return ret;
    }
}
