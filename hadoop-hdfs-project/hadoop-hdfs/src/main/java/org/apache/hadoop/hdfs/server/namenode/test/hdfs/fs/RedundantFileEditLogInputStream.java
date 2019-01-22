package org.apache.hadoop.hdfs.server.namenode.test.hdfs.fs;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;

public class RedundantFileEditLogInputStream extends EditLogInputStream {
    public static final Log LOG = LogFactory.getLog(EditLogInputStream.class.getName());

    @Override
    public String getName() {
        StringBuilder bld = new StringBuilder();
        String prefix = "";
        for (EditLogInputStream elis : streams) {
            bld.append(prefix);
            bld.append(elis.getName());
            prefix = ", ";
        }
        return bld.toString();
    }

    @Override
    public long getFirstTxId() {
        return streams[curIdx].getFirstTxId();

    }

    @Override
    public long getLastTxId() {
        return streams[curIdx].getLastTxId();
    }

    @Override
    public void close() throws IOException {
        IOUtils.cleanup(LOG,  streams);

    }

    @Override
    protected FSEditLogOp nextOp() throws IOException {
        return null;
    }

    @Override
    public int getVersion() throws IOException {
        return streams[curIdx].getVersion();
    }

    @Override
    public long getPosition() {
        return streams[curIdx].getPosition();
    }

    @Override
    public long length() throws IOException {
        return streams[curIdx].length();
    }

    @Override
    public boolean isInProgress() {
        return false;
    }

    @Override
    public void setMaxOpSize(int maxOpSize) {

    }
    private int curIdx;
    private long prevTxId;
    private final EditLogInputStream[] streams;
    private IOException prevException;
    private State state;

    static public enum State {
        /** We need to skip until prevTxId + 1 */
        SKIP_UNTIL,
        /** We're ready to read opcodes out of the current stream */
        OK,
        /** The current stream has failed. */
        STREAM_FAILED,
        /** The current stream has failed, and resync() was called.  */
        STREAM_FAILED_RESYNC,
        /** There are no more opcodes to read from this
         * RedundantEditLogInputStream */
        EOF;
    }
   public RedundantFileEditLogInputStream(Collection<EditLogInputStream> streams,
                                long startTxId) {
        this.curIdx = 0;
        this.prevTxId = (startTxId == HdfsConstants.INVALID_TXID) ?
                HdfsConstants.INVALID_TXID : (startTxId - 1);
        this.state = (streams.isEmpty()) ? State.EOF : State.SKIP_UNTIL;
        this.prevException = null;
        // EditLogInputStreams in a RedundantEditLogInputStream must be finalized,
        // and can't be pre-transactional.
        EditLogInputStream first = null;
        for (EditLogInputStream s : streams) {
            Preconditions.checkArgument(s.getFirstTxId() !=
                    HdfsConstants.INVALID_TXID, "invalid first txid in stream: %s", s);
            Preconditions.checkArgument(s.getLastTxId() !=
                    HdfsConstants.INVALID_TXID, "invalid last txid in stream: %s", s);
            if (first == null) {
                first = s;
            } else {
                Preconditions.checkArgument(s.getFirstTxId() == first.getFirstTxId(),
                        "All streams in the RedundantEditLogInputStream must have the same " +
                                "start transaction ID!  " + first + " had start txId " +
                                first.getFirstTxId() + ", but " + s + " had start txId " +
                                s.getFirstTxId());
            }
        }

        this.streams = streams.toArray(new EditLogInputStream[0]);

        // We sort the streams here so that the streams that end later come first.
        Arrays.sort(this.streams, new Comparator<EditLogInputStream>() {
            @Override
            public int compare(EditLogInputStream a, EditLogInputStream b) {
                return Longs.compare(b.getLastTxId(), a.getLastTxId());
            }
        });
    }
}
