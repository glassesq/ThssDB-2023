package cn.edu.thssdb.communication;

import cn.edu.thssdb.runtime.ServerRuntime;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.storage.DiskBuffer;
import cn.edu.thssdb.storage.page.Page;
import cn.edu.thssdb.storage.writeahead.WriteLog;

import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;

import static cn.edu.thssdb.runtime.ServerRuntime.config;

public class IO {
    /**
     * This method can be used safely due to the presence of locks.
     * TODO: parse page according to the page type, free of other information.
     *
     * @param spaceId spaceId
     * @param pageId  pageId
     * @return not parsed Page
     */
    public static Page read(int spaceId, int pageId) throws Exception {
        // TODO: LOCK
        return DiskBuffer.read(spaceId, pageId);
    }

    /**
     * This method write values on both disk buffer and the WAL log buffer.
     * Can be used safely due to the presence of locks.
     *
     * @param transactionId transaction that requests this write
     * @param page          page reference
     * @param offset        offset
     * @param length        length of bytes to write
     * @param newValue      bytes value to write
     * @param redo_only     whether it is redo-only
     */
    public static void write(long transactionId, Page page, int offset, int length, byte[] newValue, boolean redo_only) {
        // TODO: Two-phase Lock

        // TODO: Latch For Page Writing
        /* Write the changes to disk buffer */
        boolean dirty = false;
        byte[] oldValue = new byte[0];
        if (!redo_only) {
            oldValue = new byte[length];
            for (int i = offset, s = 0; i < offset + length; i++, s++) {
                if (page.bytes[i] != newValue[s]) {
                    dirty = true;
                }
                oldValue[s] = page.bytes[i];
                page.bytes[i] = newValue[s];
            }
        }
        /* Write-Ahead Log */
        /* only add write log when there is actually change. */
        if (dirty)
            WriteLog.addCommonLog(transactionId, page.spaceId, page.pageId, offset, length, oldValue, newValue);
        // TODO: release Latch (may be advanced)
    }

    /**
     * Push every log in WAL buffer and relevant pages to disk.
     * This method use {@code Latches} for actually page writing. The {@code latch} shall be released immediately.
     * Latch is not the same lock using in two-phase transaction.
     */
    private static void pushWALAndPages() throws Exception {
        // TODO: Latch for WAL
        /* push all write log records in buffer to disk */
        HashSet<Long> dirtyPages = new HashSet<>();
        for (WriteLog.WriteLogEntry entry : WriteLog.buffer) {
            entry.writeToDisk();
            if (entry.type == WriteLog.COMMON_LOG) dirtyPages.add(((long) entry.spaceId << 32) | entry.pageId);
        }
        WriteLog.buffer.clear();
        /* output all dirty relevant pages to disk */
        for (Long spId : dirtyPages) {
            // TODO: Latch For Page Writing ( no transactions are allowed to write WAL buffer now. )
            // Latch can be delayed to the actual page writing, due to the presence of WAL latches.
            // No other transactions can modify pages without the obtaining of WAL latches.
            // TODO: to discuss, we may move latches for pages into DiskBuffer.output function?
            DiskBuffer.output((int) (spId >> 32), spId.intValue());
            // TODO: release latch For Page Writing ( no transactions are allowed to write WAL buffer now. )
        }
        // TODO: release Latch for WAL
    }

    public static void writeTransactionStart(long transactionId) throws Exception {
        // TODO: latch
        WriteLog.addSpecialLog(transactionId, WriteLog.START_LOG);
        // TODO: release latch
    }

    public static void writeCreateDatabase(long transactionId, String name, int databaseId) throws Exception {
        // TODO: latch
        WriteLog.addSpecialDatabaseLog(transactionId, WriteLog.CREATE_DATABASE_LOG,
                databaseId, name.getBytes(StandardCharsets.UTF_8));
        // TODO: release latch
    }

    public static void writeDeleteDatabase(long transactionId, String name, int databaseId) throws Exception {
        // TODO: latch
        WriteLog.addSpecialDatabaseLog(transactionId, WriteLog.DELETE_DATABASE_LOG,
                databaseId, name.getBytes(StandardCharsets.UTF_8));
    }

    public static void writeCreateTable(long transactionId, int databaseId, Table.TableMetadata metadata) {
        // TODO: latch
        WriteLog.addCreateTableLog(transactionId, databaseId, metadata);
        // TODO: release latch
    }

    /**
     * push all updates on metadataObject to disk
     *
     * @throws Exception write metadata file failed.
     */
    private static void pushMetadataUpdate() throws Exception {
        // TODO: Lock for Metadata File.
        FileOutputStream metadataStream = new FileOutputStream(config.MetadataFilename);
        metadataStream.write(ServerRuntime.metadataArray.toString().getBytes(StandardCharsets.UTF_8));
        metadataStream.close();
    }

    public static void pushTransactionCommit(long transactionId) throws Exception {
        // TODO: latch
        WriteLog.addSpecialLog(transactionId, WriteLog.COMMIT_LOG);
        pushWALAndPages();
        pushMetadataUpdate();
        // TODO: release latch
    }

    public static void pushTransactionAbort(long transactionId) throws Exception {
        // TODO: latch
        // TODO: REDO pushWALAndPages();
        // WriteLog.addSpecialLog(transactionId, WriteLog.ABORT_LOG);
        // TODO: release latch
    }
}
