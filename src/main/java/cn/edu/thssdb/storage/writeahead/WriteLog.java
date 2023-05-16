package cn.edu.thssdb.storage.writeahead;


import cn.edu.thssdb.runtime.ServerRuntime;
import cn.edu.thssdb.schema.Table;

import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class WriteLog {
    public static class WriteLogEntry {
        byte[] newValue;
        byte[] oldValue;

        public long transactionId;

        public int length;
        public int offset;
        public int pageId;
        public int spaceId;
        public boolean redo_only;

        public int databaseId;

        /**
         * Type of the record.
         * COMMON(0)
         * COMMIT(1); transaction commit;
         * START(2); transaction start;
         * ABORT(3); transaction abort;
         * CREATE_DATABASE(4); create database newValue (databaseId);
         * DELETE_DATABASE(5); delete database newValue (databaseId);
         * CREATE_TABLE(6); create table newValue (tableId);
         */
        public int type;

        public WriteLogEntry(long transactionId, int type) {
            this.transactionId = transactionId;
            this.type = type;
        }

        public WriteLogEntry(long transactionId, int spaceId, int pageId, int offset, int length, byte[] oldValue, byte[] newValue, boolean redo_only) {
            this.transactionId = transactionId;
            this.spaceId = spaceId;
            this.pageId = pageId;
            this.offset = offset;
            this.length = length;
            this.oldValue = oldValue;
            this.newValue = newValue;
            this.redo_only = redo_only;
        }

        @Override
        public String toString() {
            // TODO: we may use compressed format for WAL latter, currently we use plain-text.
            // TODO: this is only for test.
            switch (type) {
                case CHECKPOINT_LOG:
                    return "CHECKPOINT dirty pages are all written to disk.";
                case COMMIT_LOG:
                    return "TRANSACTION COMMIT RECORD: transactionId: " + transactionId;
                case START_LOG:
                    return "TRANSACTION START RECORD: transactionId: " + transactionId;
                case ABORT_LOG:
                    return "TRANSACTION ABORT RECORD: transactionId: " + transactionId;
                case CREATE_DATABASE_LOG:
                    return "CREATE DATABASE RECORD: databaseName: " + new String(newValue) + " databaseId: " + databaseId;
                case DELETE_DATABASE_LOG:
                    return "DELETE DATABASE RECORD: databaseName: " + new String(newValue) + " databaseId: " + databaseId;
                case CREATE_TABLE_LOG:
                    return "CREATE TABLE RECORD: databaseId: " + databaseId + " tableInfo: \n" + new String(newValue);
                default:
                    StringBuilder result = new StringBuilder("RECORD: transactionId: " + transactionId + " spaceId: " + spaceId + " pageId: " + pageId + " offset: " + offset + " length: " + length + " redo_only: " + redo_only + "\n");
                    result.append("old-value: ");
                    for (byte b : this.oldValue) {
                        result.append(String.format("%02x ", b));
                    }
                    result.append("\nnew-value: ");
                    for (byte b : this.newValue) {
                        result.append(String.format("%02x ", b));
                    }
                    return result.toString();
            }
        }

        public void writeToDisk() throws Exception {
            FileOutputStream stream = new FileOutputStream(ServerRuntime.config.WALFilename, true);
            stream.write((this + "\n").getBytes());
        }

    }

    /* Log Type */
    public static final int COMMON_LOG = 0;
    public static final int COMMIT_LOG = 1;
    public static final int START_LOG = 2;
    public static final int ABORT_LOG = 3;
    public static final int CREATE_DATABASE_LOG = 4;
    public static final int DELETE_DATABASE_LOG = 5;
    public static final int CREATE_TABLE_LOG = 6;

    public static final int CHECKPOINT_LOG = 1000;


    /**
     * Write Ahead Log Buffer
     */
    public static ArrayList<WriteLogEntry> buffer = new ArrayList<>();

    /**
     * Add Common Write Log to WAL Buffer
     *
     * @param transactionId transactionId of the operation
     * @param spaceId       spaceId
     * @param pageId        pageId
     * @param offset        offset
     * @param length        length of bytes to write
     * @param oldValue      old value. For undo, oldValue's length shall be 0.
     * @param newValue      new value to write
     */
    public static void addCommonLog(long transactionId, int spaceId, int pageId, int offset, int length, byte[] oldValue, byte[] newValue) {
        // TODO: Latch for WAL updates

        // TODO: transaction ID
        WriteLogEntry entry;
        if (oldValue.length > 0) {
            entry = new WriteLogEntry(transactionId, spaceId, pageId, offset, length, oldValue, newValue, false);
        } else {
            entry = new WriteLogEntry(transactionId, spaceId, pageId, offset, length, oldValue, newValue, true);
        }
        entry.type = COMMON_LOG; /* 0 for common entry */
        buffer.add(entry);

        // TODO: release Latch for WAL updates
//        System.out.println(entry);
    }

    public static void addSpecialLog(long transactionId, int type) {
        // TODO: Latch for WAL updates
        WriteLogEntry entry = new WriteLogEntry(transactionId, type);
        buffer.add(entry);
        // TODO: release latch for WAL updates
    }

    public static void addSpecialDatabaseLog(long transactionId, int type, int databaseId, byte[] databaseName) {
        // TODO: Latch for WAL updates
        WriteLogEntry entry = new WriteLogEntry(transactionId, type);
        entry.databaseId = databaseId;
        entry.newValue = databaseName;
        buffer.add(entry);
        // TODO: release latch for WAL updates
    }

    public static void addCreateTableLog(long transactionId, int databaseId, Table.TableMetadata metadata) {
        WriteLogEntry entry = new WriteLogEntry(transactionId, CREATE_TABLE_LOG);
        entry.databaseId = databaseId;
        entry.newValue = metadata.object.toString().getBytes(StandardCharsets.UTF_8);
        buffer.add(entry);
    }


}
