package cn.edu.thssdb.storage.writeahead;


import java.util.ArrayList;

public class WriteLog {
    public static class WriteLogEntry {
        byte[] newValue;
        byte[] oldValue;

        public int transactionId;

        public int length;
        public int offset;
        public int pageId;
        public int spaceId;
        public boolean redo_only;

        /**
         * Type of the record.
         * COMMON(0)
         */
        public int type;

        public WriteLogEntry(int transactionId, int spaceId, int pageId, int offset, int length, byte[] oldValue, byte[] newValue, boolean redo_only) {
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
            StringBuilder result = new StringBuilder("RECORD: transactionId: " + transactionId +" spaceId: " + spaceId + " pageId: " + pageId + " offset: " + offset + " length: " + length + " redo_only: " + redo_only + "\n");
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

    /**
     * Write Ahead Log Buffer
     */
    public static ArrayList<WriteLogEntry> buffer = new ArrayList<>();

    /**
     * Add Common Write Log to WAL Buffer
     *
     * @param transactionId transactionId of the operation
     * @param spaceId  spaceId
     * @param pageId   pageId
     * @param offset   offset
     * @param length   length of bytes to write
     * @param oldValue old value. For undo, oldValue's length shall be 0.
     * @param newValue new value to write
     */
    public static void addCommonLog(int transactionId, int spaceId, int pageId, int offset, int length, byte[] oldValue, byte[] newValue) {
        // TODO: transaction ID
        WriteLogEntry entry;
        if (oldValue.length > 0) {
            entry = new WriteLogEntry(transactionId, spaceId, pageId, offset, length, oldValue, newValue, false);
        } else {
            entry = new WriteLogEntry(transactionId, spaceId, pageId, offset, length, oldValue, newValue, true);
        }
        entry.type = 0; /* 0 for common entry */
        buffer.add(entry);
        System.out.println(entry);
    }

    public static void writeAllToDisk() {
        //TODO

    }
}
