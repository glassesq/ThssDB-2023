package cn.edu.thssdb.storage.page;

import cn.edu.thssdb.communication.IO;
import cn.edu.thssdb.runtime.ServerRuntime;

/**
 * Basic class for all page
 */
public class Page {

    /* FIL Header */
    /**
     * checksum of the page.
     *
     * @deprecated not used right now for performance consideration.
     */
    int checksum;

    /**
     * spaceId is a 4-byte Unsigned Integer.
     * It stored in binary using big-endian.
     */
    public int spaceId;

    /**
     * pageId is a 4-byte Unsigned Integer.
     * It stored in binary using big-endian.
     */
    public int pageId;

    /**
     * 8-byte LSN (reserved).
     */
    long LSN;

    /**
     * 4-byte previous(left) pageId, used in B-link-tree.
     */
    int previousPageId;

    /**
     * 4-byte next(right) pageId, used in B-link-tree.
     */
    int nextPageId;


    /**
     * the type of current page.
     * OVERALL(0),
     * EXTENT_MANAGE(1),
     * INDEX_ROOT(2),
     * INDEX_INTERNAL(3),
     * INDEX_LEAF(4),
     * DATA(5),
     * RESERVED(6)
     */
    int pageType;

    /**
     * List Node for Double Linked List. In form of (PageId, Offset).
     */
    public class ListNode {
        //        int previousPageId;
        //       int previousOffset;
        public int nextPageId;
        public int nextOffset;

        public void parse(int pos) {
            /*
            RESERVED 6 byte for two-way linked list
            previousPageId = parseIntegerBig(pos);
            previousOffset = parseShortBig(pos + 4);
            */
            nextPageId = parseIntegerBig(pos + 6);
            nextOffset = parseShortBig(pos + 10);
        }

        /**
         * Write the list node info on both disk buffer and WAL log buffer.
         * This method can be safely used.
         *
         * @param page on which the node resides.
         * @param pos  offset where the listNode starts.
         */
        public void write(long transactionId, Page page, int pos) {
            byte[] newValues = new byte[16];
            /* RESERVED 6 byte for two-way linked list
            newValues[0] = (byte) (previousPageId >> 24);
            newValues[1] = (byte) (previousPageId >> 16);
            newValues[2] = (byte) (previousPageId >> 8);
            newValues[3] = (byte) previousPageId;
            newValues[4] = (byte) (previousOffset >> 8);
            newValues[5] = (byte) previousOffset;
             */
            newValues[6] = (byte) (nextPageId >> 24);
            newValues[7] = (byte) (nextPageId >> 16);
            newValues[8] = (byte) (nextPageId >> 8);
            newValues[9] = (byte) nextPageId;
            newValues[10] = (byte) (nextOffset >> 8);
            newValues[11] = (byte) nextOffset;
            IO.write(transactionId, page, pos, 12, newValues, false);
        }


    }

    /**
     * List Base Node for Double Linked List. In form of (PageId, Offset).
     * Compared to List Node, it has additional length field.
     */
    public class ListBaseNode {
        int length;
        int nextPageId;
        int nextOffset;

        public void parse(int pos) {
            length = parseIntegerBig(pos);
            /*
            RESERVED for two-way linked list.
            previousPageId = parseIntegerBig(pos + 4);
            previousOffset = parseShortBig(pos + 8);
             */
            nextPageId = parseIntegerBig(pos + 10);
            nextOffset = parseShortBig(pos + 14);
        }

        /**
         * Write the list base node info on both disk buffer and WAL log buffer.
         * This method can be safely used.
         *
         * @param page on which the node resides.
         * @param pos  offset where the listBaseNode starts.
         */
        public void write(long transactionId, Page page, int pos) {
            byte[] newValues = new byte[20];
            newValues[0] = (byte) (length >> 24);
            newValues[1] = (byte) (length >> 16);
            newValues[2] = (byte) (length >> 8);
            newValues[3] = (byte) length;
            /*
            RESERVED for two-way linked list.
            newValues[pos + 4] = (byte) (previousPageId >> 24);
            newValues[pos + 5] = (byte) (previousPageId >> 16);
            newValues[pos + 6] = (byte) (previousPageId >> 8);
            newValues[pos + 7] = (byte) previousPageId;
            newValues[pos + 8] = (byte) (previousOffset >> 8);
            newValues[pos + 9] = (byte) previousOffset;
             */
            newValues[10] = (byte) (nextPageId >> 24);
            newValues[11] = (byte) (nextPageId >> 16);
            newValues[12] = (byte) (nextPageId >> 8);
            newValues[13] = (byte) nextPageId;
            newValues[14] = (byte) (nextOffset >> 8);
            newValues[15] = (byte) nextOffset;
            IO.write(transactionId, page, pos, 16, newValues, false);
        }
    }

    /* raw bytes of this page. */
    public byte[] bytes = new byte[ServerRuntime.config.pageSize];

    public int parseIntegerBig(int pos) {
        return ((bytes[pos] & 0xFF) << 24) | ((bytes[pos + 1] & 0xFF) << 16) | ((bytes[pos + 2] & 0xFF) << 8) | (bytes[pos + 3] & 0xFF);
    }

    public long parseLongBig(int pos) {
        return (long) parseIntegerBig(pos) << 32 | parseIntegerBig(pos + 4);
    }

    public int parseShortBig(int pos) {
        return ((bytes[pos] & 0xFF) << 8) | (bytes[pos + 1] & 0xFF);
    }

    // TODO: check for CHECKSUM

    /**
     * parse FIL Header
     */
    public void parseFILHeader() {
        /* CHECKSUM FOR 4 BYTE */
        spaceId = parseIntegerBig(4);
        pageId = parseIntegerBig(8);
        pageType = parseShortBig(12);
        /* RESERVED FOR 2 BYTE */
        LSN = parseLongBig(16);
        previousPageId = parseIntegerBig(24);
        nextPageId = parseIntegerBig(28);
    }

    /**
     * Write FIL Header on both disk buffer and WAL log buffer.
     */
    public void writeFILHeader(long transactionId) {
        byte[] newValue = new byte[32];
        // TODO: checkSum
        newValue[4] = (byte) (spaceId >> 24);
        newValue[5] = (byte) (spaceId >> 16);
        newValue[6] = (byte) (spaceId >> 8);
        newValue[7] = (byte) (spaceId);
        newValue[8] = (byte) (pageId >> 24);
        newValue[9] = (byte) (pageId >> 16);
        newValue[10] = (byte) (pageId >> 8);
        newValue[11] = (byte) (pageId);
        newValue[12] = (byte) (pageType >> 8);
        newValue[13] = (byte) (pageType);
        /* RESERVED FOR 2 byte */
        newValue[16] = (byte) (LSN >> 56);
        newValue[17] = (byte) (LSN >> 48);
        newValue[18] = (byte) (LSN >> 40);
        newValue[19] = (byte) (LSN >> 32);
        newValue[20] = (byte) (LSN >> 24);
        newValue[21] = (byte) (LSN >> 16);
        newValue[22] = (byte) (LSN >> 8);
        newValue[23] = (byte) (LSN);
        newValue[24] = (byte) (previousPageId >> 24);
        newValue[25] = (byte) (previousPageId >> 16);
        newValue[26] = (byte) (previousPageId >> 8);
        newValue[27] = (byte) (previousPageId);
        newValue[28] = (byte) (nextPageId >> 24);
        newValue[29] = (byte) (nextPageId >> 16);
        newValue[30] = (byte) (nextPageId >> 8);
        newValue[31] = (byte) (nextPageId);
        IO.write(transactionId, this, 0, 32, newValue, false);
    }

    public void parse() {
        parseFILHeader();
    }

    public void writeAll(long transactionId) {
        // TODO: checksum
        writeFILHeader(transactionId);
    }
}
