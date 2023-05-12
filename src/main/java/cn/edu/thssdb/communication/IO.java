package cn.edu.thssdb.communication;

import cn.edu.thssdb.storage.DiskBuffer;
import cn.edu.thssdb.storage.page.Page;
import cn.edu.thssdb.storage.writeahead.WriteLog;

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
     * @param page      page reference
     * @param offset    offset
     * @param length    length of bytes to write
     * @param newValue  bytes value to write
     * @param redo_only whether it is redo-only
     */
    public static void write(Page page, int offset, int length, byte[] newValue, boolean redo_only) {
        // TODO: LOCK
        /* Write the changes to disk buffer */
        byte[] oldValue = new byte[0];
        if (!redo_only) {
            oldValue = new byte[length];
            for (int i = offset, s = 0; i < offset + length; i++, s++) {
                oldValue[s] = page.bytes[i];
                page.bytes[i] = newValue[s];
            }
        }

        /* Write-Ahead Log */
        WriteLog.addCommonLog(page.spaceId, page.pageId, offset, length, oldValue, newValue);
    }
}
