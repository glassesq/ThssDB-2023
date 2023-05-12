package cn.edu.thssdb.storage;

import cn.edu.thssdb.runtime.ServerRuntime;
import cn.edu.thssdb.storage.page.Page;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;

public class DiskBuffer {

    /**
     * map from {@code spID = spaceId[4byte]-pageID[4byte] }  to a page class.
     */
    public static HashMap<Long, Page> buffer;

    public static long concat(int spaceId, int pageId) {
        return (long) spaceId << 32 | pageId;
    }

    /**
     * read a page from buffer
     * this method shall be used carefully due to the lack of lock.
     *
     * @param spaceId spaceId
     * @param pageId  pageId
     * @return byte[] page
     */
    public static Page read(int spaceId, int pageId) throws Exception {
        // TODO: performance consideration
        Page page = buffer.get(concat(spaceId, pageId));
        if (page == null) {
            input(spaceId, pageId);
            page = buffer.get(concat(spaceId, pageId));
        }
        return page;
    }

    /**
     * read a page from disk to buffer.
     *
     * @param spaceId spaceId
     * @param pageId  pageId
     * @throws Exception if the reading process fails.
     */
    public static void input(int spaceId, int pageId) throws Exception {
        // TODO: buffer lock
        String tablespaceFilename = ServerRuntime.getTablespaceFile(spaceId);
        FileInputStream tablespaceFile = new FileInputStream(tablespaceFilename);
        byte[] pageBytes = new byte[(int) ServerRuntime.config.pageSize];
        int bytes = tablespaceFile.read(pageBytes, ServerRuntime.config.pageSize * (int) ((long) 0x00000000FFFFFFFF & pageId), ServerRuntime.config.pageSize);
        if (bytes != ServerRuntime.config.pageSize) {
            throw new Exception("read page error. Wrong length" + bytes);
        }
        Page page = new Page();
        page.bytes = pageBytes;
        buffer.put(concat(spaceId, pageId), page);
        tablespaceFile.close();
    }

    /**
     * write a pge from buffer to disk.
     *
     * @param spaceId spaceId
     * @param pageId  pageId
     * @throws Exception if writing fails.
     */
    public void output(int spaceId, int pageId) throws Exception {
        // TODO: buffer lock
        String tablespaceFilename = ServerRuntime.getTablespaceFile(spaceId);
        FileOutputStream tablespaceFile = new FileOutputStream(tablespaceFilename);
        byte[] page = buffer.get(concat(spaceId, pageId)).bytes;
        tablespaceFile.write(page, ServerRuntime.config.pageSize * (int) ((long) 0x00000000FFFFFFFF & pageId), ServerRuntime.config.pageSize);
        buffer.remove(concat(spaceId, pageId));
        tablespaceFile.close();
    }

    // TODO: implement this function
    /*
    public void dumpPages() {

    }
     */
}
