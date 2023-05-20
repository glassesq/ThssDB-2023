package cn.edu.thssdb.storage;

import cn.edu.thssdb.runtime.ServerRuntime;
import cn.edu.thssdb.storage.page.*;

import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

import static cn.edu.thssdb.storage.page.Page.*;

public class DiskBuffer {

    static ReentrantLock diskBufferLatch = new ReentrantLock();

    /**
     * map from {@code spID = spaceId[4byte]-pageID[4byte] }  to a page class.
     * !important: Buffer shall be accessed with diskBufferLatch.
     */
    private final static HashMap<Long, Page> buffer = new HashMap<>();


    public static long concat(int spaceId, int pageId) {
        return (Integer.toUnsignedLong(spaceId) << 32) | pageId;
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
        Page page = getFromBuffer(concat(spaceId, pageId));
        if (page == null) {
            input(spaceId, pageId);
            page = getFromBuffer(concat(spaceId, pageId));
        }
        return page;
    }

    /**
     * get from buffer with diskBufferLatch
     * Because {@code HashMap.get} and {@code HashMap.put} are not atomic, the {@code diskBufferLatch} is needed for concurrency access.
     *
     * @param key hashmap key
     * @return hashmap value
     */
    public static Page getFromBuffer(long key) {
        diskBufferLatch.lock();
        Page page = buffer.get(key);
        diskBufferLatch.unlock();
        return page;
    }

    /**
     * put a page into disk buffer with diskBufferLatch.
     * Because {@code HashMap.get} and {@code HashMap.put} are not atomic, the {@code diskBufferLatch} is needed for concurrency access.
     *
     * @param page page object
     */
    public static void putToBuffer(Page page) {
        diskBufferLatch.lock();
        buffer.put(concat(page.spaceId, page.pageId), page);
        diskBufferLatch.unlock();
    }

    /**
     * remove page from buffer with diskBufferLatch
     * Because {@code HashMap.remove} is not atomic, the {@code diskBufferLatch} is needed for concurrency access.
     *
     * @param key hashmap key
     */
    public static void removeFromBuffer(long key) {
        diskBufferLatch.lock();
        buffer.remove(key);
        diskBufferLatch.unlock();
    }


    /**
     * read a page from disk to buffer.
     *
     * @param spaceId spaceId
     * @param pageId  pageId
     * @throws Exception if the reading process fails.
     */
    public static void input(int spaceId, int pageId) throws Exception {
        String tablespaceFilename = ServerRuntime.getTablespaceFile(spaceId);
        RandomAccessFile tablespaceFile = new RandomAccessFile(tablespaceFilename, "r");
        byte[] pageBytes = new byte[(int) ServerRuntime.config.pageSize];
        tablespaceFile.seek(ServerRuntime.config.pageSize * ((long) 0x00000000FFFFFFFF & pageId));
        int bytes = tablespaceFile.read(pageBytes, 0, ServerRuntime.config.pageSize);
        if (bytes != ServerRuntime.config.pageSize) {
            throw new Exception("read page error. Wrong length" + bytes);
        }

        int pageType = ((int) pageBytes[12] << 8) | pageBytes[13];
        System.out.println("pageType" + pageType);
        Page page;
        switch (pageType) {
            case OVERALL_PAGE:
                page = new OverallPage(pageBytes);
                break;
            case INDEX_PAGE:
                page = new IndexPage(pageBytes);
                System.out.println("here is a index page");
                break;
            case EXTENT_MANAGE_PAGE:
                page = new ExtentManagePage(pageBytes);
                break;
            case DATA_PAGE:
                page = new DataPage(pageBytes);
            default:
                page = new Page(pageBytes);
                System.out.println("here is a default page");
        }

        /* Because {@code HashMap.get} and {@code HashMap.put} are not atomic, the {@code diskBufferLatch} is needed for concurrency access. */
        diskBufferLatch.lock();
        if (!buffer.containsKey(concat(spaceId, pageId))) {
            buffer.put(concat(spaceId, pageId), page);
        }
        /* Because {@code HashMap.get} and {@code HashMap.put} are not atomic, the {@code diskBufferLatch} is needed for concurrency access. */
        diskBufferLatch.unlock();

        tablespaceFile.close();
    }


    /**
     * write a pge from buffer to disk.
     *
     * @param spaceId spaceId
     * @param pageId  pageId
     * @throws Exception if writing fails.
     */
    public static void output(int spaceId, int pageId) throws Exception {
        Page page = getFromBuffer(concat(spaceId, pageId));

        /* avoid writing and outputting page simultaneously */
        page.pageWriteAndOutputLatch.lock();

        String tablespaceFilename = ServerRuntime.getTablespaceFile(spaceId);
        RandomAccessFile tablespaceFile = new RandomAccessFile(tablespaceFilename, "rw");
        byte[] pageBytes = page.bytes;
        tablespaceFile.seek(ServerRuntime.config.pageSize * ((long) 0x00000000FFFFFFFF & pageId));
        tablespaceFile.write(pageBytes, 0, ServerRuntime.config.pageSize);
        tablespaceFile.close();

        // TODO: add condition, only discard page when necessary
        removeFromBuffer(concat(spaceId, pageId));

        /* avoid writing and outputting page simultaneously */
        page.pageWriteAndOutputLatch.unlock();
    }

    /*
    // TODO: page discard in memory constraint scenarios
    public void dumpPages() { }
     */
}
