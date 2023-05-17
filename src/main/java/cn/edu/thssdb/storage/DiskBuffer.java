package cn.edu.thssdb.storage;

import cn.edu.thssdb.runtime.ServerRuntime;
import cn.edu.thssdb.storage.page.*;

import java.io.RandomAccessFile;
import java.util.HashMap;

import static cn.edu.thssdb.storage.page.Page.*;

public class DiskBuffer {

    /**
     * map from {@code spID = spaceId[4byte]-pageID[4byte] }  to a page class.
     */
    public static HashMap<Long, Page> buffer = new HashMap<>();

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
        // TODO: performance consideration
        Page page = buffer.get(concat(spaceId, pageId));
        if (page == null) {
            input(spaceId, pageId);
            page = buffer.get(concat(spaceId, pageId));
        }
        return page;
    }


    /**
     * put a page into disk buffer.
     *
     * @param page page object
     */
    public static void put(Page page) {
        buffer.put(concat(page.spaceId, page.pageId), page);
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
                OverallPage overallPage = new OverallPage();
                overallPage.bytes = pageBytes;
                overallPage.parse();
                page = overallPage;
                break;
            case INDEX_PAGE:
                IndexPage indexPage = new IndexPage();
                indexPage.bytes = pageBytes;
                indexPage.parse();
                page = indexPage;
                System.out.println("here is a index page");
                break;
            case EXTENT_MANAGE_PAGE:
                page = new ExtentManagePage();
                page.bytes = pageBytes;
                break;
            case DATA_PAGE:
                page = new DataPage();
                page.bytes = pageBytes;
            default:
                page = new Page();
                page.bytes = pageBytes;
                System.out.println("here is a default page");
        }
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
    public static void output(int spaceId, int pageId) throws Exception {
        // TODO: buffer lock
        String tablespaceFilename = ServerRuntime.getTablespaceFile(spaceId);
        RandomAccessFile tablespaceFile = new RandomAccessFile(tablespaceFilename, "rw");
        byte[] page = buffer.get(concat(spaceId, pageId)).bytes;
        tablespaceFile.seek(ServerRuntime.config.pageSize * ((long) 0x00000000FFFFFFFF & pageId));
        tablespaceFile.write(page, 0, ServerRuntime.config.pageSize);
        buffer.remove(concat(spaceId, pageId));
        tablespaceFile.close();
    }

    // TODO: implement this function
    /*
    public void dumpPages() {

    }
     */
}
