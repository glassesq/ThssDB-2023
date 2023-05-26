package cn.edu.thssdb.storage;

import cn.edu.thssdb.runtime.ServerRuntime;
import cn.edu.thssdb.storage.page.*;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import java.io.RandomAccessFile;

import static cn.edu.thssdb.storage.page.Page.*;

public class DiskBuffer {

  /**
   * map from {@code spID = spaceId[4byte]-pageID[4byte] } to a page class. !important: Buffer shall
   * be accessed with diskBufferLatch.
   */
  public static final LoadingCache<Long, Page> buffer =
      Caffeine.newBuilder()
          .softValues()
          /*.evictionListener(
          (Long key, Page page, RemovalCause cause) -> {
              System.out.printf(
                  "SpaceId %s Page %s was evicted (%s)%n",
                  (int) (key >> 32), key.intValue(), cause))
          } */
          .build(
              key -> {
                int spaceId = (int) (key >> 32);
                int pageId = key.intValue();
                return input(spaceId, pageId);
              });

  public static long concat(int spaceId, int pageId) {
    return (Integer.toUnsignedLong(spaceId) << 32) | pageId;
  }

  /**
   * read a page from buffer this method shall be used carefully due to the lack of lock.
   *
   * @param spaceId spaceId
   * @param pageId pageId
   * @return Page object
   */
  public static Page read(int spaceId, int pageId) throws Exception {
    return getFromBuffer(concat(spaceId, pageId));
  }

  /**
   * get from buffer, the buffer is thread-safe
   *
   * @param key hashmap key
   * @return hashmap value
   */
  public static Page getFromBuffer(long key) {
    return buffer.get(key);
  }

  /**
   * put page to buffer, the buffer is thread-safe
   *
   * @param page page object
   */
  public static void putToBuffer(Page page) {
    buffer.put(concat(page.spaceId, page.pageId), page);
  }

  /**
   * read a page from disk to buffer.
   *
   * @param spaceId spaceId
   * @param pageId pageId
   * @throws Exception if the reading process fails.
   */
  public static Page input(int spaceId, int pageId) throws Exception {
    String tablespaceFilename = ServerRuntime.getTablespaceFile(spaceId);
    RandomAccessFile tablespaceFile = new RandomAccessFile(tablespaceFilename, "r");
    byte[] pageBytes = new byte[(int) ServerRuntime.config.pageSize];
    tablespaceFile.seek(ServerRuntime.config.pageSize * ((long) 0x00000000FFFFFFFF & pageId));
    int bytes = tablespaceFile.read(pageBytes, 0, ServerRuntime.config.pageSize);
    if (bytes != ServerRuntime.config.pageSize) {
      throw new Exception("read page error. Wrong length" + bytes);
    }

    int pageType = ((int) pageBytes[12] << 8) | pageBytes[13];
    Page page;
    switch (pageType) {
      case OVERALL_PAGE:
        page = new OverallPage(pageBytes);
        break;
      case INDEX_PAGE:
        page = new IndexPage(pageBytes);
        break;
      case EXTENT_MANAGE_PAGE:
        page = new ExtentManagePage(pageBytes);
        break;
      case DATA_PAGE:
        page = new DataPage(pageBytes);
      default:
        page = new Page(pageBytes);
    }

    tablespaceFile.close();
    return page;
  }

  /**
   * write a page from buffer to disk.
   *
   * @param spaceId spaceId
   * @param pageId pageId
   * @throws Exception if writing fails.
   */
  public static void output(int spaceId, int pageId) throws Exception {
    Page page = getFromBuffer(concat(spaceId, pageId));

    /* avoid writing and outputting page simultaneously */
    /* already locked in pushAndWriteCheckpoint */

    String tablespaceFilename = ServerRuntime.getTablespaceFile(spaceId);
    RandomAccessFile tablespaceFile = new RandomAccessFile(tablespaceFilename, "rw");
    byte[] pageBytes = page.bytes;
    tablespaceFile.seek(ServerRuntime.config.pageSize * ((long) 0x00000000FFFFFFFF & pageId));
    tablespaceFile.write(pageBytes, 0, ServerRuntime.config.pageSize);
    tablespaceFile.close();

    /* avoid writing and outputting page simultaneously */
    page.pageWriteAndOutputLatch.unlock();
  }
}
