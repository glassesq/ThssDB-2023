package cn.edu.thssdb.storage;

import cn.edu.thssdb.runtime.ServerRuntime;
import cn.edu.thssdb.storage.page.*;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;

import java.io.RandomAccessFile;
import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static cn.edu.thssdb.runtime.ServerRuntime.persistPage;
import static cn.edu.thssdb.storage.page.Page.*;
import static java.lang.System.exit;

public class DiskBuffer {

  public static AtomicInteger testCounter = new AtomicInteger();

  public static class Thrower extends TimerTask {
    public void run() {
      //      System.out.println("start throw pages");
      throwPages();
      //      System.out.println("start throw pages ok");
    }
  }

  public static final ReferenceQueue<Page> referenceQueue = new ReferenceQueue<>();
  public static final ReentrantLock bufferLock = new ReentrantLock();
  public static final ConcurrentHashMap<Long, SharedSuite> recoverArea = new ConcurrentHashMap<>();
  public static final ConcurrentHashMap<PhantomReference<Page>, Long> throwSet =
      new ConcurrentHashMap<>();

  public static void throwPages() {
    //    System.out.println(buffer.estimatedSize() + " " + recoverArea.size());
    //    System.out.println(Thread.currentThread() + " start throw page");
    System.out.println(persistPage.size() + " " + recoverArea.size());
    //    try {
    //      for (Long key : persistPage.keySet()) {
    //        HashSet<Page> set = persistPage.get(key);
    //        if (set == null) continue;
    //        for (Page p : set) {
    //          System.out.println(p.pageReadAndWriteLatch.readLock());
    //          System.out.println(p.pageReadAndWriteLatch.writeLock());
    //          System.out.println("key :" + key + " " + p.pageId + " " + p.pageReadAndWriteLatch);
    //        }
    //      }
    //    } catch (Exception e) {
    //      e.printStackTrace();
    //    }
    // 获取当前正在运行的线程及其持有的锁
    //    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    //    long[] threadIds = threadMXBean.getAllThreadIds();
    //    ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadIds);

    // 输出线程及其持有的锁
    //    for (ThreadInfo threadInfo : threadInfos) {
    //      System.out.println( threadInfo.getThreadName() + " " + threadInfo.getLockInfo() + " " +
    // threadInfo.getThreadState() + " ");
    //    }

    try {
      int cnt = 0;
      Reference<?> ref;
      while (true) {
        ref = referenceQueue.poll();
        if (ref == null) break;
        Long key = throwSet.get(ref);
        throwSet.remove(ref);
        if (key == null) continue;
        SharedSuite suite = recoverArea.get(key);
        if (suite == null) continue;
        int spaceId = (int) (key >> 32);
        int pageId = key.intValue();
        //        System.out.println("suitelock:" + suite.suiteLock);
        //        System.out.println("suite counter:" + suite.counter + " " + pageId);
        if (--suite.counter == 0) {
          //          System.out.println( "suite counter:" + suite.counter + " recover size:" +
          // recoverArea.size());
          try {
            DiskBuffer.output(key, suite);
          } catch (Exception e) {
            e.printStackTrace();
            exit(45);
          }
          recoverArea.remove(key);
          //          System.out.println( "remove really! spaceId:" + (int) (key >> 32) + " " +
          // key.intValue() + " " + recoverArea.size());
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      exit(44);
    }
    System.out.println(Thread.currentThread() + " start throw page ok");
  }

  /**
   * map from {@code spID = spaceId[4byte]-pageID[4byte] } to a page class. !important: Buffer shall
   * be accessed with diskBufferLatch.
   */
  public static final LoadingCache<Long, Page> buffer =
      Caffeine.newBuilder()
          .maximumSize(100)
          .removalListener(
              (Long key, Page page, RemovalCause cause) -> {
                //                System.out.println("throw out removal listener:" +
                // key.intValue());
                //                throwPages();
              })
          .build(
              key -> {
                //                System.out.println("buffer lock:" + bufferLock);
                //                System.out.println("get from buffer:" + key.intValue());
                //                System.out.println("get from buffer:" + key.intValue());
                int spaceId = (int) (key >> 32);
                int pageId = key.intValue();
                //                System.out.println("get from buffer:" + key.intValue());
                SharedSuite suite = recoverArea.get(key);
                //                System.out.println("get from buffer:" + key.intValue());
                Page page;
                if (suite == null) {
                  page = input(spaceId, pageId);
                  recoverArea.put(key, page.makeSuite());
                } else {
                  //                  suite.suiteLock.tryLock();
                  page = recover(spaceId, pageId, suite);
                  //                  System.out.println("recover counter: " + suite.counter);
                  if (++suite.counter == 1) recoverArea.put(key, suite);
                  //                  suite.suiteLock.unlock();
                }
                throwSet.put(new PhantomReference<>(page, referenceQueue), key);
                //                System.out.println("get from buffer:" + key.intValue());
                return page;
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
    Page page = buffer.get(key);
    return page;
  }

  /**
   * put page to buffer, the buffer is thread-safe
   *
   * @param page page object
   */
  public static void putToBuffer(Page page) {
    recoverArea.put(concat(page.spaceId, page.pageId), page.makeSuite());
    throwSet.put(new PhantomReference<>(page, referenceQueue), concat(page.spaceId, page.pageId));
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
      throw new Exception(
          "read page error. Wrong length" + bytes + "input:" + spaceId + " " + pageId);
    }

    int pageType = ((int) pageBytes[12] << 8) | pageBytes[13];
    Page page;
    switch (pageType) {
      case OVERALL_PAGE:
        page = new OverallPage(pageBytes);
        System.out.println(
            "input newly from page and page is "
                + page.spaceId
                + " with atomic:"
                + page.maxPageId.get());
        break;
      case INDEX_PAGE:
        page = new IndexPage(pageBytes, false);
        break;
      case DATA_PAGE:
        page = new DataPage(pageBytes);
        break;
      default:
        page = new Page(pageBytes);
    }

    tablespaceFile.close();
    return page;
  }

  public static Page recover(int spaceId, int pageId, SharedSuite sharedSuite) throws Exception {
    //    System.out.println("recover!" + spaceId + " " + pageId);
    byte[] pageBytes = sharedSuite.bytes;
    //    System.out.println( "step 1 recover!" + spaceId + " " + pageId + " " +
    // buffer.estimatedSize() + " " + recoverArea.size());
    int pageType = ((int) pageBytes[12] << 8) | pageBytes[13];
    //    System.out.println("step 2 recover!" + spaceId + " " + pageId);
    Page page;
    switch (pageType) {
      case OVERALL_PAGE:
        OverallPage opage = new OverallPage(pageBytes);
        //        opage.setup();
        page = opage;
        page.maxPageId = sharedSuite.maxPageId;
        //        System.out.println( "recover from page and page is " + page.spaceId + " with
        // atomic:" + page.maxPageId.get());
        break;
      case INDEX_PAGE:
        //        System.out.println("index page make.");
        page = new IndexPage(pageBytes, true);
        //        System.out.println("index page make ok.");
        break;
      case DATA_PAGE:
        page = new DataPage(pageBytes);
        break;
      default:
        page = new Page(pageBytes);
    }
    //    System.out.println("copy suite!");
    page.pageReadAndWriteLatch = sharedSuite.pageReadAndWriteLatch;
    page.pageWriteAndOutputLatch = sharedSuite.pageWriteAndOutputLatch;
    page.firstSplitLock = sharedSuite.firstSplitLatch;
    page.bLinkTreeLatch = sharedSuite.bLinkTreeLatch;
    page.infimumRecord = sharedSuite.infimumRecord;
    page.maxPageId = sharedSuite.maxPageId;
    //    System.out.println("copy suite end!");
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

  public static void output(Long key, SharedSuite suite) throws Exception {
    int spaceId = (int) (key >> 32);
    int pageId = key.intValue();
    String tablespaceFilename = ServerRuntime.getTablespaceFile(spaceId);
    RandomAccessFile tablespaceFile = new RandomAccessFile(tablespaceFilename, "rw");
    tablespaceFile.seek(ServerRuntime.config.pageSize * ((long) 0x00000000FFFFFFFF & pageId));
    tablespaceFile.write(suite.bytes, 0, ServerRuntime.config.pageSize);
    tablespaceFile.close();
  }

  public static void output(Page page) throws Exception {
    /* avoid writing and outputting page simultaneously */
    /* already locked in pushAndWriteCheckpoint */
    String tablespaceFilename = ServerRuntime.getTablespaceFile(page.spaceId);
    RandomAccessFile tablespaceFile = new RandomAccessFile(tablespaceFilename, "rw");
    byte[] pageBytes = page.bytes;
    tablespaceFile.seek(ServerRuntime.config.pageSize * ((long) 0x00000000FFFFFFFF & page.pageId));
    tablespaceFile.write(pageBytes, 0, ServerRuntime.config.pageSize);
    tablespaceFile.close();
  }
}
