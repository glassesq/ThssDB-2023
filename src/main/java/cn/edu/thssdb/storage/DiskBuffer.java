package cn.edu.thssdb.storage;

import cn.edu.thssdb.runtime.ServerRuntime;
import cn.edu.thssdb.storage.page.*;
import cn.edu.thssdb.storage.writeahead.DummyLog;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import static cn.edu.thssdb.runtime.ServerRuntime.persistPage;
import static cn.edu.thssdb.storage.page.Page.*;
import static java.lang.System.exit;

public class DiskBuffer {

  static HashMap<String, FileChannel> outputChannel = new HashMap<>();

  public static class MemoryMonitor extends TimerTask {
    public void run() {
      System.out.println(
          persistPage.size() + " " + recoverArea.size() + " " + buffer.estimatedSize());
      System.out.println(Runtime.getRuntime().freeMemory());
    }
  }

  public static int blockingFactor = 1;
  /** reference queue of page object that are removed by GC. */
  public static final ReferenceQueue<Page> referenceQueue = new ReferenceQueue<>();
  /** ConcurrentHashMap from concat(spaceId, pageId) to share suite. */
  public static final ConcurrentHashMap<Long, SharedSuite> recoverArea = new ConcurrentHashMap<>();
  /** ConcurrentHashMap from weak reference to concat(spaceId, pageId) */
  public static final ConcurrentHashMap<PhantomReference<Page>, Long> throwSet =
      new ConcurrentHashMap<>();

  public static long lastCheckpointVersion = -1;

  /** throw out pages to disk and remove them from memory */
  public static void throwPages() {
    try {
      Reference<?> ref;
      while (true) {
        ref = referenceQueue.poll();
        if (ref == null) {
          /* There are no weak reference in the poll. */
          //          System.gc();
          Thread.sleep(0, 10);
        } else {
          Long key = throwSet.get(ref);
          throwSet.remove(ref);
          if (key == null) continue;
          SharedSuite suite = recoverArea.get(key);
          if (suite == null) continue;
          suite.suiteLock.lock();
          //          System.out.println("suite count of " + key.intValue() + " is " +
          // suite.counter);
          if (--suite.counter == 0) {
            try {
              DiskBuffer.output(key, suite);
            } catch (Exception e) {
              e.printStackTrace();
              exit(45);
            }
            recoverArea.remove(key);
            suite.suiteLock.unlock();
            if (DiskBuffer.recoverArea.size() == 0) {
              /*
               * Checkpoint can be written safely. This is because pages are flushed to disk only by
               * this thread. Any new modifications on data and write logs are held until this
               * method finished.
               */
              DummyLog.dummyLogOutputLock.lock();
              if (DiskBuffer.recoverArea.size() != 0) {
                DummyLog.dummyLogOutputLock.unlock();
              } else {
                lastCheckpointVersion++;
                File srcDirectory = new File(ServerRuntime.config.tablespacePath);
                File dstDirectory =
                    new File(ServerRuntime.config.testPathRecover + lastCheckpointVersion);
                FileUtils.copyDirectory(srcDirectory, dstDirectory);
                DummyLog.writeDummyLog(-2, "checkpoint" + lastCheckpointVersion);
                DummyLog.outputDummyLogToDisk();
              }
              DummyLog.dummyLogOutputLock.unlock();
            }
          } else {
            suite.suiteLock.unlock();
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      exit(44);
    }
  }

  /**
   * map from {@code spID = spaceId[4byte]-pageID[4byte] } to a page class. !important: Buffer shall
   * be accessed with diskBufferLatch.
   */
  public static final LoadingCache<Long, Page> buffer =
      Caffeine.newBuilder()
          .maximumSize(ServerRuntime.config.bufferSize)
          .removalListener(
              (Long key, Page value, com.github.benmanes.caffeine.cache.RemovalCause cause) -> {
                //                int spaceId = (int) (key >> 32);
                //                int pageId = key.intValue();
                //                System.out.println("output " + spaceId + " " + pageId);
              })
          .build(
              key -> {
                if (Runtime.getRuntime().freeMemory() <= ServerRuntime.config.warningMemory) {
                  // TODO: refactor.
                  // better thread sleep control for memory limitation.
                  // replace busy waiting with something faster
                  System.gc();
                  blockingFactor = blockingFactor << 1;
                  int sleepTime = blockingFactor * 5;
                  Thread.sleep(0, sleepTime);
                  while (Runtime.getRuntime().freeMemory() <= ServerRuntime.config.warningMemory) {
                    if (sleepTime / 1000 > 50) {
                      if (ThreadLocalRandom.current().nextFloat() < 0.05) break;
                    } else {
                      sleepTime = sleepTime * 2;
                    }
                    System.out.println("sleep!");
                    Thread.sleep(sleepTime / 1000, sleepTime % 1000);
                    System.gc();
                  }
                  blockingFactor >>= 1;
                }
                Page page;
                SharedSuite suite = recoverArea.get(key);
                if (suite == null) {
                  /* no shared suite currently stores in memory */
                  page = input(key);
                  int spaceId = (int) (key >> 32);
                  int pageId = key.intValue();
                  //                  System.out.println("input " + spaceId + " " + pageId);
                  recoverArea.put(key, page.makeSuite());
                } else {
                  /* The corresponding share suite currently stores in memory, therefore we recover from the shared suite. */
                  int spaceId = (int) (key >> 32);
                  int pageId = key.intValue();
                  //                  System.out.println("recover " + spaceId + " " + pageId);
                  page = recover(suite);
                  suite.suiteLock.lock();
                  //                  System.out.println("[recover] suite count of " +
                  // key.intValue() + " is " + suite.counter);
                  if (++suite.counter == 1) {
                    /* optimistic locking:
                    the shared suite has been removed asynchronously from memory just after we retrieve it from recoverArea. */
                    recoverArea.put(key, suite);
                  }
                  suite.suiteLock.unlock();
                }
                throwSet.put(new PhantomReference<>(page, referenceQueue), key);
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
    return buffer.get(key);
  }

  /**
   * put page to buffer, the buffer is thread-safe
   *
   * @param page page object
   */
  public static void putToBuffer(Page page) {
    recoverArea.put(concat(page.spaceId, page.pageId), page.makeSuite());
    //    System.out.println("first pin " + page.spaceId + " " + page.pageId);
    throwSet.put(new PhantomReference<>(page, referenceQueue), concat(page.spaceId, page.pageId));
    buffer.put(concat(page.spaceId, page.pageId), page);
  }

  /**
   * read a page from disk to buffer.
   *
   * @param key concat(spaceId, pageId)
   * @throws Exception if the reading process fails.
   */
  public static Page input(Long key) throws Exception {
    int spaceId = (int) (key >> 32);
    int pageId = key.intValue();
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
        break;
      case INDEX_PAGE:
        page = new IndexPage(pageBytes, false);
        break;
      default:
        page = new Page(pageBytes);
    }
    tablespaceFile.close();

    return page;
  }

  /**
   * recover a page from sharedSuite in {@code recoverArea}.
   *
   * @param sharedSuite shared suite
   * @return recovered page
   */
  public static Page recover(SharedSuite sharedSuite) {
    byte[] pageBytes = sharedSuite.bytes;
    int pageType = ((int) pageBytes[12] << 8) | pageBytes[13];
    Page page;
    switch (pageType) {
      case OVERALL_PAGE:
        page = new OverallPage(pageBytes);
        break;
      case INDEX_PAGE:
        page = new IndexPage(pageBytes, true);
        break;
      default:
        page = new Page(pageBytes);
    }
    page.pageReadAndWriteLatch = sharedSuite.pageReadAndWriteLatch;
    page.pageWriteAndOutputLatch = sharedSuite.pageWriteAndOutputLatch;
    page.firstSplitLock = sharedSuite.firstSplitLatch;
    page.bLinkTreeLatch = sharedSuite.bLinkTreeLatch;
    page.infimumRecord = sharedSuite.infimumRecord;
    page.maxPageId = sharedSuite.maxPageId;
    page.freespaceStart = sharedSuite.freespaceStart;

    return page;
  }

  /**
   * Output a shared suite to disk.
   *
   * @param key concat(spaceId, pageId)
   * @param suite shared suite
   * @throws Exception IO error
   */
  public static void output(Long key, SharedSuite suite) throws Exception {
    int spaceId = (int) (key >> 32);
    int pageId = key.intValue();
    String tablespaceFilename = ServerRuntime.getTablespaceFile(spaceId);
    FileChannel channel = outputChannel.get(tablespaceFilename);
    if (channel == null) {
      RandomAccessFile tablespaceFile = new RandomAccessFile(tablespaceFilename, "rw");
      channel = tablespaceFile.getChannel();
      outputChannel.put(tablespaceFilename, channel);
    }
    channel.position(ServerRuntime.config.pageSize * ((long) 0x00000000FFFFFFFF & pageId));
    ByteBuffer buf = ByteBuffer.allocate(ServerRuntime.config.pageSize);
    buf.put(suite.bytes);
    buf.flip();
    channel.write(buf);
    channel.force(false);
  }
}
