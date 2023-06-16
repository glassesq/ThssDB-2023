package cn.edu.thssdb.communication;

import cn.edu.thssdb.runtime.ServerRuntime;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.storage.DiskBuffer;
import cn.edu.thssdb.storage.page.Page;
import cn.edu.thssdb.storage.writeahead.DummyLog;
import cn.edu.thssdb.storage.writeahead.WriteLog;

import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static cn.edu.thssdb.runtime.ServerRuntime.config;
import static cn.edu.thssdb.storage.writeahead.WriteLog.*;
import static java.lang.System.exit;

public class IO {
  static HashSet<Long> dirtyPages = new HashSet<>();

  static ReentrantReadWriteLock dirtyPageLatch = new ReentrantReadWriteLock();

  /**
   * This method can be used safely due to the presence of locks.
   *
   * @param spaceId spaceId
   * @param pageId pageId
   * @return not parsed Page
   */
  public static Page read(int spaceId, int pageId) {
    try {
      return DiskBuffer.read(spaceId, pageId);
    } catch (Exception e) {
      e.printStackTrace();
      exit(66);
    }
    return null;
  }

  /**
   * This method write values on both disk buffer and the WAL log buffer. Can be used safely due to
   * the presence of locks.
   *
   * @param transactionId transaction that requests this write
   * @param page page reference
   * @param offset offset
   * @param length length of bytes to write
   * @param newValue bytes value to write
   * @param redo_only whether it is redo-only
   */
  public static void write(
      long transactionId, Page page, int offset, int length, byte[] newValue, boolean redo_only) {

    if (config.useDummyLog) {
      page.pageWriteAndOutputLatch.lock();
      for (int i = offset, s = 0; i < offset + length; i++, s++) {
        page.bytes[i] = newValue[s];
      }
      page.isDirty.set(true);
      page.pageWriteAndOutputLatch.unlock();
    } else {
      /* Write the changes to disk buffer */
      /* Latch for page reading is not needed because of our design avoid reading from bytes directly. */

      dirtyPageLatch.readLock().lock();
      /* avoid writing and outputting page simultaneously */
      page.pageWriteAndOutputLatch.lock();

      boolean dirty = false;
      byte[] oldValue = new byte[0];
      byte[] realNewValue = new byte[length]; /* in case newValue.length != length */
      if (!redo_only) {
        oldValue = new byte[length];
        for (int i = offset, s = 0; i < offset + length; i++, s++) {
          if (page.bytes[i] != newValue[s]) {
            dirty = true;
          }
          oldValue[s] = page.bytes[i];
          page.bytes[i] = newValue[s];
          realNewValue[s] = newValue[s];
        }
      }

      /* Write-Ahead Log */
      /* only add write log when there is actually change. */
      if (dirty) {
        WriteLog.addCommonLog(
            transactionId, page.spaceId, page.pageId, offset, length, oldValue, realNewValue);
        dirtyPages.add(DiskBuffer.concat(page.spaceId, page.pageId));
      }

      /* avoid writing and outputting page simultaneously */
      /* we slightly delay the release of this latch. This is to avoid reversing of the Write-ahead log's order. */
      page.pageWriteAndOutputLatch.unlock();

      dirtyPageLatch.readLock().unlock();
    }
  }

  /** Push every log in WAL buffer and mark relevant pages as dirty. */
  private static void pushWriteAheadLogOnly() throws Exception {
    /* push all write log records in buffer to disk */
    if (config.useDummyLog) {
      DummyLog.outputDummyLogToDisk();
    } else {
      WriteLog.outputWriteLogToDisk(true);
    }
  }

  /**
   * checkpoint push all updates to log file, data file and metadata file
   *
   * @throws Exception IO error
   */
  public static void pushAndWriteCheckpoint() throws Exception {
    if (config.useDummyLog) {
      /* DO NOTHING! */
      // TODO
    } else {
      //      writeLogFileLatch.lock();
      //
      //      /* make sure there will be no writing logs here. */
      //      dirtyPageLatch.writeLock().lock();
      //
      //      HashSet<Long> shadows = new HashSet<>(dirtyPages);
      //      for (Long spId : shadows) {
      //        DiskBuffer.getFromBuffer(spId).pageWriteAndOutputLatch.lock();
      //      }
      //      dirtyPages.clear();
      //
      //      WriteLog.outputWriteLogToDisk(false);
      //
      //      dirtyPageLatch.writeLock().unlock();
      //
      //      /* output all dirty relevant pages to disk */
      //      for (Long spId : shadows) {
      //        DiskBuffer.output((int) (spId >> 32), spId.intValue());
      //      }
      //
      //      // TODO: metadata Latch
      //      /* output all metadata to disk */
      //      FileOutputStream metadataStream = new FileOutputStream(config.MetadataFilename);
      //
      // metadataStream.write(ServerRuntime.metadataArray.toString().getBytes(StandardCharsets.UTF_8));
      //      metadataStream.close();
      //      // TODO: metadata Latch
      //
      //      /* write checkpoint */
      //      WriteLog.WriteLogEntry entry = new WriteLog.WriteLogEntry(-1, CHECKPOINT_LOG);
      //      stream = new FileOutputStream(ServerRuntime.config.WALFilename, true);
      //      entry.writeToDisk();
      //      stream.close();
      //
      //      writeLogFileLatch.unlock();
    }
  }

  public static void writeTransactionStart(long sessionId, long transactionId) {
    if (config.useDummyLog) {
      DummyLog.writeDummyLog(transactionId, "transaction start for session " + sessionId + ".");
    } else {
      WriteLog.addSpecialLog(transactionId, WriteLog.START_LOG);
    }
  }

  public static void writeDummyStatementLog(long transactionId, String statement) {
    DummyLog.writeDummyLog(transactionId, statement);
  }

  public static void writeCreateDatabase(long transactionId, String name, int databaseId)
      throws Exception {
    if (config.useDummyLog) {
      DummyLog.writeDummyLog(transactionId, "create database " + name + " with id " + databaseId);

      Database.DatabaseMetadata.metaDataLatch.readLock().lock();
      FileOutputStream metadataStream = new FileOutputStream(config.MetadataFilename);
      metadataStream.write(ServerRuntime.metadataArray.toString().getBytes(StandardCharsets.UTF_8));
      metadataStream.close();
      Database.DatabaseMetadata.metaDataLatch.readLock().unlock();
    } else {
      WriteLog.addSpecialDatabaseLog(
          transactionId,
          WriteLog.CREATE_DATABASE_LOG,
          databaseId,
          name.getBytes(StandardCharsets.UTF_8));
    }
  }

  public static void writeDropDatabase(long transactionId, String name, int databaseId)
      throws Exception {
    if (config.useDummyLog) {
      DummyLog.writeDummyLog(transactionId, "drop database " + name + " with id" + databaseId);
      Database.DatabaseMetadata.metaDataLatch.readLock().lock();
      FileOutputStream metadataStream = new FileOutputStream(config.MetadataFilename);
      metadataStream.write(ServerRuntime.metadataArray.toString().getBytes(StandardCharsets.UTF_8));
      metadataStream.close();
      Database.DatabaseMetadata.metaDataLatch.readLock().unlock();
    } else {
      WriteLog.addSpecialDatabaseLog(
          transactionId,
          WriteLog.DELETE_DATABASE_LOG,
          databaseId,
          name.getBytes(StandardCharsets.UTF_8));
    }
  }

  public static void writeCreateTable(
      long transactionId, int databaseId, Table.TableMetadata metadata) throws Exception {
    if (config.useDummyLog) {
      DummyLog.writeDummyLog(
          transactionId, "create table " + metadata.toString() + " in database " + databaseId);
      Database.DatabaseMetadata.metaDataLatch.readLock().lock();
      FileOutputStream metadataStream = new FileOutputStream(config.MetadataFilename);
      metadataStream.write(ServerRuntime.metadataArray.toString().getBytes(StandardCharsets.UTF_8));
      metadataStream.close();
      Database.DatabaseMetadata.metaDataLatch.readLock().unlock();
    } else {
      WriteLog.addCreateTableLog(transactionId, databaseId, metadata);
    }
  }

  public static void writeDropTable(
      long transactionId, int databaseId, Table.TableMetadata metadata) throws Exception {
    if (config.useDummyLog) {
      DummyLog.writeDummyLog(
          transactionId, "drop table " + metadata.toString() + " in database " + databaseId);
      Database.DatabaseMetadata.metaDataLatch.readLock().lock();
      FileOutputStream metadataStream = new FileOutputStream(config.MetadataFilename);
      metadataStream.write(ServerRuntime.metadataArray.toString().getBytes(StandardCharsets.UTF_8));
      metadataStream.close();
      Database.DatabaseMetadata.metaDataLatch.readLock().unlock();
    } else {
      // WriteLog.addCreateTableLog(transactionId, databaseId, metadata);
    }
  }

  /**
   * transaction requests a commit TODO: this implementation is only for test. The current
   * implementation is unsafe, incorrect and has poor performance.
   *
   * @param transactionId transaction
   */
  public static void pushTransactionCommit(long transactionId) {
    try {
      if (config.useDummyLog) {
        int stamp = DummyLog.writeDummyLog(transactionId, "transaction commit.");
        if (DummyLog.checkCounter.get() < stamp)
          pushWriteAheadLogOnly(/* TODO: transactionId (maybe not output all.) */ );
      } else {
        WriteLog.addSpecialLog(transactionId, WriteLog.COMMIT_LOG);
        pushWriteAheadLogOnly(/* TODO: transactionId (maybe not output all.) */ );
      }
      /* FOR TEST ONLY */
      pushAndWriteCheckpoint();
    } catch (Exception e) {
      e.printStackTrace();
      exit(63);
    }
  }

  /**
   * trace a newly created page object. this method is lock/latch free. Because it is impossible for
   * multiple transactions to create two different pages with the same Page object.
   *
   * @param page page to be traced
   */
  public static void traceNewPage(Page page) {
    DiskBuffer.putToBuffer(page);
  }
}
