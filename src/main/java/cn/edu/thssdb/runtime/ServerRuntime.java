package cn.edu.thssdb.runtime;

import cn.edu.thssdb.plan.LogicalGenerator;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.storage.DiskBuffer;
import cn.edu.thssdb.storage.page.IndexPage;
import cn.edu.thssdb.storage.page.Page;
import cn.edu.thssdb.storage.writeahead.DummyLog;
import cn.edu.thssdb.utils.Pair;
import cn.edu.thssdb.utils.StatusUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.json.JSONArray;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The runtime of the database server. Every member variable and function shall be static in this
 * class.
 */
public class ServerRuntime {

  //  public static ConcurrentHashMap<Long, IndexPage.RecordInPage> father = new
  // ConcurrentHashMap<>();

  /** An array shadow of all metadata. */
  public static JSONArray metadataArray;

  /** From databaseId to databaseMetadata Object. Fast reference of metadata object. */
  public static HashMap<Integer, Database.DatabaseMetadata> databaseMetadata = new HashMap<>();

  /** From tablespaceId to tablespaceMetadata Object. Fast reference of metadata object. */
  public static HashMap<Integer, Table.TableMetadata> tableMetadata = new HashMap<>();

  /** From databaseName to databaseId Object. Fast reference of metadata object. */
  public static HashMap<String, Integer> databaseNameLookup = new HashMap<>();

  /** A map from {@code sessionId} to {@code SessionRuntime} */
  public static HashMap<Long, SessionRuntime> sessions = new HashMap<>();

  private static final AtomicLong sessionCounter = new AtomicLong(0);

  private static final AtomicLong transactionCounter = new AtomicLong(0);

  /** A map from {@code transactionId} to the locks it holds. */
  public static ConcurrentHashMap<Long, ArrayList<Lock>> locks = new ConcurrentHashMap<>();

  public static ConcurrentHashMap<Long, HashSet<Page>> persistPage = new ConcurrentHashMap<>();

  private static final AtomicInteger tablespaceCounter = new AtomicInteger(0);

  private static final AtomicInteger databaseCounter = new AtomicInteger(0);
  /** Configuration of the whole server. */
  public static final Configuration config = new Configuration();

  /**
   * transaction get a two phase lock
   *
   * @param transactionId transaction id
   * @param lock lock
   */
  private static void getTwoPhaseLock(long transactionId, Lock lock) {
    lock.lock();
    locks.get(transactionId).add(lock);
  }

  /**
   * transaction get a write-lock
   *
   * @param transactionId transaction id
   * @param lock lock
   */
  public static void getWriteLock(
      long transactionId, ReentrantReadWriteLock lock, IndexPage tracePage) {
    // TODO: save pages because of soft referenced cache.
    if (lock == null) return;
    persistPage.putIfAbsent(transactionId, new HashSet<>());
    persistPage.get(transactionId).add(tracePage);
    getTwoPhaseLock(transactionId, lock.writeLock());
  }

  /**
   * transaction get a read lock
   *
   * @param transactionId transaction id
   * @param lock lock
   */
  public static void getReadLock(
      long transactionId, ReentrantReadWriteLock lock, IndexPage tracePage) {
    if (lock == null) return;
    if (config.serializable) {
      if (!persistPage.containsKey(transactionId)) persistPage.put(transactionId, new HashSet<>());
      persistPage.get(transactionId).add(tracePage);
      //      getTwoPhaseLock(transactionId, lock.readLock());
      /** to avoid deadlock in TransactionTest */
      getTwoPhaseLock(transactionId, lock.writeLock());
    } else {
      // TODO: deadlock check
      lock.readLock().lock();
    }
  }

  /**
   * release a read lock when serializable is off
   *
   * @param lock lock
   */
  public static void releaseReadLock(ReentrantReadWriteLock lock) {
    if (lock == null) return;
    if (!config.serializable && lock.getReadLockCount() > 0) {
      lock.readLock().unlock();
    }
  }

  /**
   * transaction release all locks
   *
   * @param transactionId transaction id
   */
  public static void releaseAllLocks(long transactionId) {
    if (locks.get(transactionId) == null) return;
    ArrayList<Lock> lockToRelease = locks.get(transactionId);
    locks.remove(transactionId);
    for (Lock lock : lockToRelease) {
      lock.unlock();
    }
    persistPage.remove(transactionId);
  }

  /**
   * increase transaction_counter by one
   *
   * @return 8-byte new transaction id (unused).
   * @throws IllegalStateException the transaction counter is exhausted.
   */
  public static long newTransaction() throws IllegalStateException {
    long tid = transactionCounter.incrementAndGet();
    if (tid == Long.MAX_VALUE) {
      throw new IllegalStateException(
          "The transaction counter is exhausted. Please restart the server. ");
    }
    locks.put(tid, new ArrayList<>());
    return tid;
  }

  /**
   * increase database_counter by one
   *
   * @return 4-byte new database id (unused).
   * @throws IllegalStateException the database counter is exhausted.
   */
  public static int newDatabase() throws IllegalStateException {
    int did = databaseCounter.incrementAndGet();
    if (did == Integer.MAX_VALUE) {
      throw new IllegalStateException("The database counter is exhausted.");
    }
    return did;
  }

  /**
   * increase tablespace counter by one.
   *
   * @return 4-byte tablespace id (unused).
   * @throws IllegalStateException the tablespace counter is exhausted.
   */
  public static int newTablespace() throws IllegalStateException {
    // TODO: tablespace id can be reused
    int tid = tablespaceCounter.incrementAndGet();
    if (tid == Integer.MAX_VALUE) {
      throw new IllegalStateException("The tablespace id is exhausted. Out of capability.");
    }
    return tid;
  }

  /**
   * increase the session counter by one and prepare SessionRuntime.
   *
   * @return 8-byte session id of which the sessionRuntime is prepared.
   */
  public static long newSession() {
    long sessionId = sessionCounter.incrementAndGet();
    SessionRuntime sessionRuntime = new SessionRuntime();
    sessions.put(sessionId, sessionRuntime);
    sessionRuntime.sessionId = sessionId;
    System.out.println(Thread.currentThread() + " " + sessionId);
    return sessionId;
  }

  /**
   * close a session.
   *
   * @param sessionId for the session to be closed.
   */
  public static void closeSession(long sessionId) {
    SessionRuntime sessionRuntime = sessions.get(sessionId);
    if (sessionRuntime != null) sessionRuntime.stop();
    sessions.remove(sessionId);
  }

  /**
   * check if the session exists.
   *
   * @param sessionId the session.
   * @return true or false.
   */
  public static boolean checkForSession(long sessionId) {
    SessionRuntime sessionRuntime = sessions.get(sessionId);
    return sessionRuntime != null;
  }

  /**
   * get the absolute path of the tablespace file
   *
   * @param spaceId tablespace
   * @return absolute path of the tablespace file
   */
  public static String getTablespaceFile(int spaceId) {
    // TODO: read from metadata.
    // TODO: REPLACE FOR TEST
    return ServerRuntime.config.tablespacePath + "/tablespace" + spaceId + ".tablespace";
  }

  /**
   * run plan in the session.
   *
   * @param sessionId the session.
   * @param plan the logical plan.
   * @return executeStatementResp.
   */
  public static ExecuteStatementResp runPlan(long sessionId, LogicalPlan plan, String statement) {
    SessionRuntime sessionRuntime = sessions.get(sessionId);
    if (sessionRuntime == null) {
      return new ExecuteStatementResp(
          StatusUtil.fail(
              "SessionRuntime does not exist for session"
                  + sessionId
                  + ". Uncommitted actions shall be automatically aborted. Please connect to the server again."),
          false);
    }
    return sessionRuntime.runPlan(plan, statement);
  }

  /**
   * recover from an empty database according to the DummyLog
   *
   * @throws Exception IO error
   */
  public static void recoverFromDummyLog() throws Exception {
    File sourceFile = new File(ServerRuntime.config.DummyLogFilename);
    File targetFile = new File(ServerRuntime.config.DummyLogRecoverFilename);
    FileUtils.copyFile(sourceFile, targetFile);
    sourceFile.delete();
    File metadataFile = new File(config.MetadataFilename);
    metadataFile.delete();

    setup();

    targetFile = new File(ServerRuntime.config.DummyLogRecoverFilename);

    Pair<Long, String> operation;
    LineIterator iterator = FileUtils.lineIterator(targetFile, "UTF-8");
    String message;
    while (true) {
      if (iterator.hasNext()) {
        message = iterator.nextLine();
        System.out.println(message);
      } else break;
      if (message == null) break;
      operation = DummyLog.getStatementFromLog(message);
      if (operation == null) continue;
      while (sessionCounter.get() < operation.left) {
        newSession();
      }
      LogicalPlan plan = LogicalGenerator.generate(operation.right);
      ServerRuntime.runPlan(operation.left, plan, operation.right);
    }
  }

  /**
   * setup the server. The lock is <b>not</b> required right now. It is under start-up process.
   * Multiple transactions are impossible.
   *
   * @throws Exception create WALFile failed.
   */
  public static void setup() throws Exception {
    File testDir = new File(ServerRuntime.config.tablespacePath);
    testDir.mkdirs();

    if (config.useDummyLog) {
      File DummyFile = new File(config.DummyLogFilename);
      DummyFile.createNewFile();
      if (!DummyFile.exists()) throw new Exception("We cannot create WAL file.");
      DummyLog.writer = new BufferedWriter(new FileWriter(config.DummyLogFilename, true));
    } else {
      File WALFile = new File(config.WALFilename);
      WALFile.createNewFile();
      if (!WALFile.exists()) throw new Exception("We cannot create WAL file.");
    }

    /* memory monitor, can be commented for real use */
    Timer timer = new Timer();
    timer.schedule(new DiskBuffer.MemoryMonitor(), 0, 2000);
    /* Throw pages out of memory. */
    new Thread(DiskBuffer::throwPages).start();
    new Thread(DiskBuffer::throwPages).start();
    new Thread(DiskBuffer::throwPages).start();
    new Thread(DiskBuffer::throwPages).start();
    new Thread(DiskBuffer::throwPages).start();
    /* Checkpoint maker */
    new Thread(
            () -> {
              while (true) {
                long lastTransaction = transactionCounter.get();
                try {
                  /* TODO: busy waiting */
                  Thread.sleep(1000);
                } catch (Exception ignored) {
                }
                if (lastTransaction == transactionCounter.get()) {
                  DiskBuffer.buffer.invalidateAll();
                  System.gc();
                }
              }
            })
        .start();

    File metadataFile = new File(config.MetadataFilename);
    if (!metadataFile.exists()) {
      metadataFile.createNewFile();
      if (!metadataFile.exists()) throw new Exception("We cannot find or create metadata file.");
      FileOutputStream metadataStream = new FileOutputStream(config.MetadataFilename);
      metadataStream.write("[]".getBytes());
      metadataStream.close();
      metadataArray = new JSONArray();
    } else {
      FileInputStream metadataStream = new FileInputStream(config.MetadataFilename);
      byte[] metadataBytes = new byte[(int) metadataFile.length()];
      metadataStream.read(metadataBytes, 0, metadataBytes.length);
      metadataStream.close();
      String metadataString = new String(metadataBytes, StandardCharsets.UTF_8);
      metadataArray = new JSONArray(metadataString);
      for (int i = 0; i < metadataArray.length(); i++) {
        Database.DatabaseMetadata m =
            Database.DatabaseMetadata.createDatabaseMetadata(metadataArray.getJSONObject(i));
        databaseMetadata.put(m.databaseId, m);
        databaseNameLookup.put(m.name, m.databaseId);
        if (databaseCounter.intValue() < m.databaseId) databaseCounter.set(m.databaseId);
        for (Integer k : m.tables.keySet()) {
          tableMetadata.put(k, m.tables.get(k));
          if (tablespaceCounter.intValue() < m.tables.get(k).spaceId)
            tablespaceCounter.set(m.tables.get(k).spaceId);
        }
      }
    }
  }
}
