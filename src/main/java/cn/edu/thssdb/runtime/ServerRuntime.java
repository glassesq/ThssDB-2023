package cn.edu.thssdb.runtime;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.utils.StatusUtil;
import org.json.JSONArray;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

/**
 * The runtime of the database server. Every member variable and function shall be static in this
 * class.
 */
public class ServerRuntime {

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
  public static HashMap<Long, ArrayList<Lock>> locks = new HashMap<>();

  private static final AtomicInteger tablespaceCounter = new AtomicInteger(0);

  private static final AtomicInteger databaseCounter = new AtomicInteger(0);
  /** Configuration of the whole server. */
  public static final Configuration config = new Configuration();

  /**
   * transaction get a lock
   * @param transactionId transaction id
   * @param lock lock
   */
  public static void getLock(long transactionId, Lock lock) {
    lock.lock();
    locks.get(transactionId).add(lock);
  }

  /**
   * transaction release all locks
   * @param transactionId transaction id
   */
  public static void releaseAllLocks(long transactionId) {
    for (Lock lock : locks.get(transactionId)) {
      lock.unlock();
    }
    locks.remove(transactionId);
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
    return ServerRuntime.config.testPath + "/tablespace" + spaceId + ".tablespace";
  }

  /**
   * run plan in the session.
   *
   * @param sessionId the session.
   * @param plan the logical plan.
   * @return executeStatementResp.
   */
  public static ExecuteStatementResp runPlan(long sessionId, LogicalPlan plan) {
    SessionRuntime sessionRuntime = sessions.get(sessionId);
    if (sessionRuntime == null) {
      return new ExecuteStatementResp(
          StatusUtil.fail(
              "SessionRuntime does not exist for session"
                  + sessionId
                  + ". Uncommitted actions shall be automatically aborted. Please connect to the server again."),
          false);
    }
    return sessionRuntime.runPlan(plan);
  }

  /**
   * setup the server. The lock is <b>not</b> required right now. It is under start-up process.
   * Multiple transactions are impossible. TODO: recover mechanism.
   *
   * @throws Exception create WALFile failed.
   */
  public static void setup() throws Exception {
    File WALFile = new File(config.WALFilename);
    WALFile.createNewFile();
    if (!WALFile.exists()) throw new Exception("We cannot create WAL file.");
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
      /* FOR TEST */
      System.out.println("Metadata Load Successful from " + config.MetadataFilename);
      for (Integer k : databaseMetadata.keySet()) {
        System.out.println(
            "database "
                + databaseMetadata.get(k).databaseId
                + " : "
                + databaseMetadata.get(k).name
                + " with "
                + databaseMetadata.get(k).tables.size()
                + " tables");
      }
    }
  }
}
