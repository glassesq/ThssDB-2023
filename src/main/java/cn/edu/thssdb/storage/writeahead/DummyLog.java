package cn.edu.thssdb.storage.writeahead;

import cn.edu.thssdb.runtime.ServerRuntime;
import cn.edu.thssdb.utils.Pair;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DummyLog {
  public static BufferedWriter writer;
  public static ConcurrentLinkedQueue<String> dummyLogList = new ConcurrentLinkedQueue<>();
  public static ReentrantLock dummyLogOutputLock = new ReentrantLock();
  /**
   * current number of logs which are put into buffer. There should always be {@code dirtyCounter <=
   * checkCounter}
   */
  public static AtomicInteger dirtyCounter = new AtomicInteger(0);
  /**
   * current number of logs which are outputted. There should always be {@code dirtyCounter <=
   * checkCounter}
   */
  public static AtomicInteger checkCounter = new AtomicInteger(0);

  public static Pattern sessionIdPattern = Pattern.compile("^(-?\\d+):(-?\\d+):success:(.*)");

  /**
   * Check if this log need to be replayed and parse statement from it
   *
   * @param log log string
   * @return a pair. The left is sessionId, the right is statement to be executed.
   */
  public static Pair<Long, String> getStatementFromLog(String log) {
    Matcher matcher = sessionIdPattern.matcher(log);
    if (!matcher.matches()) return null;
    try {
      long sessionId = Long.parseLong(matcher.group(2));
      if (sessionId == -1) return null;
      return new Pair<>(sessionId, matcher.group(3));
    } catch (Exception ignored) {
    }
    return null;
  }

  /**
   * write dummy log to buffer
   *
   * @param transactionId tID
   * @param msg message log
   * @return current dirtyCounter pointer
   */
  public static int writeDummyLog(long transactionId, String msg) {
    dummyLogList.offer(transactionId + ":" + msg + "\n");
    return dirtyCounter.incrementAndGet();
  }

  /** output dummy log from buffer to disk */
  public static void outputDummyLogToDisk() throws Exception {
    String log;
    dummyLogOutputLock.lock();
    if (writer == null)
      writer = new BufferedWriter(new FileWriter(ServerRuntime.config.DummyLogFilename, true));
    while (true) {
      log = dummyLogList.poll();
      if (log == null) break;
      writer.write(log);
      checkCounter.incrementAndGet();
    }
    writer.flush();
    dummyLogOutputLock.unlock();
  }
}
