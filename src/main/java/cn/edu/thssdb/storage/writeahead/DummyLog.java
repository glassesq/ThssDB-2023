package cn.edu.thssdb.storage.writeahead;

import cn.edu.thssdb.runtime.ServerRuntime;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class DummyLog {
  public static BufferedWriter writer;
  public static ConcurrentLinkedQueue<String> lockFreeList = new ConcurrentLinkedQueue<>();
  public static ReentrantLock lock = new ReentrantLock();
  public static AtomicInteger dirtyCounter = new AtomicInteger(0);
  public static AtomicInteger checkCounter = new AtomicInteger(0);

  public static int writeDummyLog(long transactionId, String msg) {
    lockFreeList.offer(transactionId + ":" + msg + "\n");
    return dirtyCounter.incrementAndGet();
  }

  public static void outputDummyLogToDisk() throws Exception {
    String log;
    lock.lock();
    if (writer == null)
      writer = new BufferedWriter(new FileWriter(ServerRuntime.config.DummyLogFilename, true));
    while (true) {
      log = lockFreeList.poll();
      if (log == null) break;
      writer.write(log);
      checkCounter.incrementAndGet();
    }
    writer.flush();
    lock.unlock();
  }
}
