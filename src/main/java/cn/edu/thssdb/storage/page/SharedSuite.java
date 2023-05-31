package cn.edu.thssdb.storage.page;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SharedSuite {
  public ReentrantReadWriteLock pageReadAndWriteLatch = null;
  public ReentrantLock pageWriteAndOutputLatch = null;

  public byte[] bytes = null;

  public ReentrantLock firstSplitLatch = null;

  public ReentrantLock bLinkTreeLatch = null;

  public IndexPage.RecordInPage infimumRecord = null;

  public int counter;

  public ReentrantLock suiteLock = new ReentrantLock();

  public AtomicInteger maxPageId = null;
}
