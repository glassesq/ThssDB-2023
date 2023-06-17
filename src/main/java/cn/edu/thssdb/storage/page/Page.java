package cn.edu.thssdb.storage.page;

import cn.edu.thssdb.communication.IO;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Basic class for all page */
public class Page {
  public SharedSuite makeSuite() {
    SharedSuite suite = new SharedSuite();
    suite.freespaceStart = this.freespaceStart;
    suite.bLinkTreeLatch = this.bLinkTreeLatch;
    suite.bytes = this.bytes;
    suite.infimumRecord = this.infimumRecord;
    suite.pageWriteAndOutputLatch = this.pageWriteAndOutputLatch;
    suite.pageReadAndWriteLatch = this.pageReadAndWriteLatch;
    suite.maxPageId = this.maxPageId;
    suite.counter = 1;
    suite.isDirty = this.isDirty;
    return suite;
  }

  public AtomicInteger freespaceStart = new AtomicInteger();
  public ReentrantReadWriteLock pageReadAndWriteLatch = new ReentrantReadWriteLock();
  public ReentrantLock pageWriteAndOutputLatch = new ReentrantLock();

  /* FIL Header */
  /**
   * checksum of the page.
   *
   * @deprecated not used right now for performance consideration.
   */
  int checksum;

  /** spaceId is a 4-byte Unsigned Integer. It stored in binary using big-endian. */
  public int spaceId;

  /** pageId is a 4-byte POSITIVE Integer. It stored in binary using big-endian. */
  public int pageId;

  /** 8-byte LSN (reserved). */
  long LSN;

  /**
   * 4-byte previous(left) pageId, used in B-link-tree.
   *
   * @deprecated
   */
  protected int previousPageId;

  /**
   * 4-byte next(right) pageId, used in B-link-tree.
   *
   * @deprecated
   */
  protected int nextPageId;

  /** the type of current page. OVERALL(0), INDEX_ROOT(2), DATA(3), */
  public int pageType;

  public static final int OVERALL_PAGE = 0;
  public static final int INDEX_PAGE = 2;
  public static final int DATA_PAGE = 3;

  /* raw bytes of this page. */
  public byte[] bytes;

  public ReentrantLock bLinkTreeLatch = new ReentrantLock();

  public IndexPage.RecordInPage infimumRecord;

  public AtomicInteger maxPageId = null;

  public AtomicBoolean isDirty = new AtomicBoolean(false);

  public Page(byte[] bytes) {
    this.bytes = bytes;
    parseFILHeader();
  }

  public int parseIntegerBig(int pos) {
    return ((bytes[pos] & 0xFF) << 24)
        | ((bytes[pos + 1] & 0xFF) << 16)
        | ((bytes[pos + 2] & 0xFF) << 8)
        | (bytes[pos + 3] & 0xFF);
  }

  public long parseLongBig(int pos) {
    return Integer.toUnsignedLong(parseIntegerBig(pos)) << 32 | parseIntegerBig(pos + 4);
  }

  public int parseShortBig(int pos) {
    return ((bytes[pos] & 0xFF) << 8) | (bytes[pos + 1] & 0xFF);
  }

  public long parseSevenByteBig(int pos) {
    return (Integer.toUnsignedLong(parseIntegerBig(pos)) << 24)
        | (Integer.toUnsignedLong(parseShortBig(pos + 4)) << 8)
        | Integer.toUnsignedLong(bytes[pos + 6] & 0xFF);
  }

  /** parse FIL Header */
  protected void parseFILHeader() {
    /* CHECKSUM FOR 4 BYTE */
    spaceId = parseIntegerBig(4);
    pageId = parseIntegerBig(8);
    pageType = parseShortBig(12);
    /* RESERVED FOR 2 BYTE */
    LSN = parseLongBig(16);
    previousPageId = parseIntegerBig(24);
    nextPageId = parseIntegerBig(28);
  }

  /**
   * Write FIL Header on both disk buffer and WAL log buffer.
   *
   * @param transactionId transactionId
   */
  public void writeFILHeader(long transactionId) {
    byte[] newValue = new byte[32];
    newValue[4] = (byte) (spaceId >> 24);
    newValue[5] = (byte) (spaceId >> 16);
    newValue[6] = (byte) (spaceId >> 8);
    newValue[7] = (byte) (spaceId);
    newValue[8] = (byte) (pageId >> 24);
    newValue[9] = (byte) (pageId >> 16);
    newValue[10] = (byte) (pageId >> 8);
    newValue[11] = (byte) (pageId);
    newValue[12] = (byte) (pageType >> 8);
    newValue[13] = (byte) (pageType);
    /* RESERVED FOR 2 byte */
    newValue[16] = (byte) (LSN >> 56);
    newValue[17] = (byte) (LSN >> 48);
    newValue[18] = (byte) (LSN >> 40);
    newValue[19] = (byte) (LSN >> 32);
    newValue[20] = (byte) (LSN >> 24);
    newValue[21] = (byte) (LSN >> 16);
    newValue[22] = (byte) (LSN >> 8);
    newValue[23] = (byte) (LSN);
    newValue[24] = (byte) (previousPageId >> 24);
    newValue[25] = (byte) (previousPageId >> 16);
    newValue[26] = (byte) (previousPageId >> 8);
    newValue[27] = (byte) (previousPageId);
    newValue[28] = (byte) (nextPageId >> 24);
    newValue[29] = (byte) (nextPageId >> 16);
    newValue[30] = (byte) (nextPageId >> 8);
    newValue[31] = (byte) (nextPageId);
    IO.write(transactionId, this, 0, 32, newValue, false);
  }

  //  @Override
  //  protected void finalize() throws Throwable {
  //    System.out.println("finalize: " + Thread.currentThread());
  //    System.out.println("Page " + pageId + " is finalized.");
  //    System.out.println(this.pageReadAndWriteLatch);
  //    super.finalize();
  //  }

  public boolean isShadow() {
    return true;
  }

  public boolean shadow() {
    return true;
  }

  public boolean unShadow() {
    return true;
  }
}
