package cn.edu.thssdb.storage.page;

import cn.edu.thssdb.communication.IO;
import cn.edu.thssdb.runtime.ServerRuntime;

import java.util.concurrent.atomic.AtomicInteger;

public class OverallPage extends Page {
  /* Tablespace Header */

  //  public int maxInitedPage;

  //  public int currentDataPage;

  public int flags;

  public OverallPage(byte[] bytes) {
    super(bytes);
    parseTablespace();
  }

  /**
   * create an overall page
   *
   * @param spaceId spaceId
   * @param pageId pageId
   */
  public static OverallPage createOverallPage(long transactionId, int spaceId, int pageId) {
    OverallPage overallPage = new OverallPage(new byte[ServerRuntime.config.pageSize]);
    overallPage.spaceId = spaceId;
    overallPage.pageId = pageId;
    IO.traceNewPage(overallPage);
    overallPage.pageType = OVERALL_PAGE;
    overallPage.setup();
    overallPage.writeFILHeader(transactionId);
    overallPage.writeTablespace(transactionId);
    overallPage.maxPageId.set(ServerRuntime.config.indexLeftmostLeafIndex);
    return overallPage;
  }

  private void parseTablespace() {
    if (maxPageId == null) maxPageId = new AtomicInteger();
    maxPageId.set(parseIntegerBig(32));
    //    maxInitedPage = parseIntegerBig(32 + 8);
    //    currentDataPage = parseIntegerBig(32 + 12);
    flags = parseIntegerBig(32 + 16);
    /* RESERVED FOR 52 bytes */
  }

  /**
   * write tablespace header on both disk buffer and WAL log buffer.
   *
   * @param transactionId transactionId requests the method
   */
  public void writeTablespace(long transactionId) {
    /* First 16 valid bytes */
    byte[] newValue = new byte[16];
    int value = maxPageId.get();
    newValue[0] = (byte) (value >> 24);
    newValue[1] = (byte) (value >> 16);
    newValue[2] = (byte) (value >> 8);
    newValue[3] = (byte) value;
    //    newValue[4] = (byte) (maxInitedPage >> 24);
    //    newValue[5] = (byte) (maxInitedPage >> 16);
    //    newValue[6] = (byte) (maxInitedPage >> 8);
    //    newValue[7] = (byte) maxInitedPage;
    //    newValue[8] = (byte) (currentDataPage >> 24);
    //    newValue[9] = (byte) (currentDataPage >> 16);
    //    newValue[10] = (byte) (currentDataPage >> 8);
    //    newValue[11] = (byte) currentDataPage;
    newValue[12] = (byte) (flags >> 24);
    newValue[13] = (byte) (flags >> 16);
    newValue[14] = (byte) (flags >> 8);
    newValue[15] = (byte) flags;
    IO.write(transactionId, this, 32, 16, newValue, false);
  }

  /**
   * Allocate one page if possible.
   *
   * @return allocated pageId.
   */
  public int allocatePage(long transactionId) throws Exception {
    //    System.out.println( "################################# allocate new page: try for " +
    // transactionId + " in space:" + spaceId);
    int allocatedPageId = maxPageId.incrementAndGet();
    //    System.out.println("################################# allocate new page: " +
    // allocatedPageId);
    writeTablespace(transactionId);
    return allocatedPageId;
  }

  public void setup() {
    parseTablespace();
  }
}
