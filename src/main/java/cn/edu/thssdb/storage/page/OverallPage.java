package cn.edu.thssdb.storage.page;

import cn.edu.thssdb.communication.IO;
import cn.edu.thssdb.runtime.ServerRuntime;

import java.util.concurrent.atomic.AtomicInteger;

public class OverallPage extends Page {

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
    overallPage.maxPageId = new AtomicInteger();
    IO.traceNewPage(overallPage);
    overallPage.pageType = OVERALL_PAGE;
    overallPage.maxPageId.set(ServerRuntime.config.indexLeftmostLeafIndex);
    overallPage.writeFILHeader(transactionId);
    overallPage.writeTablespace(transactionId);
    return overallPage;
  }

  private void parseTablespace() {
    if (maxPageId == null) {
      maxPageId = new AtomicInteger();
    }
    maxPageId.set(parseIntegerBig(32));
  }

  /**
   * write tablespace header on both disk buffer and WAL log buffer.
   *
   * @param transactionId transactionId requests the method
   */
  public void writeTablespace(long transactionId) {
    byte[] newValue = new byte[4];
    int value = maxPageId.get();
    newValue[0] = (byte) (value >> 24);
    newValue[1] = (byte) (value >> 16);
    newValue[2] = (byte) (value >> 8);
    newValue[3] = (byte) value;
    IO.write(transactionId, this, 32, 4, newValue, false);
  }

  /**
   * Allocate one page if possible.
   *
   * @return allocated pageId.
   */
  public int allocatePage(long transactionId) throws Exception {
    int allocatedPageId = maxPageId.incrementAndGet();
    writeTablespace(transactionId);
    return allocatedPageId;
  }
}
