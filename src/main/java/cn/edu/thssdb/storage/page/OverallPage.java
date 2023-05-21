package cn.edu.thssdb.storage.page;

import cn.edu.thssdb.communication.IO;
import cn.edu.thssdb.runtime.ServerRuntime;

public class OverallPage extends ExtentManagePage {
  /* Tablespace Header */

  /** links all available extents as a list. Note: this is a one-way linked list. */
  public ListBaseNode availableExtents = new ListBaseNode();

  /** links all full extents as a list. Note: this is a one-way linked list. */
  public ListBaseNode fullExtents = new ListBaseNode();

  /** on which the first element of availableExtents. */
  public ExtentManagePage currentAvailableExtentManager = this;

  public int maxPageId;
  public int maxInitedPage;
  public int currentDataPage;

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
    overallPage.writeExtentManager(transactionId);
    overallPage.usePage(transactionId, 0);
    overallPage.usePage(transactionId, 1);
    overallPage.usePage(transactionId, 2);
    return overallPage;
  }

  private void parseTablespace() {
    maxPageId = parseIntegerBig(32 + 4);
    maxInitedPage = parseIntegerBig(32 + 8);
    currentDataPage = parseIntegerBig(32 + 12);
    flags = parseIntegerBig(32 + 16);
    /* RESERVED FOR 20 bytes */
    availableExtents.parse(32 + 36);
    fullExtents.parse(32 + 52);
  }

  /**
   * write tablespace header on both disk buffer and WAL log buffer.
   *
   * @param transactionId transactionId requests the method
   */
  public void writeTablespace(long transactionId) {
    /* First 16 valid bytes */
    byte[] newValue = new byte[16];
    newValue[0] = (byte) (maxPageId >> 24);
    newValue[1] = (byte) (maxPageId >> 16);
    newValue[2] = (byte) (maxPageId >> 8);
    newValue[3] = (byte) maxPageId;
    newValue[4] = (byte) (maxInitedPage >> 24);
    newValue[5] = (byte) (maxInitedPage >> 16);
    newValue[6] = (byte) (maxInitedPage >> 8);
    newValue[7] = (byte) maxInitedPage;
    newValue[8] = (byte) (currentDataPage >> 24);
    newValue[9] = (byte) (currentDataPage >> 16);
    newValue[10] = (byte) (currentDataPage >> 8);
    newValue[11] = (byte) currentDataPage;
    newValue[12] = (byte) (flags >> 24);
    newValue[13] = (byte) (flags >> 16);
    newValue[14] = (byte) (flags >> 8);
    newValue[15] = (byte) flags;
    IO.write(transactionId, this, 32, 16, newValue, false);
    /* RESERVED 20 bytes */
    /* Two ListBaseNode */
    availableExtents.write(transactionId, this, 32 + 36);
    fullExtents.write(transactionId, this, 32 + 52);
  }

  /**
   * Allocate one page if possible. And then set the corresponding bit to true. Update information
   * in relevant ExtentManage Pages.
   *
   * @return allocated pageId.
   */
  public int allocatePage(long transactionId) throws Exception {
    // TODO: latch
    if (availableExtents.length == 0) {
      throw new Exception("No extent can be allocated now. Further Implementation Needed.");
    }
    ExtentEntry entry;
    int allocatedPageId;
    int extentIdWithinManager = (availableExtents.nextOffset - 100) / 20;

    entry = currentAvailableExtentManager.extentEntries[extentIdWithinManager];
    currentAvailableExtentManager.usePage(
        transactionId, extentIdWithinManager * 64 + availableExtents.nextPageId);
    allocatedPageId =
        entry.allocatePage() + extentIdWithinManager * 64 + availableExtents.nextPageId;

    if (entry.minAvailablePageId == 64) {
      /* this entry's pages are exhausted. Therefore, we may choose the second node in availableExtents List. */
      int nextEntryPageId = entry.listNode.nextPageId;
      int nextEntryOffset = entry.listNode.nextOffset;

      /* update fullExtents List */
      entry.listNode.nextPageId = fullExtents.nextPageId;
      entry.listNode.nextOffset = fullExtents.nextOffset;
      entry.listNode.write(
          transactionId, currentAvailableExtentManager, 100 + extentIdWithinManager * 20);

      fullExtents.length += 1;
      fullExtents.nextPageId = availableExtents.nextPageId;
      fullExtents.nextOffset = availableExtents.nextOffset;
      fullExtents.write(transactionId, this, 32 + 52);

      /* update availableExtents List */
      availableExtents.length -= 1;
      availableExtents.nextPageId = nextEntryPageId;
      availableExtents.nextOffset = nextEntryOffset;
      availableExtents.write(
          transactionId, this, 32 + 36); /* availableExtentListNode starts from position 36 + 32 */

      /* update currentAvailableExtentManager, on which the first element of availableExtents. */
      if (availableExtents.length > 0) {
        currentAvailableExtentManager =
            (ExtentManagePage) IO.read(this.spaceId, availableExtents.nextPageId);
      } else {
        // TODO: more extents shall be added.
      }
    }
    return allocatedPageId;
  }

  public void setup() {
    // TODO: this is only for test. need to be revised.
    parseExtentEntry();
    parseTablespace();
  }
}
