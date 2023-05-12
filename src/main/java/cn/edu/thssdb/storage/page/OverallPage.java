package cn.edu.thssdb.storage.page;

import cn.edu.thssdb.communication.IO;

public class OverallPage extends ExtentManagePage {
    /* Tablespace Header */

    /**
     * links all available extents as a list.
     * Note: this is a one-way linked list.
     */
    public ListBaseNode availableExtents;

    /**
     * links all full extents as a list.
     * Note: this is a one-way linked list.
     */
    public ListBaseNode fullExtents;

    /**
     * on which the first element of availableExtents.
     */
    public ExtentManagePage currentAvailableExtentManager = this;

    public int maxPageId;
    public int maxInitedPage;
    public int currentDataPage;

    public int flags;

    @Override
    public void parse() {
        parseFILHeader();
        parseExtentEntry();
        parseTablespace();
    }

    public void parseTablespace() {
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
     */
    public void writeTablespace() {
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
        IO.write(this, 32, 16, newValue, false);
        /* RESERVED 20 bytes */
        /* Two ListBaseNode */
        availableExtents.write(this, 32 + 36);
        fullExtents.write(this, 32 + 52);
    }

    /**
     * Allocate one page if possible. And then set the corresponding bit to true.
     * Update information in relevant ExtentManage Pages.
     *
     * @return allocated pageId.
     */
    public int allocatePage() throws Exception {
        if (availableExtents.length == 0) {
            throw new Exception("No extent can be allocated now. Further Implementation Needed.");
        }
        ExtentEntry entry;
        int allocatedPageId;
        int extentIdWithinManager = (availableExtents.nextOffset - 100) / 20;

        entry = currentAvailableExtentManager.extentEntries[extentIdWithinManager];
        allocatedPageId = entry.allocatePage() + extentIdWithinManager * 64 + availableExtents.nextPageId;


        if (entry.minAvailablePageId == 64) {
            /* this entry's pages are exhausted. Therefore, we may choose the second node in availableExtents List. */
            int nextEntryPageId = entry.listNode.nextPageId;
            int nextEntryOffset = entry.listNode.nextOffset;

            /* update fullExtents List */
            entry.listNode.nextPageId = fullExtents.nextPageId;
            entry.listNode.nextOffset = fullExtents.nextOffset;
            entry.listNode.write(currentAvailableExtentManager, 100 + extentIdWithinManager * 20);

            fullExtents.length += 1;
            fullExtents.nextPageId = availableExtents.nextPageId;
            fullExtents.nextOffset = availableExtents.nextOffset;
            fullExtents.write(this, 32 + 52);

            /* update availableExtents List */
            availableExtents.length -= 1;
            availableExtents.nextPageId = nextEntryPageId;
            availableExtents.nextOffset = nextEntryOffset;
            availableExtents.write(this, 32 + 36); /* availableExtentListNode starts from position 36 + 32 */

            /* update currentAvailableExtentManager, on which the first element of availableExtents. */
            if (availableExtents.length > 0) {
                currentAvailableExtentManager = (ExtentManagePage) IO.read(this.spaceId, nextPageId);
            } else {
                // TODO: more extents shall be added.
            }
        }
        return allocatedPageId;
    }

    @Override
    public void writeAll() {
        writeFILHeader();
        writeExtentManager();
        writeTablespace();
    }
}
