package cn.edu.thssdb.storage.page;

import cn.edu.thssdb.communication.IO;

public class ExtentManagePage extends Page {
    public class ExtentEntry {
        ListNode listNode;

        /**
         * Page Status in this extent. 1 for used and 0 for unused.
         */
        long pageStatus = 0;

        public void parse(int pos) {
            listNode = new ListNode();
            listNode.parse(pos);
            pageStatus = parseLongBig(pos + 12);
            for (minAvailablePageId = 0; minAvailablePageId < 64; minAvailablePageId++) {
                if ((pageStatus & ((long) 1 << minAvailablePageId)) != 0) break;
            }
        }

        int minAvailablePageId = 0;


        /**
         * Allocate one page if possible. And then set the corresponding bit to true.
         *
         * @return allocated page order within the extent. (0-255)
         */
        public int allocatePage() {
            int allocatedPage = minAvailablePageId;
            pageStatus = pageStatus | ((long) 1 << allocatedPage);
            minAvailablePageId++;
            for (; minAvailablePageId < 64; minAvailablePageId++) {
                if ((pageStatus & ((long) 1 << minAvailablePageId)) != 0) break;
            }
            return allocatedPage;
        }

        /**
         * write the page status bitmap **only** on both disk buffer and WAL log buffer.
         *
         * @param page page to write
         * @param pos  offset
         */
        public void writeOnlyBitmap(long transactionId, Page page, int pos) {
            // TODO: merge for performance consideration
            byte[] newValue = new byte[8];
            newValue[0] = (byte) (pageStatus >> 56);
            newValue[1] = (byte) (pageStatus >> 48);
            newValue[2] = (byte) (pageStatus >> 40);
            newValue[3] = (byte) (pageStatus >> 32);
            newValue[4] = (byte) (pageStatus >> 24);
            newValue[5] = (byte) (pageStatus >> 16);
            newValue[6] = (byte) (pageStatus >> 8);
            newValue[7] = (byte) (pageStatus);
            IO.write(transactionId, page, pos + 12, 8, newValue, false);
        }
    }

    public ExtentEntry[] extentEntries = new ExtentEntry[256];


    public void parseExtentEntry() {
        for (int i = 0; i <= 255; i++) {
            extentEntries[i] = new ExtentEntry();
            extentEntries[i].parse(100 + i * 20);
        }
    }

    @Override
    public void parse() {
        parseFILHeader();
        parseExtentEntry();
    }

    public void writeExtentManager(long transactionId) {
        for (int i = 0; i <= 255; i++) {
            extentEntries[i].listNode.write(transactionId, this, 100 + i * 20);
            extentEntries[i].writeOnlyBitmap(transactionId, this, 100 + i * 20);
        }
    }

    @Override
    public void writeAll(long transactionId) {
        writeFILHeader(transactionId);
        writeExtentManager(transactionId);
    }

}
