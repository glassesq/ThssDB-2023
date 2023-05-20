package cn.edu.thssdb.storage.page;

import cn.edu.thssdb.communication.IO;
import cn.edu.thssdb.runtime.ServerRuntime;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.RecordLogical;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.schema.ValueWrapper;
import cn.edu.thssdb.utils.Pair;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

public class IndexPage extends Page {

    /**
     * recordEntry is only for storage purpose.
     * the actual values of primary/non-primary fields shall be accessed other way.
     */
    public static class RecordInPage {
        public int myOffset;

        /* public ArrayList<int> variableFieldLength; */
        public byte[] nullBitmap;
        public byte flags;
        // TODO: deleted flag
        public byte numberRecordOwnedInDirectory;
        public byte recordType;
        public int nextAbsoluteOffset;
        public RecordInPage nextRecordInPage = null;
        /**
         * this field is only for creation use.
         * You should never use this field to iterate user records.
         */
        public RecordInPage previousRecordInPage = null;

        /* ************** base point of the record ***************** */
        /* primary key */
        private byte[] primaryKeys;

        /**
         * this field stores parsed value of primaryKey, ready for comparison
         */
        public ValueWrapper[] primaryKeyValues;
        public long updateTransactionId = 0;
        public long rollPointer = 0;
        public int childPageId = 0;

        /* non-primary key values. */
        public byte[] nonPrimaryKeys;

        /**
         * this field stores parsed value of nonPrimaryKey, ready for comparison
         */
        public ValueWrapper[] nonPrimaryKeyValues;
        public static final byte SYSTEM_INFIMUM_RECORD = 0;
        public static final byte SYSTEM_SUPREME_RECORD = 1;
        public static final byte USER_DATA_RECORD = 2;
        public static final byte USER_POINTER_RECORD = 3;

        public RecordInPage() {
        }

        /**
         * copy constructor
         *
         * @param record record to copy
         */
        public RecordInPage(RecordInPage record) {
            this.myOffset = record.myOffset;
            this.nullBitmap = Arrays.copyOf(record.nullBitmap, record.nullBitmap.length);
            this.flags = record.flags;
            this.numberRecordOwnedInDirectory = record.numberRecordOwnedInDirectory;
            this.recordType = record.recordType;
            this.nextAbsoluteOffset = 0;
            this.primaryKeys = Arrays.copyOf(record.primaryKeys, record.primaryKeys.length);
            this.primaryKeyValues = new ValueWrapper[record.primaryKeyValues.length];
            for (int i = 0; i < primaryKeyValues.length; i++) {
                this.primaryKeyValues[i] = new ValueWrapper(record.primaryKeyValues[i]);
            }
            this.updateTransactionId = record.updateTransactionId;
            this.rollPointer = record.rollPointer;
            this.childPageId = record.childPageId;
            this.nonPrimaryKeys = Arrays.copyOf(record.nonPrimaryKeys, record.nonPrimaryKeys.length);
            this.nonPrimaryKeyValues = new ValueWrapper[record.nonPrimaryKeyValues.length];
            for (int i = 0; i < nonPrimaryKeyValues.length; i++) {
                this.nonPrimaryKeyValues[i] = new ValueWrapper(this.nonPrimaryKeyValues[i]);
            }
            this.previousRecordInPage = null;
            this.nextRecordInPage = null;
        }

        public static RecordInPage createRecordInPageEntry(byte recordType, int primaryKeyLength, int nonPrimaryKeyLength, int nullBitmapLength, int nextAbsoluteOffset) {
            RecordInPage entry = new RecordInPage();
            entry.recordType = recordType;
            switch (recordType) {
                case SYSTEM_INFIMUM_RECORD:
                    entry.primaryKeys = "in".getBytes(StandardCharsets.US_ASCII);
                    entry.nonPrimaryKeys = new byte[0];
                    entry.nullBitmap = new byte[0];
                    entry.nextAbsoluteOffset = 52 + 10;
                    entry.myOffset = 52 + 4;
                    break;
                case SYSTEM_SUPREME_RECORD:
                    entry.primaryKeys = "ax".getBytes(StandardCharsets.US_ASCII);
                    entry.nonPrimaryKeys = new byte[0];
                    entry.nullBitmap = new byte[0];
                    entry.nextAbsoluteOffset = 52 + 4;
                    entry.myOffset = 52 + 10;
                    break;
                case USER_DATA_RECORD:
                    entry.primaryKeys = new byte[primaryKeyLength];
                    entry.nonPrimaryKeys = new byte[nonPrimaryKeyLength];
                    entry.nullBitmap = new byte[nullBitmapLength];
                    entry.nextAbsoluteOffset = nextAbsoluteOffset;
                    break;
                case USER_POINTER_RECORD:
                    entry.primaryKeys = new byte[primaryKeyLength];
                    entry.nonPrimaryKeys = new byte[0];
                    entry.nullBitmap = new byte[0];
                    entry.nextAbsoluteOffset = nextAbsoluteOffset;
                    break;
            }
            return entry;
        }


        /**
         * parse **every** filed of record.
         * primary keys and non-primary keys are stored in bytes[].
         * parse until it meets a SYSTEM_SUPREME_LOG
         *
         * @param page                page
         * @param pos                 basic position
         * @param primaryKeyLength    primaryKeyLength
         * @param nonPrimaryKeyLength nonPrimaryKeyLength
         * @param nullBitmapLength    nullBitmapLength (in byte)
         */
        public void parseDeeplyInPage(Page page, int pos, int primaryKeyLength, int nonPrimaryKeyLength, int nullBitmapLength, Table.TableMetadata metadata) {
            this.myOffset = pos;
            /* variable length */
            this.flags = (byte) (page.bytes[pos - 4] & 0xF0);
            this.numberRecordOwnedInDirectory = (byte) (page.bytes[pos - 4] & 0x0F);
            this.recordType = page.bytes[pos - 3];
            this.nextAbsoluteOffset = page.parseShortBig(pos - 2);
            ArrayList<Integer> primaryOffsetList;
            /* base point */
            switch (this.recordType) {
                case (SYSTEM_SUPREME_RECORD):
                    this.nullBitmap = new byte[0];

                    this.primaryKeys = "ax".getBytes(StandardCharsets.US_ASCII);
                    this.primaryKeyValues = null;

                    this.nonPrimaryKeys = new byte[0];
                    this.nonPrimaryKeyValues = new ValueWrapper[0];
                    return;
                case (SYSTEM_INFIMUM_RECORD):
                    this.nullBitmap = new byte[0];

                    this.primaryKeys = "in".getBytes(StandardCharsets.US_ASCII);
                    this.primaryKeyValues = null;

                    this.nonPrimaryKeys = new byte[0];
                    this.nonPrimaryKeyValues = new ValueWrapper[0];
                    break;
                case (USER_POINTER_RECORD):
                    this.nullBitmap = new byte[0];

                    this.primaryKeys = new byte[primaryKeyLength];
                    System.arraycopy(page.bytes, pos, this.primaryKeys, 0, primaryKeyLength);
                    this.primaryKeyValues = new ValueWrapper[metadata.getPrimaryKeyNumber()];

                    this.nonPrimaryKeys = new byte[0];
                    this.nonPrimaryKeyValues = new ValueWrapper[0];

                    primaryOffsetList = metadata.getPrimaryOffsetInOrder();
                    for (int i = 0; i < metadata.columnDetails.size(); i++) {
                        Column column = metadata.columnDetails.get(i);
                        byte[] newValue = new byte[column.getLength()];
                        if (column.primary >= 0) {
                            System.arraycopy(primaryKeys, primaryOffsetList.get(column.primary), newValue, 0, column.getLength());
                            primaryKeyValues[column.primary] = (new ValueWrapper(newValue, column.type, column.getLength(), column.offPage));
                        }
                    }

                    this.childPageId = page.parseIntegerBig(pos + primaryKeyLength);
                    break;
                case (USER_DATA_RECORD):
                    this.nullBitmap = new byte[nullBitmapLength];
                    System.arraycopy(page.bytes, pos - 4 - nullBitmapLength, this.nullBitmap, 0, nullBitmapLength);

                    this.primaryKeys = new byte[primaryKeyLength];
                    System.arraycopy(page.bytes, pos, this.primaryKeys, 0, primaryKeyLength);
                    this.primaryKeyValues = new ValueWrapper[metadata.getPrimaryKeyNumber()];

                    this.updateTransactionId = page.parseLongBig(pos + primaryKeyLength);
                    this.rollPointer = page.parseSevenByteBig(pos + primaryKeyLength + 8);

                    this.nonPrimaryKeys = new byte[nonPrimaryKeyLength];
                    System.arraycopy(page.bytes, pos + primaryKeyLength + 8 + 7, this.nonPrimaryKeys, 0, nonPrimaryKeyLength);
                    this.nonPrimaryKeyValues = new ValueWrapper[metadata.getNonPrimaryKeyNumber()];

                    primaryOffsetList = metadata.getPrimaryOffsetInOrder();
                    int nonPrimaryOffset = 0;
                    int npIndex = 0;
                    for (int i = 0; i < metadata.columnDetails.size(); i++) {
                        Column column = metadata.columnDetails.get(i);
                        byte[] newValue = new byte[column.getLength()];
                        if (column.primary >= 0) {
                            System.arraycopy(primaryKeys, primaryOffsetList.get(column.primary), newValue, 0, column.getLength());
                            primaryKeyValues[column.primary] = (new ValueWrapper(newValue, column.type, column.getLength(), column.offPage));
                        } else {
                            System.arraycopy(nonPrimaryKeys, nonPrimaryOffset, newValue, 0, column.getLength());
                            nonPrimaryKeyValues[npIndex] = (new ValueWrapper(newValue, column.type, column.getLength(), column.offPage));
                            ++npIndex;
                            nonPrimaryOffset += column.getLength();
                        }
                    }
                    break;
            }
        }

        /**
         * write this record to both disk buffer and WAL log.
         *
         * @param transactionId transaction
         * @param page          page
         * @param pos           position of the record
         */
        public void write(long transactionId, Page page, int pos) {
            // TODO: variable length field
            int primaryKeyLength = primaryKeys.length;
            int nonPrimaryKeyLength = nonPrimaryKeys.length;
            int nullBitmapLength = nullBitmap.length;
            byte[] newValue = new byte[nullBitmapLength + 4 + primaryKeyLength + 13 + nonPrimaryKeyLength + 4]; /* maximum */
            System.arraycopy(nullBitmap, 0, newValue, 0, nullBitmapLength);
            newValue[nullBitmapLength] = (byte) ((flags << 4) | numberRecordOwnedInDirectory);
            newValue[nullBitmapLength + 1] = recordType;
            newValue[nullBitmapLength + 2] = (byte) (nextAbsoluteOffset >> 8);
            newValue[nullBitmapLength + 3] = (byte) nextAbsoluteOffset;
            System.arraycopy(primaryKeys, 0, newValue, nullBitmapLength + 4, primaryKeyLength);

            switch (recordType) {
                case USER_DATA_RECORD:
                    newValue[nullBitmapLength + 4 + primaryKeyLength] = (byte) (this.updateTransactionId >> 56);
                    newValue[nullBitmapLength + 4 + primaryKeyLength + 1] = (byte) (this.updateTransactionId >> 48);
                    newValue[nullBitmapLength + 4 + primaryKeyLength + 2] = (byte) (this.updateTransactionId >> 40);
                    newValue[nullBitmapLength + 4 + primaryKeyLength + 3] = (byte) (this.updateTransactionId >> 32);
                    newValue[nullBitmapLength + 4 + primaryKeyLength + 4] = (byte) (this.updateTransactionId >> 24);
                    newValue[nullBitmapLength + 4 + primaryKeyLength + 5] = (byte) (this.updateTransactionId >> 16);
                    newValue[nullBitmapLength + 4 + primaryKeyLength + 6] = (byte) (this.updateTransactionId >> 8);
                    newValue[nullBitmapLength + 4 + primaryKeyLength + 7] = (byte) this.updateTransactionId;
                    newValue[nullBitmapLength + 4 + primaryKeyLength + 8] = (byte) (rollPointer >> 48);
                    newValue[nullBitmapLength + 4 + primaryKeyLength + 9] = (byte) (rollPointer >> 40);
                    newValue[nullBitmapLength + 4 + primaryKeyLength + 10] = (byte) (rollPointer >> 32);
                    newValue[nullBitmapLength + 4 + primaryKeyLength + 11] = (byte) (rollPointer >> 24);
                    newValue[nullBitmapLength + 4 + primaryKeyLength + 12] = (byte) (rollPointer >> 16);
                    newValue[nullBitmapLength + 4 + primaryKeyLength + 13] = (byte) (rollPointer >> 8);
                    newValue[nullBitmapLength + 4 + primaryKeyLength + 14] = (byte) rollPointer;
                    System.arraycopy(nonPrimaryKeys, 0, newValue, nullBitmapLength + 4 + primaryKeyLength + 15, nonPrimaryKeyLength);
                    IO.write(transactionId, page, pos - nullBitmapLength - 4, nullBitmapLength + 4 + primaryKeyLength + 15 + nonPrimaryKeyLength, newValue, false);
                    break;
                case USER_POINTER_RECORD:
                    newValue[nullBitmapLength + 4 + primaryKeyLength] = (byte) (this.childPageId >> 24);
                    newValue[nullBitmapLength + 4 + primaryKeyLength + 1] = (byte) (this.childPageId >> 16);
                    newValue[nullBitmapLength + 4 + primaryKeyLength + 2] = (byte) (this.childPageId >> 8);
                    newValue[nullBitmapLength + 4 + primaryKeyLength + 3] = (byte) this.childPageId;
                    IO.write(transactionId, page, pos - nullBitmapLength - 4, nullBitmapLength + 4 + primaryKeyLength + 4, newValue, false);
                    break;
                case SYSTEM_SUPREME_RECORD:
                case SYSTEM_INFIMUM_RECORD:
                    IO.write(transactionId, page, pos - nullBitmapLength - 4, nullBitmapLength + 4 + 2, newValue, false);
                    break;
                default:
                    break;
            }
        }

        @Override
        public String toString() {
            StringBuilder result = new StringBuilder("RecordInPage " + "flags=" + flags + ", numberRecordOwnedInDirectory=" + numberRecordOwnedInDirectory + ", recordType=" + recordType + ", nextAbsoluteOffset=" + nextAbsoluteOffset + ", updateTransactionId=" + updateTransactionId + ", rollPointer=" + rollPointer + ", childPageId=" + childPageId);
            result.append("\nprimaryKeys=\n");
            for (byte b : this.primaryKeys) {
                result.append(String.format("%02x ", b));
            }
            result.append("\nnonPrimaryKeys=\n");
            for (byte b : this.nonPrimaryKeys) {
                result.append(String.format("%02x ", b));
            }
            result.append("\nnullBitmap=\n");
            for (byte b : this.nullBitmap) {
                result.append(String.format("%02x ", b));
            }
            return result.toString();
        }
    }

    public int pageLevel = 0;
    public int numberDirectory = 0;
    public int freespaceStart = 64;
    /**
     * number of user records placed in this page.
     */
    public int numberPlacedRecords = 0;
    /**
     * number of valid user records in this page. Not including those marked as deleted.
     */
    public int numberValidRecords = 0;
    public long maxTransactionId = 0;

    ReentrantLock bLinkTreeLatch = new ReentrantLock();

    RecordInPage infimumRecord;
    RecordInPage supremeRecord;

    public IndexPage(byte[] bytes) {
        super(bytes);
        if (pageType == INDEX_PAGE) {
            /* if the page is already set up */
            parseIndexHeader();
            parseAllRecords();
        }
    }

    /**
     * create an initialized index page.
     *
     * @param transactionId transactionId who creates this page
     * @param spaceId       spaceId
     * @param pageId        pageId
     * @return index page
     */
    public static IndexPage createIndexPage(long transactionId, int spaceId, int pageId) {
        IndexPage indexPage = new IndexPage(new byte[ServerRuntime.config.pageSize]);
        indexPage.spaceId = spaceId;
        indexPage.pageId = pageId;
        IO.traceNewPage(indexPage);
        indexPage.pageType = INDEX_PAGE;
        indexPage.setup();
        indexPage.writeFILHeader(transactionId);
        indexPage.writeIndexHeader(transactionId);
        indexPage.infimumRecord.write(transactionId, indexPage, 52 + 4);
        indexPage.supremeRecord.write(transactionId, indexPage, 52 + 10);
        indexPage.infimumRecord.nextRecordInPage = indexPage.supremeRecord;
        indexPage.supremeRecord.previousRecordInPage = indexPage.infimumRecord;
        return indexPage;
    }

    /**
     * if the index node is the root node.
     *
     * @return true if it is the root.
     */
    public boolean isRoot() {
        return pageLevel == 0;
    }

    public void parseIndexHeader() {
        pageLevel = parseShortBig(32);
        numberDirectory = parseShortBig(32 + 2);
        freespaceStart = parseShortBig(32 + 4);
        numberPlacedRecords = parseShortBig(32 + 6);
        numberValidRecords = parseShortBig(32 + 8);
        maxTransactionId = parseLongBig(32 + 10);
        /* RESERVED for 20 byte */
    }

    public void writeAll(long transactionId) {
        writeFILHeader(transactionId);
        writeIndexHeader(transactionId);
        RecordInPage record = infimumRecord;
        while (true) {
            record.write(transactionId, this, record.myOffset);
            if (record == this.supremeRecord) break;
            record = record.nextRecordInPage;
        }
    }

    /**
     * write index header to both disk buffer and WAL log buffer.
     *
     * @param transactionId transactionId
     */
    public void writeIndexHeader(long transactionId) {
        byte[] newValue = new byte[18]; /* ignored the 2 reserved byte */
        newValue[0] = (byte) (pageLevel >> 8);
        newValue[1] = (byte) pageLevel;
        newValue[2] = (byte) (numberDirectory >> 8);
        newValue[3] = (byte) numberDirectory;
        newValue[4] = (byte) (freespaceStart >> 8);
        newValue[5] = (byte) freespaceStart;
        newValue[6] = (byte) (numberPlacedRecords >> 8);
        newValue[7] = (byte) numberPlacedRecords;
        newValue[8] = (byte) (numberValidRecords >> 8);
        newValue[9] = (byte) numberValidRecords;
        newValue[10] = (byte) (maxTransactionId >> 56);
        newValue[11] = (byte) (maxTransactionId >> 48);
        newValue[12] = (byte) (maxTransactionId >> 40);
        newValue[13] = (byte) (maxTransactionId >> 32);
        newValue[14] = (byte) (maxTransactionId >> 24);
        newValue[15] = (byte) (maxTransactionId >> 16);
        newValue[16] = (byte) (maxTransactionId >> 8);
        newValue[17] = (byte) maxTransactionId;
        IO.write(transactionId, this, 32, 18, newValue, false);
    }

    /**
     * Parse the system + user record part of this page using {@code page.bytes}.
     * {@code this.spaceId} shall be parsed first.
     * <b> {@code this.spaceId} and its tableMetadata must be traceable in ServerRuntime. </b>
     *
     * @param transactionId transaction that request this method
     * @return records excluding infimumRecord and supremeRecord.
     */
    public ArrayList<RecordLogical> getAllRecordLogical(long transactionId) {
        Table.TableMetadata metadata = ServerRuntime.tableMetadata.get(this.spaceId);
        ArrayList<RecordLogical> recordList = new ArrayList<>();
        RecordInPage record = infimumRecord;
        while (record != supremeRecord) {
            if (record != infimumRecord) {
                recordList.add(new RecordLogical(record, metadata));
            }
            record = record.nextRecordInPage;
        }
        return recordList;
    }

    /**
     * Parse the system + user record part of this page using {@code page.bytes}.
     * Save them into this.records;
     * <b> {@code this.spaceId} and its tableMetadata must be traceable in ServerRuntime.</b>
     * The method shall be only called once when inputting this page from disk.
     */
    private void parseAllRecords() {
        Table.TableMetadata metadata = ServerRuntime.tableMetadata.get(this.spaceId);
        System.out.println(this.spaceId);
        int primaryKeyLength = metadata.getPrimaryKeyLength();
        int nonPrimaryKeyLength = metadata.getNonPrimaryKeyLength();
        int nullBitmapLength = metadata.getNullBitmapLengthInByte();

        int currentPos = 52 + 4;
        infimumRecord = new RecordInPage();

        RecordInPage record = infimumRecord;
        while (true) {
            record.parseDeeplyInPage(this, currentPos, primaryKeyLength, nonPrimaryKeyLength, nullBitmapLength, metadata);
            System.out.println(record);
            if (currentPos == 52 + 10) {
                supremeRecord = record;
                break;
            }
            currentPos = record.nextAbsoluteOffset;
            record.nextRecordInPage = new RecordInPage();
            record.nextRecordInPage.previousRecordInPage = record;
            record = record.nextRecordInPage;

        }
    }

    /**
     * insert {@code recordToBeInserted} into page just before the {@code recordToBeInserted} assuming there is enough space for it.
     * This method implements {@code A <- node.insert(A, w, v)} in the paper, where A is {@code this} and (w,v) is {@code recordLogical}.
     * <br/> <br/>
     * This method writes <b> ATOMICALLY </b>.
     * Read operations can be performed without locks. This is because operations on iterative structures ({@code nextRecordInPage})are atomic.
     *
     * @param transactionId      transaction
     * @param recordToBeInserted record to be inserted
     * @param nextRecord         record that is just after the record to be inserted
     * @return true if success, false if there is not enough space for the record to be inserted.
     */
    public boolean insertDataRecordInternal(long transactionId, RecordLogical recordToBeInserted, RecordInPage nextRecord) {
        // avoid writing simultaneously
        bLinkTreeLatch.lock();

        Table.TableMetadata metadata = ServerRuntime.tableMetadata.get(this.spaceId);
        if (freespaceStart + metadata.getMaxRecordLength(DATA_PAGE) >= ServerRuntime.config.pageSize) {
            bLinkTreeLatch.unlock();
            return false;
        }

        int primaryKeyLength = metadata.getPrimaryKeyLength();
        int nonPrimaryKeyLength = metadata.getNonPrimaryKeyLength();
        int nullBitmapLength = metadata.getNullBitmapLengthInByte();
        RecordInPage record = RecordInPage.createRecordInPageEntry(RecordInPage.USER_DATA_RECORD, primaryKeyLength, nonPrimaryKeyLength, nullBitmapLength, nextRecord.myOffset);
        System.out.println(nextRecord.myOffset);
        record.nextRecordInPage = nextRecord;

        record.previousRecordInPage = nextRecord.previousRecordInPage;

        record.primaryKeyValues = new ValueWrapper[metadata.getPrimaryKeyNumber()];
        record.nonPrimaryKeyValues = new ValueWrapper[metadata.getNonPrimaryKeyNumber()];

        int nonPrimaryOffset = 0;
        ArrayList<Integer> primaryOffsetList = metadata.getPrimaryOffsetInOrder();

        int npIndex = 0;
        for (int i = 0; i < metadata.columnDetails.size(); i++) {
            Column column = metadata.columnDetails.get(i);
            if (column.primary >= 0) {
                record.primaryKeyValues[column.primary] = new ValueWrapper(recordToBeInserted.primaryKeyValues[column.primary]);
                System.arraycopy(recordToBeInserted.primaryKeyValues[column.primary].bytes, 0, record.primaryKeys, primaryOffsetList.get(column.primary), column.getLength());
            } else {
                record.nonPrimaryKeyValues[npIndex] = new ValueWrapper(recordToBeInserted.nonPrimaryKeyValues[npIndex]);
                System.arraycopy(recordToBeInserted.nonPrimaryKeyValues[npIndex].bytes, 0, record.nonPrimaryKeys, nonPrimaryOffset, column.getLength());
                nonPrimaryOffset += column.getLength();
                ++npIndex;
            }
        }

        record.updateTransactionId = transactionId;

        // previousRecordInPage is not used in reading.
        nextRecord.previousRecordInPage = record;

        record.myOffset = this.freespaceStart + 4 + nullBitmapLength;
        record.previousRecordInPage.nextAbsoluteOffset = record.myOffset;

        /* BEGIN ATOMIC */
        /* reference assignment is atomic.*/
        record.previousRecordInPage.nextRecordInPage = record;
        /* END ATOMIC */

        this.freespaceStart = record.myOffset + primaryKeyLength + nonPrimaryKeyLength + 15;
        writeIndexHeader(transactionId);
        record.write(transactionId, this, record.myOffset);
        record.nextRecordInPage.write(transactionId, this, record.nextRecordInPage.myOffset);
        record.previousRecordInPage.write(transactionId, this, record.previousRecordInPage.myOffset);

        System.out.println("############################## The record below is inserted:");
        System.out.println(record);
        System.out.println("##############################");

        // avoid writing simultaneously
        bLinkTreeLatch.unlock();

        return true;
    }

    /**
     * split current node which is root
     *
     * @param transactionId transactionId
     * @throws Exception IO error
     */
    public void splitRoot(long transactionId) throws Exception {
        bLinkTreeLatch.lock();

        OverallPage overallPage = (OverallPage) IO.read(this.spaceId, ServerRuntime.config.overallPageIndex);
        int leftPageId = overallPage.allocatePage(transactionId);
        int rightPageId = overallPage.allocatePage(transactionId);
        IndexPage leftPage = createIndexPage(transactionId, this.spaceId, leftPageId);
        IndexPage rightPage = createIndexPage(transactionId, this.spaceId, rightPageId);

        Table.TableMetadata metadata = ServerRuntime.tableMetadata.get(this.spaceId);
        ArrayList<RecordInPage> recordInPage = new ArrayList<>();
        RecordInPage record = this.infimumRecord;
        while (record != supremeRecord) {
            if (record != infimumRecord) recordInPage.add(record);
            record = record.nextRecordInPage;
        }

        int size = recordInPage.size();

        RecordInPage prevRecord = leftPage.infimumRecord;
        int currentPos = 64 + metadata.getNullBitmapLengthInByte() + 4;
        for (int i = 0; i < size / 2; i++) {
            RecordInPage shadowRecord = new RecordInPage(recordInPage.get(i));
            shadowRecord.myOffset = currentPos;
            shadowRecord.previousRecordInPage = prevRecord;

            prevRecord.nextAbsoluteOffset = currentPos;
            prevRecord.nextRecordInPage = shadowRecord;

            currentPos = currentPos + metadata.getMaxRecordLength(shadowRecord.recordType);

            prevRecord = shadowRecord;
            if (i + 1 >= size / 2) {
                shadowRecord.nextAbsoluteOffset = 52 + 10;
                shadowRecord.nextRecordInPage = leftPage.supremeRecord;
                leftPage.supremeRecord.previousRecordInPage = shadowRecord;
            }
        }

        prevRecord = rightPage.infimumRecord;
        currentPos = 64 + metadata.getNullBitmapLengthInByte() + 4;
        for (int i = size / 2; i < size; i++) {
            RecordInPage shadowRecord = new RecordInPage(recordInPage.get(i));
            shadowRecord.myOffset = currentPos;
            shadowRecord.previousRecordInPage = prevRecord;

            prevRecord.nextAbsoluteOffset = currentPos;
            prevRecord.nextRecordInPage = shadowRecord;

            currentPos = currentPos + metadata.getMaxRecordLength(shadowRecord.recordType);

            prevRecord = shadowRecord;
            if (i + 1 >= size) {
                shadowRecord.nextAbsoluteOffset = 52 + 10;
                shadowRecord.nextRecordInPage = rightPage.supremeRecord;
                rightPage.supremeRecord.previousRecordInPage = shadowRecord;
            }

        }

        RecordInPage leftPointerRecord = RecordInPage.createRecordInPageEntry(RecordInPage.USER_POINTER_RECORD, metadata.getPrimaryKeyLength(), metadata.getNonPrimaryKeyLength(), metadata.getNullBitmapLengthInByte(), 0);
        record = leftPage.supremeRecord.previousRecordInPage;
        System.arraycopy(record.primaryKeys, 0, leftPointerRecord.primaryKeys, 0, metadata.getPrimaryKeyLength());
        leftPointerRecord.primaryKeyValues = new ValueWrapper[record.primaryKeyValues.length];
        for (int i = 0; i < record.primaryKeyValues.length; i++) {
            leftPointerRecord.primaryKeyValues[i] = new ValueWrapper(record.primaryKeyValues[i]);
        }

        RecordInPage rightPointerRecord = RecordInPage.createRecordInPageEntry(RecordInPage.USER_POINTER_RECORD, metadata.getPrimaryKeyLength(), metadata.getNonPrimaryKeyLength(), metadata.getNullBitmapLengthInByte(), 0);
        record = rightPage.supremeRecord.previousRecordInPage;
        System.arraycopy(rightPage.supremeRecord.previousRecordInPage.primaryKeys, 0, rightPointerRecord.primaryKeys, 0, metadata.getPrimaryKeyLength());
        rightPointerRecord.primaryKeyValues = new ValueWrapper[record.primaryKeyValues.length];
        for (int i = 0; i < record.primaryKeyValues.length; i++) {
            rightPointerRecord.primaryKeyValues[i] = new ValueWrapper(record.primaryKeyValues[i]);
        }

        leftPointerRecord.myOffset = leftPage.freespaceStart + 4 + metadata.getNullBitmapLengthInByte();
        rightPointerRecord.myOffset = leftPointerRecord.myOffset + 4 + metadata.getNullBitmapLengthInByte();

        leftPointerRecord.nextRecordInPage = rightPointerRecord;
        leftPointerRecord.previousRecordInPage = infimumRecord;
        leftPointerRecord.nextAbsoluteOffset = rightPointerRecord.myOffset;
        leftPointerRecord.childPageId = leftPageId;

        rightPointerRecord.nextRecordInPage = supremeRecord;
        rightPointerRecord.previousRecordInPage = leftPointerRecord;
        rightPointerRecord.nextAbsoluteOffset = supremeRecord.myOffset;
        rightPointerRecord.childPageId = rightPageId;

        supremeRecord.previousRecordInPage = rightPointerRecord;

        infimumRecord.nextAbsoluteOffset = leftPointerRecord.myOffset;

        /* BEGIN ATOMIC */
        infimumRecord.nextRecordInPage = leftPointerRecord;
        /* END ATOMIC */

        /* update info */
        leftPage.pageLevel = 1;
        leftPage.nextPageId = rightPageId;

        rightPage.pageLevel = 1;
        rightPage.previousPageId = leftPageId;

        leftPage.writeAll(transactionId);
        rightPage.writeAll(transactionId);
        this.writeAll(transactionId);

        bLinkTreeLatch.unlock();
    }

    /**
     * scan for primaryKey == searchKey internal the page.
     *
     * @param transactionId transactionKey
     * @param searchKey     value of primary key
     * @return a pair of boolean and recordInPage.
     * The boolean indicates whether the searchKey is found. If it is found, the recordInPage is the record that contains the searchKey.
     * Otherwise, it stores the first record that is larger than the searchKey.
     */
    public Pair<Boolean, RecordInPage> scanInternal(long transactionId, ValueWrapper[] searchKey) {
        RecordInPage record = infimumRecord.nextRecordInPage;
        while (record != supremeRecord) {
            System.out.println(Arrays.toString(searchKey[0].bytes));
            System.out.println(Arrays.toString(record.primaryKeyValues[0].bytes));
            int compareResult = ValueWrapper.compareArray(searchKey, record.primaryKeyValues);
            if (compareResult == 0) {
                System.out.println(Arrays.toString(record.primaryKeyValues));
                return new Pair<>(true, record);
            } else if (compareResult < 0) {
                return new Pair<>(false, record);
            }
            record = record.nextRecordInPage;
        }
        return new Pair<>(false, record);
    }

    /**
     * set up an empty index Page.
     */
    public void setup() {
        infimumRecord = RecordInPage.createRecordInPageEntry(RecordInPage.SYSTEM_INFIMUM_RECORD, 0, 0, 0, 52 + 10);
        supremeRecord = RecordInPage.createRecordInPageEntry(RecordInPage.SYSTEM_SUPREME_RECORD, 0, 0, 0, 0);
    }

}
