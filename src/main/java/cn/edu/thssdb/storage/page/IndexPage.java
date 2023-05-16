package cn.edu.thssdb.storage.page;

import cn.edu.thssdb.communication.IO;
import cn.edu.thssdb.runtime.ServerRuntime;
import cn.edu.thssdb.schema.RecordLogical;
import cn.edu.thssdb.schema.Table;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import static cn.edu.thssdb.storage.page.IndexPage.RecordInPage.USER_DATA_RECORD;

public class IndexPage extends Page {

    /**
     * recordEntry is only for storage purpose.
     * the actual values of primary/non-primary fields shall be accessed other way.
     */
    public static class RecordInPage {
        /* public ArrayList<int> variableFieldLength; */
        public byte[] nullBitmap;
        public byte flags;
        public byte numberRecordOwnedInDirectory;
        public byte recordType;
        public int nextAbsoluteOffset;

        /* ************** base point of the record ***************** */
        /* primary key */
        public byte[] primaryKeys;
        public long updateTransactionId = 0;
        public long rollPointer = 0;
        public int childPageId = 0;

        /* non-primary key values. */
        public byte[] nonPrimaryKeys;
        public static final byte SYSTEM_INFIMUM_RECORD = 0;
        public static final byte SYSTEM_SUPREME_RECORD = 1;
        public static final byte USER_DATA_RECORD = 2;
        public static final byte USER_POINTER_RECORD = 3;

        public static RecordInPage createRecordInPageEntry(byte recordType, int primaryKeyLength, int nonPrimaryKeyLength, int nullBitmapLength, int nextAbsoluteOffset) {
            RecordInPage entry = new RecordInPage();
            entry.recordType = recordType;
            switch (recordType) {
                case SYSTEM_INFIMUM_RECORD:
                    entry.primaryKeys = "in".getBytes(StandardCharsets.US_ASCII);
                    entry.nonPrimaryKeys = new byte[0];
                    entry.nullBitmap = new byte[0];
                    entry.nextAbsoluteOffset = 52 + 10;
                    break;
                case SYSTEM_SUPREME_RECORD:
                    entry.primaryKeys = "ax".getBytes(StandardCharsets.US_ASCII);
                    entry.nonPrimaryKeys = new byte[0];
                    entry.nullBitmap = new byte[0];
                    entry.nextAbsoluteOffset = 52 + 4;
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
        public void parseDeeplyInPage(Page page, int pos, int primaryKeyLength, int nonPrimaryKeyLength, int nullBitmapLength) {
            /* variable length */
            this.flags = (byte) (page.bytes[pos - 4] & 0xF0);
            this.numberRecordOwnedInDirectory = (byte) (page.bytes[pos - 4] & 0x0F);
            this.recordType = page.bytes[pos - 3];
            this.nextAbsoluteOffset = page.parseShortBig(pos - 2);
            /* base point */
            switch (this.recordType) {
                case (SYSTEM_SUPREME_RECORD):
                    this.primaryKeys = "ax".getBytes(StandardCharsets.US_ASCII);
                    this.nonPrimaryKeys = new byte[0];
                    this.nullBitmap = new byte[0];
                    /* end of the parsing */
                    return;
                case (SYSTEM_INFIMUM_RECORD):
                    this.primaryKeys = "in".getBytes(StandardCharsets.US_ASCII);
                    this.nonPrimaryKeys = new byte[0];
                    this.nullBitmap = new byte[0];
//                    parseRecursivelyDeeplyInPage(page, this.nextAbsoluteOffset, primaryKeyLength, nonPrimaryKeyLength, nullBitmapLength);
                    break;
                case (USER_POINTER_RECORD):
                    this.primaryKeys = new byte[primaryKeyLength];
                    System.arraycopy(page.bytes, pos, this.primaryKeys, 0, primaryKeyLength);
                    this.nonPrimaryKeys = new byte[0];
                    this.childPageId = page.parseIntegerBig(pos + primaryKeyLength);
                    this.nullBitmap = new byte[0];
//                    parseRecursivelyDeeplyInPage(page, this.nextAbsoluteOffset, primaryKeyLength, nonPrimaryKeyLength, nullBitmapLength);
                    break;
                case (USER_DATA_RECORD):
                    this.primaryKeys = new byte[primaryKeyLength];
                    System.arraycopy(page.bytes, pos, this.primaryKeys, 0, primaryKeyLength);
                    this.updateTransactionId = page.parseLongBig(pos + primaryKeyLength);
                    this.rollPointer = page.parseSevenByteBig(pos + primaryKeyLength + 8);
                    this.nonPrimaryKeys = new byte[nonPrimaryKeyLength];
                    System.arraycopy(page.bytes, pos + primaryKeyLength + 8 + 7, this.nonPrimaryKeys, 0, nonPrimaryKeyLength);
                    this.nullBitmap = new byte[nullBitmapLength];
                    System.arraycopy(page.bytes, pos - 4 - nullBitmapLength, this.nullBitmap, 0, nullBitmapLength);
//                    parseRecursivelyDeeplyInPage(page, this.nextAbsoluteOffset, primaryKeyLength, nonPrimaryKeyLength, nullBitmapLength);
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
            // TODO
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

    RecordInPage infimumRecord;
    RecordInPage supremeRecord;

    /**
     * create an initialized index page.
     *
     * @param transactionId transactionId who creates this page
     * @param spaceId       spaceId
     * @param pageId        pageId
     * @param temporary     if this paged is temporary
     * @return index page
     */
    public static IndexPage createIndexPage(long transactionId, int spaceId, int pageId, boolean temporary) {
        IndexPage indexPage = new IndexPage();
        indexPage.spaceId = spaceId;
        indexPage.pageId = pageId;
        if (!temporary) {
            IO.traceNewPage(indexPage);
            indexPage.setup();
            indexPage.writeFILHeader(transactionId);
            indexPage.writeIndexHeader(transactionId);
            indexPage.infimumRecord.write(transactionId, indexPage, 52 + 4);
            indexPage.supremeRecord.write(transactionId, indexPage, 52 + 10);
        } else {
            // TODO: temporary page.
        }
        return indexPage;
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
     * parse the page from {@code page.bytes}.
     * The {@code user record} part is not parsed.
     */
    @Override
    public void parse() {
        parseFILHeader();
        // TODO: repeatedly parsing FIL Header. (performance consideration).
        parseIndexHeader();
    }

    /**
     * Parse the system + user record part of this page using {@code page.bytes}.
     * <b> The {@code parse()} method shall be called in advance for this method to work.</b>
     *
     * @param transactionId transaction that request this method
     * @param metadata      tableMetadata
     * @return records excluding infimumRecord and supremeRecord.
     */
    public ArrayList<RecordLogical> getAllRecordLogical(long transactionId, Table.TableMetadata metadata) {
        int primaryKeyLength = metadata.getPrimaryKeyLength();
        int nonPrimaryKeyLength = metadata.getNonPrimaryKeyLength();
        int nullBitmapLength = metadata.getNullBitmapLengthInByte();

        int currentPos = 52 + 4;
        infimumRecord = new RecordInPage();

        RecordInPage record = infimumRecord;
        ArrayList<RecordLogical> recordList = new ArrayList<>();
        while (true) {
            record.parseDeeplyInPage(this, currentPos, primaryKeyLength, nonPrimaryKeyLength, nullBitmapLength);
            System.out.println(record);
            if (record.recordType == USER_DATA_RECORD) {
                recordList.add(new RecordLogical(record, metadata));
            }
            if (currentPos == 52 + 10) {
                supremeRecord = record;
                break;
            }
            currentPos = record.nextAbsoluteOffset;
            record = new RecordInPage();
        }
        return recordList;
    }

    /**
     * set up an empty index Page.
     */
    public void setup() {
        infimumRecord = RecordInPage.createRecordInPageEntry(RecordInPage.SYSTEM_INFIMUM_RECORD, 0, 0, 0, 52 + 10);
        supremeRecord = RecordInPage.createRecordInPageEntry(RecordInPage.SYSTEM_SUPREME_RECORD, 0, 0, 0, 0);
    }

}
