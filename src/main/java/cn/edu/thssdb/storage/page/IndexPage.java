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
import java.util.Stack;

import static java.lang.System.exit;

public class IndexPage extends Page {

  /**
   * recordEntry is only for storage purpose. the actual values of primary/non-primary fields shall
   * be accessed other way.
   */
  public static class RecordInPage {
    public int myOffset;
    public byte[] nullBitmap;
    public byte flags;
    public static final byte DELETE_FLAG = 0b1;
    public static final byte RIGHTEST_FLAG = 0b10;

    public void setNull(int nonPrimaryKeyOrder, boolean isNull) {
      if (isNull) nullBitmap[nonPrimaryKeyOrder / 8] |= (1 << (nonPrimaryKeyOrder % 8));
      else nullBitmap[nonPrimaryKeyOrder / 8] &= ~(1 << (nonPrimaryKeyOrder % 8));
    }

    public boolean isNullBit(int nonPrimaryKeyOrder) {
      return (nullBitmap[nonPrimaryKeyOrder / 8] & (1 << (nonPrimaryKeyOrder % 8))) != 0;
    }

    public void setRightest() {
      flags |= RIGHTEST_FLAG;
    }

    public void unsetRightest() {
      flags &= ~RIGHTEST_FLAG;
    }

    public boolean isRightest() {
      return (flags & RIGHTEST_FLAG) != 0;
    }

    public void setDeleted() {
      flags |= DELETE_FLAG;
    }

    public void unsetDeleted() {
      flags &= ~DELETE_FLAG;
    }

    public boolean isNotDeleted() {
      return (flags & DELETE_FLAG) == 0;
    }

    public boolean isPointToNextLevel() {
      if (recordType == USER_POINTER_RECORD) {
        return true;
      }
      return recordType == SYSTEM_SUPREME_RECORD && isRightest();
    }

    public byte recordType;
    public int nextAbsoluteOffset;
    public RecordInPage nextRecordInPage = null;

    /* ************** base point of the record ***************** */
    /* primary key */
    private byte[] primaryKeys;

    /** this field stores parsed value of primaryKey, ready for comparison */
    public ValueWrapper[] primaryKeyValues;

    public int childPageId = 0;
    /* non-primary key values. */
    public byte[] nonPrimaryKeys;
    /** this field stores parsed value of nonPrimaryKey, ready for comparison */
    public ValueWrapper[] nonPrimaryKeyValues;

    public static final byte SYSTEM_INFIMUM_RECORD = 0;
    public static final byte SYSTEM_SUPREME_RECORD = 1;
    public static final byte USER_DATA_RECORD = 2;
    public static final byte USER_POINTER_RECORD = 3;

    public RecordInPage() {}
    /**
     * copy constructor
     *
     * @param record record to copy
     */
    public RecordInPage(RecordInPage record) {
      this.myOffset = record.myOffset;
      this.nullBitmap = Arrays.copyOf(record.nullBitmap, record.nullBitmap.length);
      this.flags = record.flags;
      this.recordType = record.recordType;
      this.nextAbsoluteOffset = 0;
      this.primaryKeys = Arrays.copyOf(record.primaryKeys, record.primaryKeys.length);
      this.primaryKeyValues = new ValueWrapper[record.primaryKeyValues.length];
      for (int i = 0; i < primaryKeyValues.length; i++) {
        this.primaryKeyValues[i] = new ValueWrapper(record.primaryKeyValues[i]);
      }
      this.childPageId = record.childPageId;
      this.nonPrimaryKeys = Arrays.copyOf(record.nonPrimaryKeys, record.nonPrimaryKeys.length);
      this.nonPrimaryKeyValues = new ValueWrapper[record.nonPrimaryKeyValues.length];
      for (int i = 0; i < nonPrimaryKeyValues.length; i++) {
        this.nonPrimaryKeyValues[i] = new ValueWrapper(record.nonPrimaryKeyValues[i]);
      }
      this.nextRecordInPage = null;
    }

    public static RecordInPage createRecordInPageEntry(
        byte recordType,
        int primaryKeyLength,
        int nonPrimaryKeyLength,
        int nullBitmapLength,
        int nextAbsoluteOffset,
        int childPageId) {
      RecordInPage entry = new RecordInPage();
      entry.recordType = recordType;
      switch (recordType) {
        case SYSTEM_INFIMUM_RECORD:
          entry.primaryKeys = "in".getBytes(StandardCharsets.US_ASCII);
          entry.nonPrimaryKeys = new byte[0];
          entry.primaryKeyValues = new ValueWrapper[0];
          entry.nonPrimaryKeyValues = new ValueWrapper[0];
          entry.nullBitmap = new byte[0];
          entry.nextAbsoluteOffset = 52 + 10;
          entry.myOffset = 52 + 4;
          break;
        case SYSTEM_SUPREME_RECORD:
          entry.primaryKeys = "ax".getBytes(StandardCharsets.US_ASCII);
          entry.nonPrimaryKeys = new byte[0];
          entry.primaryKeyValues = new ValueWrapper[0];
          entry.nonPrimaryKeyValues = new ValueWrapper[0];
          entry.nullBitmap = new byte[0];
          entry.nextAbsoluteOffset = nextAbsoluteOffset;
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
          entry.nonPrimaryKeyValues = new ValueWrapper[0];
          entry.nullBitmap = new byte[0];
          entry.nextAbsoluteOffset = nextAbsoluteOffset;
          entry.childPageId = childPageId;
          break;
      }
      return entry;
    }

    /**
     * parse **every** filed of record. primary keys and non-primary keys are stored in bytes[].
     * parse until it meets a SYSTEM_SUPREME_LOG
     *
     * @param page page
     * @param pos basic position
     * @param metadata table metadata
     */
    public void parseDeeplyInPage(Page page, int pos, Table.TableMetadata metadata) {

      int primaryKeyLength = metadata.getPrimaryKeyLength();
      int nonPrimaryKeyLength = metadata.getNonPrimaryKeyLength();
      int nullBitmapLength = metadata.getNullBitmapLengthInByte();
      this.myOffset = pos;
      /* variable length */
      this.flags = page.bytes[pos - 4];
      this.recordType = page.bytes[pos - 3];
      this.nextAbsoluteOffset = page.parseShortBig(pos - 2);
      int primaryKeyNumber = metadata.getPrimaryKeyNumber();
      int nonPrimaryKeyNumber = metadata.getNonPrimaryKeyNumber();
      ArrayList<Integer> primaryOffsetList;
      ArrayList<Integer> nonPrimaryOffsetList;
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
          primaryKeyNumber = metadata.getPrimaryKeyNumber();
          for (int i = 0; i < primaryKeyNumber; i++) {
            Column column = metadata.getColumnDetailByPrimaryField(i);
            int length = column.getLength();
            byte[] newValue = new byte[length];
            System.arraycopy(primaryKeys, primaryOffsetList.get(i), newValue, 0, length);
            primaryKeyValues[i] = new ValueWrapper(newValue, column.type, length, column.offPage);
          }

          this.childPageId = page.parseIntegerBig(pos + primaryKeyLength);
          break;
        case (USER_DATA_RECORD):
          this.nullBitmap = new byte[nullBitmapLength];
          System.arraycopy(
              page.bytes, pos - 4 - nullBitmapLength, this.nullBitmap, 0, nullBitmapLength);

          this.primaryKeys = new byte[primaryKeyLength];
          System.arraycopy(page.bytes, pos, this.primaryKeys, 0, primaryKeyLength);
          this.primaryKeyValues = new ValueWrapper[metadata.getPrimaryKeyNumber()];

          this.nonPrimaryKeys = new byte[nonPrimaryKeyLength];
          System.arraycopy(
              page.bytes,
              pos + primaryKeyLength + 8 + 7,
              this.nonPrimaryKeys,
              0,
              nonPrimaryKeyLength);
          this.nonPrimaryKeyValues = new ValueWrapper[metadata.getNonPrimaryKeyNumber()];

          primaryOffsetList = metadata.getPrimaryOffsetInOrder();
          for (int i = 0; i < primaryKeyNumber; i++) {
            Column column = metadata.getColumnDetailByOrderInType(i, true);
            int length = column.getLength();
            byte[] newValue = new byte[length];
            System.arraycopy(primaryKeys, primaryOffsetList.get(i), newValue, 0, length);
            primaryKeyValues[i] = new ValueWrapper(newValue, column.type, length, column.offPage);
          }

          nonPrimaryOffsetList = metadata.getNonPrimaryKeyOffsetInOrder();
          for (int i = 0; i < nonPrimaryKeyNumber; i++) {
            Column column = metadata.getColumnDetailByOrderInType(i, false);
            int length = column.getLength();
            byte[] newValue = new byte[length];
            System.arraycopy(nonPrimaryKeys, nonPrimaryOffsetList.get(i), newValue, 0, length);
            if (this.isNullBit(i)) nonPrimaryKeyValues[i] = new ValueWrapper(true, column.type);
            else
              nonPrimaryKeyValues[i] =
                  new ValueWrapper(newValue, column.type, length, column.offPage);
          }

          break;
      }
    }

    /**
     * write this record to both disk buffer and WAL log.
     *
     * @param transactionId transaction
     * @param page page
     * @param pos position of the record
     */
    public void write(long transactionId, Page page, int pos) {
      // TODO: variable length field
      int primaryKeyLength = primaryKeys.length;
      int nonPrimaryKeyLength = nonPrimaryKeys.length;
      int nullBitmapLength = nullBitmap.length;
      byte[] newValue =
          new byte
              [nullBitmapLength
                  + 4
                  + primaryKeyLength
                  + 13
                  + nonPrimaryKeyLength
                  + 4]; /* maximum */
      System.arraycopy(nullBitmap, 0, newValue, 0, nullBitmapLength);
      newValue[nullBitmapLength] = flags;
      newValue[nullBitmapLength + 1] = recordType;
      newValue[nullBitmapLength + 2] = (byte) (nextAbsoluteOffset >> 8);
      newValue[nullBitmapLength + 3] = (byte) nextAbsoluteOffset;
      System.arraycopy(primaryKeys, 0, newValue, nullBitmapLength + 4, primaryKeyLength);

      switch (recordType) {
        case USER_DATA_RECORD:
          System.arraycopy(
              nonPrimaryKeys,
              0,
              newValue,
              nullBitmapLength + 4 + primaryKeyLength + 15,
              nonPrimaryKeyLength);
          IO.write(
              transactionId,
              page,
              pos - nullBitmapLength - 4,
              nullBitmapLength + 4 + primaryKeyLength + 15 + nonPrimaryKeyLength,
              newValue,
              false);
          break;
        case USER_POINTER_RECORD:
          newValue[nullBitmapLength + 4 + primaryKeyLength] = (byte) (this.childPageId >> 24);
          newValue[nullBitmapLength + 4 + primaryKeyLength + 1] = (byte) (this.childPageId >> 16);
          newValue[nullBitmapLength + 4 + primaryKeyLength + 2] = (byte) (this.childPageId >> 8);
          newValue[nullBitmapLength + 4 + primaryKeyLength + 3] = (byte) this.childPageId;
          IO.write(
              transactionId,
              page,
              pos - nullBitmapLength - 4,
              nullBitmapLength + 4 + primaryKeyLength + 4,
              newValue,
              false);
          break;
        case SYSTEM_SUPREME_RECORD:
        case SYSTEM_INFIMUM_RECORD:
          IO.write(
              transactionId,
              page,
              pos - nullBitmapLength - 4,
              nullBitmapLength + 4 + 2,
              newValue,
              false);
          break;
        default:
          break;
      }
    }

    /**
     * set next record using {@code nextAbsoluteOffset} and {@code nextRecordInPage}.
     *
     * @param nextRecord next record
     */
    public void setNextRecordInPage(RecordInPage nextRecord) {
      this.nextAbsoluteOffset = nextRecord.myOffset;
      this.nextRecordInPage = nextRecord;
    }

    @Override
    public String toString() {
      StringBuilder result =
          new StringBuilder(
              "RecordInPage "
                  + "myOffset="
                  + myOffset
                  + ", flags="
                  + flags
                  + ", recordType="
                  + recordType
                  + ", nextAbsoluteOffset="
                  + nextAbsoluteOffset
                  + ", childPageId="
                  + childPageId);
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

  public IndexPage(byte[] bytes, boolean suiteRecover) {
    super(bytes);
    if (!suiteRecover) {
      infimumRecord =
          RecordInPage.createRecordInPageEntry(
              RecordInPage.SYSTEM_INFIMUM_RECORD, 0, 0, 0, 52 + 10, 0);
      if (pageType == INDEX_PAGE) {
        parseIndexHeader();
        parseAllRecords();
        if (infimumRecord.nextRecordInPage.recordType == RecordInPage.USER_POINTER_RECORD) {
          this.pageReadAndWriteLatch = null;
        }
      }
    }
  }

  /**
   * create an initialized index page.
   *
   * @param transactionId transactionId who creates this page
   * @param spaceId spaceId
   * @param pageId pageId
   * @return index page
   */
  public static IndexPage createIndexPage(long transactionId, int spaceId, int pageId) {
    IndexPage indexPage = new IndexPage(new byte[ServerRuntime.config.pageSize], false);
    indexPage.spaceId = spaceId;
    indexPage.pageId = pageId;
    IO.traceNewPage(indexPage);
    indexPage.pageType = INDEX_PAGE;
    RecordInPage supremeRecord =
        RecordInPage.createRecordInPageEntry(RecordInPage.SYSTEM_SUPREME_RECORD, 0, 0, 0, 0, 0);
    if (pageId == ServerRuntime.config.indexRootPageIndex) supremeRecord.setRightest();
    indexPage.writeFILHeader(transactionId);
    indexPage.writeIndexHeader(transactionId);
    indexPage.infimumRecord.write(transactionId, indexPage, 52 + 4);
    supremeRecord.write(transactionId, indexPage, 52 + 10);
    indexPage.infimumRecord.nextRecordInPage = supremeRecord;
    indexPage.freespaceStart.set(64);
    return indexPage;
  }

  /**
   * if the index node is the root node.
   *
   * @return true if it is the root.
   */
  public boolean isRoot() {
    return this.pageId == ServerRuntime.config.indexRootPageIndex;
  }

  public void parseIndexHeader() {
    freespaceStart.set(parseShortBig(32 + 4));
  }

  public void writeAll(long transactionId) {
    writeFILHeader(transactionId);
    writeIndexHeader(transactionId);
    RecordInPage record = infimumRecord;
    while (true) {
      record.write(transactionId, this, record.myOffset);
      if (record.recordType == RecordInPage.SYSTEM_SUPREME_RECORD) break;
      record = record.nextRecordInPage;
    }
  }

  /**
   * write index header to both disk buffer and WAL log buffer.
   *
   * @param transactionId transactionId
   */
  public void writeIndexHeader(long transactionId) {
    byte[] newValue = new byte[18];
    newValue[4] = (byte) (freespaceStart.get() >> 8);
    newValue[5] = (byte) freespaceStart.get();
    IO.write(transactionId, this, 32, 18, newValue, false);
  }

  /**
   * Parse the system + user record part of this page using {@code page.bytes}. {@code this.spaceId}
   * shall be parsed first. <b> {@code this.spaceId} and its tableMetadata must be traceable in
   * ServerRuntime. </b>
   *
   * @param transactionId transaction that request this method
   * @return a pair. The first is the right pageId, the second are records excluding infimumRecord
   *     and supremeRecord.
   */
  public Pair<Integer, ArrayList<RecordLogical>> getAllRecordLogical(long transactionId) {
    ServerRuntime.getReadLock(transactionId, this.pageReadAndWriteLatch, this);
    ArrayList<RecordLogical> recordList = new ArrayList<>();

    RecordInPage record = infimumRecord;
    while (record.recordType != RecordInPage.SYSTEM_SUPREME_RECORD) {
      if (record.recordType != RecordInPage.SYSTEM_INFIMUM_RECORD) {
        if (record.isNotDeleted()) {
          recordList.add(new RecordLogical(record));
        }
      }
      record = record.nextRecordInPage;
    }
    ServerRuntime.releaseReadLock(this.pageReadAndWriteLatch);
    return new Pair<>(record.nextAbsoluteOffset, recordList);
  }

  /**
   * get leftmost leaf page (data page) of this tree. This method shall be only used by root page.
   *
   * @param transactionId transaction id
   * @return if root is a leaf page, return (0, all records in root). Otherwise, return
   *     (leftmostDataPageId, empty array).
   */
  public Pair<Integer, ArrayList<RecordLogical>> getLeftmostDataPage(long transactionId) {
    if (this.pageId != ServerRuntime.config.indexRootPageIndex) {
      /* only root page can have access to this method. */
      return null;
    }
    if (this.pageReadAndWriteLatch == null)
      return new Pair<>(ServerRuntime.config.indexLeftmostLeafIndex, new ArrayList<>());
    firstSplitLock.lock();
    if (this.infimumRecord.nextRecordInPage.recordType != RecordInPage.SYSTEM_SUPREME_RECORD) {
      firstSplitLock.unlock();
      return new Pair<>(ServerRuntime.config.indexLeftmostLeafIndex, new ArrayList<>());
    }
    try {
      IndexPage leftPage =
          (IndexPage) IO.read(this.spaceId, ServerRuntime.config.indexLeftmostLeafIndex);
      ServerRuntime.getWriteLock(transactionId, leftPage.pageReadAndWriteLatch, leftPage);
      firstSplitLock.unlock();
      return new Pair<>(0, new ArrayList<>());
    } catch (Exception e) {
      System.out.println(e);
      exit(24);
    }
    /* This shall never happen! */
    return new Pair<>(-1, new ArrayList<>());
  }

  /**
   * Parse the system + user record part of this page using {@code page.bytes}. Save them into
   * this.records; <b> {@code this.spaceId} and its tableMetadata must be traceable in
   * ServerRuntime.</b> The method shall be only called once when inputting this page from disk.
   */
  public void parseAllRecords() {
    Table.TableMetadata metadata = ServerRuntime.tableMetadata.get(this.spaceId);
    int currentPos = 52 + 4;
    RecordInPage record = infimumRecord;
    while (true) {
      record.parseDeeplyInPage(this, currentPos, metadata);
      if (currentPos == 52 + 10) {
        break;
      }
      currentPos = record.nextAbsoluteOffset;
      record.nextRecordInPage = new RecordInPage();
      record = record.nextRecordInPage;
    }
  }

  /**
   * If there is enough space to insert certain record.
   *
   * @param maxLength max length of record
   * @return true for not safe.
   */
  private boolean notSafeToInsert(int maxLength) {
    return freespaceStart.get() + maxLength >= ServerRuntime.config.pageSize;
  }

  /**
   * insert {@code recordToBeInserted} (a data record) into this page just after the {@code
   * previousRecord}. If {@code recordToBeInserted} has the same primary keys as {@code
   * previousRecord}, while {@code previousRecord} is mark as deleted, it will update the old record
   * instead of inserting new one. <br>
   * <br>
   * We assume there is enough space for it. <br>
   * <br>
   * {@code bLinkTreeLatch} and {@code 2PL-WriteLock} shall be acquired before calling this method.
   * No lock operation shall be done in this scope. <br>
   * <br>
   * This method writes <b> ATOMICALLY </b>. Read operations can be performed without locks. This is
   * because operations on iterative structures ({@code nextRecordInPage}) are atomic. <br>
   * <br>
   * This method implements {@code A <- node.insert(A, w, v)} in the paper. <br>
   *
   * @param transactionId transaction
   * @param recordToBeInserted record to be inserted
   * @param previousRecord record that is just before the record to be inserted
   */
  private void insertDataRecordInternal(
      long transactionId, RecordLogical recordToBeInserted, RecordInPage previousRecord) {

    Table.TableMetadata metadata = ServerRuntime.tableMetadata.get(this.spaceId);

    RecordInPage record = makeRecordInPageFromLogical(recordToBeInserted, metadata);
    if (previousRecord.nextRecordInPage.recordType == RecordInPage.SYSTEM_SUPREME_RECORD
        || ValueWrapper.compareArray(
                recordToBeInserted.primaryKeyValues,
                previousRecord.nextRecordInPage.primaryKeyValues)
            != 0) {
      /* We allocate some free space and insert this record. */

      record.setNextRecordInPage(previousRecord.nextRecordInPage);
      record.myOffset = this.freespaceStart.get() + 4 + metadata.getNullBitmapLengthInByte();

      previousRecord.nextAbsoluteOffset = record.myOffset;
      /* ********************** BEGIN ATOMIC ********************** */
      previousRecord.nextRecordInPage = record;
      /* ********************** END ATOMIC ********************** */

      this.freespaceStart.set(
          record.myOffset
              + metadata.getPrimaryKeyLength()
              + metadata.getNonPrimaryKeyLength()
              + 15);
      writeIndexHeader(transactionId);
      record.write(transactionId, this, record.myOffset);
      if (record.myOffset == record.nextAbsoluteOffset) {
        exit(1);
      }
      previousRecord.write(transactionId, this, previousRecord.myOffset);
    } else {
      /* The space has already been allocated. However, the record on it is deleted. */
      record.setNextRecordInPage(previousRecord.nextRecordInPage.nextRecordInPage);

      record.myOffset = previousRecord.nextRecordInPage.myOffset;
      /* reference assignment is atomic.*/
      /* ********************** BEGIN ATOMIC ********************** */
      previousRecord.nextRecordInPage = record;
      /* ********************** END ATOMIC ********************** */

      writeIndexHeader(transactionId);
      record.write(transactionId, this, record.myOffset);
      if (record.myOffset == record.nextAbsoluteOffset) {
        exit(1);
      }
    }
  }

  /**
   * make a RecordInPage (data record) just as the data in record logical
   *
   * @param recordToBeInserted record to be copied
   * @param metadata metadata of table
   * @return record in page object that is newly made
   */
  public static RecordInPage makeRecordInPageFromLogical(
      RecordLogical recordToBeInserted, Table.TableMetadata metadata) {
    RecordInPage record;
    record =
        RecordInPage.createRecordInPageEntry(
            RecordInPage.USER_DATA_RECORD,
            metadata.getPrimaryKeyLength(),
            metadata.getNonPrimaryKeyLength(),
            metadata.getNullBitmapLengthInByte(),
            0,
            0);

    record.primaryKeyValues = new ValueWrapper[recordToBeInserted.primaryKeyValues.length];
    record.nonPrimaryKeyValues = new ValueWrapper[recordToBeInserted.nonPrimaryKeyValues.length];
    int primaryKeyNumber = metadata.getPrimaryKeyNumber();
    int nonPrimaryKeyNumber = metadata.getNonPrimaryKeyNumber();
    ArrayList<Integer> primaryOffsetList = metadata.getPrimaryOffsetInOrder();
    ArrayList<Integer> nonPrimaryKeyOffsetList = metadata.getNonPrimaryKeyOffsetInOrder();
    try {
      record.primaryKeys = new byte[metadata.getPrimaryKeyLength()];
      for (int i = 0; i < primaryKeyNumber; i++) {
        record.primaryKeyValues[i] = new ValueWrapper(recordToBeInserted.primaryKeyValues[i]);
        System.arraycopy(
            recordToBeInserted.primaryKeyValues[i].bytes,
            0,
            record.primaryKeys,
            primaryOffsetList.get(i),
            recordToBeInserted.primaryKeyValues[i].bytes.length);
      }

      record.nonPrimaryKeys = new byte[metadata.getNonPrimaryKeyLength()];
      for (int i = 0; i < nonPrimaryKeyNumber; i++) {
        record.nonPrimaryKeyValues[i] = new ValueWrapper(recordToBeInserted.nonPrimaryKeyValues[i]);
        if (!record.nonPrimaryKeyValues[i].isNull) {
          System.arraycopy(
              recordToBeInserted.nonPrimaryKeyValues[i].bytes,
              0,
              record.nonPrimaryKeys,
              nonPrimaryKeyOffsetList.get(i),
              recordToBeInserted.nonPrimaryKeyValues[i].bytes.length);
        }
        record.setNull(i, record.nonPrimaryKeyValues[i].isNull);
      }
    } catch (Exception e) {
      System.out.println(e);
      exit(1234);
    }
    return record;
  }

  /**
   * insert {@code dataRecordToBeInserted} (a data record) into b-link tree
   *
   * @param transactionId transaction
   * @param dataRecordToBeInserted data record to be inserted
   * @return true if succeed
   * @throws Exception IO error
   */
  public boolean insertDataRecordIntoTree(
      long transactionId, RecordLogical dataRecordToBeInserted) {
    Pair<Boolean, RecordInPage> result;
    Stack<IndexPage> ancestors = new Stack<>();
    IndexPage currentPage = this;

    if (currentPage.isRoot()
        && currentPage.infimumRecord.nextRecordInPage.recordType
            == RecordInPage.SYSTEM_SUPREME_RECORD) {
      // TODO: prettier plz :-)
      /* The first insert of the whole b-link tree. Assert(currentPage == rootPage)*/
      currentPage.firstSplitLock.lock();
      if (!(currentPage.isRoot()
          && currentPage.infimumRecord.nextRecordInPage.recordType
              == RecordInPage.SYSTEM_SUPREME_RECORD)) {
        /* double check:
        root page has been already split.
        The overhead is accepted because it will only happen a few times. */
        currentPage.firstSplitLock.unlock();
        return insertDataRecordIntoTree(transactionId, dataRecordToBeInserted);
      } else {
        Table.TableMetadata metadata = ServerRuntime.tableMetadata.get(this.spaceId);

        /* The page has already been inited when creating table. */
        int leftPageId = ServerRuntime.config.indexLeftmostLeafIndex;
        IndexPage leftPage = null;
        try {
          leftPage = (IndexPage) IO.read(this.spaceId, leftPageId);
        } catch (Exception e) {
          e.printStackTrace();
          exit(60);
        }
        /* 2PL write lock of new page */
        ServerRuntime.getWriteLock(transactionId, leftPage.pageReadAndWriteLatch, leftPage);

        /* prepare records in new left page*/
        RecordInPage leftPageSupremeRecord = leftPage.infimumRecord.nextRecordInPage;
        RecordInPage onlyRecordInLeft =
            makeRecordInPageFromLogical(dataRecordToBeInserted, metadata);
        onlyRecordInLeft.myOffset = 64 + metadata.getNullBitmapLengthInByte() + 4;

        /* build link in new left page */
        leftPage.infimumRecord.setNextRecordInPage(onlyRecordInLeft);
        onlyRecordInLeft.setNextRecordInPage(leftPageSupremeRecord);

        /* set left page as the rightest leaf page. */
        leftPageSupremeRecord.setRightest();
        leftPageSupremeRecord.nextAbsoluteOffset = 0;

        /* prepare pointer record towards left page in root page. */
        RecordInPage newSupremeRecord =
            RecordInPage.createRecordInPageEntry(
                RecordInPage.SYSTEM_SUPREME_RECORD, 0, 0, 0, leftPageId, 0);
        RecordInPage leftPointerRecord = makePointerRecord(onlyRecordInLeft, leftPageId);

        /* build link in root page */
        leftPointerRecord.myOffset = 64 + 4;
        leftPointerRecord.setNextRecordInPage(newSupremeRecord);

        /* set root page as the rightest */
        newSupremeRecord.setRightest();

        /* update freespace */
        this.freespaceStart.set(leftPointerRecord.myOffset + metadata.getPrimaryKeyNumber() + 4);
        leftPage.freespaceStart.set(
            onlyRecordInLeft.myOffset
                + metadata.getNonPrimaryKeyLength()
                + metadata.getPrimaryKeyLength()
                + 15);

        //        System.out.println("FIRST INSERT: no write lock can get.");
        //        System.out.println("FIRST INSERT: no write lock can get!! got!");

        infimumRecord.nextAbsoluteOffset = leftPointerRecord.myOffset;
        /* ******************************** BEGIN ATOMIC ********************* */
        infimumRecord.nextRecordInPage = leftPointerRecord;
        /* ******************************** END ATOMIC ********************* */

        leftPage.writeAll(transactionId);
        this.writeAll(transactionId);

        //        ServerRuntime.father.put(concat(this.spaceId, leftPageId), leftPointerRecord);

        //        currentPage.pageReadAndWriteLatch.writeLock().unlock();
        currentPage.pageReadAndWriteLatch = null;
        currentPage.firstSplitLock.unlock();
        return true;
      }
    }
    do {
      result = currentPage.scanInternal(transactionId, dataRecordToBeInserted.primaryKeyValues);
      if (result.right.recordType == RecordInPage.USER_POINTER_RECORD) {
        if (result.right.isPointToNextLevel()) ancestors.add(currentPage);
        currentPage = (IndexPage) IO.read(this.spaceId, result.right.childPageId);
      } else if (result.right.recordType == RecordInPage.SYSTEM_SUPREME_RECORD) {
        if (result.right.isPointToNextLevel()) ancestors.add(currentPage);
        currentPage = (IndexPage) IO.read(this.spaceId, result.right.nextAbsoluteOffset);
      } else break;
    } while (true);

    return currentPage.moveRightAndInsertData(transactionId, dataRecordToBeInserted, ancestors);
  }

  /**
   * move right until find the proper page to insert data record {@code dataRecordToBeInserted},
   * which either contains a data record just larger than the data record to be inserted, or is the
   * last page (rightest and lowest) in the tree. <br>
   * {@code ancestors} are provided for possible splitting. It is a stack containing the rightmost
   * page of each layer above. This method implements {@code moveRight} in the paper. <br>
   *
   * @param transactionId transaction
   * @param dataRecordToBeInserted data record to be inserted
   * @param ancestors a stack containing the rightmost page of each layer above
   * @return true if the insertion succeed
   */
  private boolean moveRightAndInsertData(
      long transactionId, RecordLogical dataRecordToBeInserted, Stack<IndexPage> ancestors) {

    IndexPage currentPage = this;
    if (currentPage.infimumRecord.nextRecordInPage.recordType == RecordInPage.USER_POINTER_RECORD) {
      System.out.println("why insert data into pointer page?");
      exit(30);
    }
    Table.TableMetadata metadata = ServerRuntime.tableMetadata.get(this.spaceId);
    Pair<Boolean, RecordInPage> insertResult;
    do {
      insertResult =
          currentPage.scanInternal(transactionId, dataRecordToBeInserted.primaryKeyValues);
      if (insertResult.right.recordType != RecordInPage.SYSTEM_SUPREME_RECORD) {
        ServerRuntime.getWriteLock(transactionId, currentPage.pageReadAndWriteLatch, currentPage);
        insertResult =
            currentPage.scanInternal(transactionId, dataRecordToBeInserted.primaryKeyValues);

        if (insertResult.right.recordType != RecordInPage.SYSTEM_SUPREME_RECORD) {

          /* The record already exists. */
          if (insertResult.left) return false;

          if (currentPage.notSafeToInsert(
              metadata.getMaxRecordLength(RecordInPage.USER_DATA_RECORD))) {

            /* split currentPage and require the 2PL lock of right page.*/
            currentPage.bLinkTreeLatch.lock();
            IndexPage rightPage = currentPage.splitMyself(transactionId, ancestors, true);
            currentPage.bLinkTreeLatch.unlock();
            if (rightPage == null) return false;

            insertResult =
                currentPage.scanInternal(transactionId, dataRecordToBeInserted.primaryKeyValues);
            if (insertResult.right.recordType != RecordInPage.SYSTEM_SUPREME_RECORD) {
              /* insert it into current page. */
              currentPage.bLinkTreeLatch.lock();
              currentPage.insertDataRecordInternal(
                  transactionId, dataRecordToBeInserted, insertResult.right);
              currentPage.bLinkTreeLatch.unlock();
              return true;
            } else {
              /* insert it into right page. */
              /* first find the insert position of right page. */
              rightPage.bLinkTreeLatch.lock();
              insertResult =
                  rightPage.scanInternal(transactionId, dataRecordToBeInserted.primaryKeyValues);
              rightPage.insertDataRecordInternal(
                  transactionId, dataRecordToBeInserted, insertResult.right);
              rightPage.bLinkTreeLatch.unlock();
              return true;
            }
          } else {
            /* safely insert! */
            currentPage.bLinkTreeLatch.lock();
            currentPage.insertDataRecordInternal(
                transactionId, dataRecordToBeInserted, insertResult.right);
            currentPage.bLinkTreeLatch.unlock();
            return true;
          }
        }
        return true;

      } else {
        try {
          currentPage = (IndexPage) IO.read(this.spaceId, insertResult.right.nextAbsoluteOffset);
        } catch (Exception neverShallHappen) {
          exit(7);
        }
      }
    } while (true);
  }

  /**
   * insert pointer record (points to child page) internal this page.
   *
   * @param transactionId transaction Id
   * @param maxRecordInChildren max record in children
   * @param childPageToPoint child page
   * @param previousPointerRecord previous pointer record
   * @param newPreviousPointerRecordValue new value of pointer record. (copy constructor shall be
   *     used when do this.)
   */
  public void insertPointerRecordInternal(
      long transactionId,
      RecordInPage maxRecordInChildren,
      int childPageToPoint,
      RecordInPage previousPointerRecord,
      RecordInPage newPreviousPointerRecordValue) {

    int primaryKeyLength = maxRecordInChildren.primaryKeys.length;
    RecordInPage pointerRecordToBeInserted =
        makePointerRecord(maxRecordInChildren, childPageToPoint);
    pointerRecordToBeInserted.myOffset = this.freespaceStart.get() + 4;
    this.freespaceStart.set(pointerRecordToBeInserted.myOffset + primaryKeyLength + 4);

    /* set next link */
    RecordInPage nextRecord = previousPointerRecord.nextRecordInPage;
    pointerRecordToBeInserted.setNextRecordInPage(nextRecord);

    /* set rightest and modify pointer to next layer. */
    if (nextRecord.recordType == RecordInPage.SYSTEM_SUPREME_RECORD && nextRecord.isRightest()) {
      nextRecord.nextAbsoluteOffset = childPageToPoint;
    }

    previousPointerRecord.nextAbsoluteOffset = pointerRecordToBeInserted.myOffset;
    /* reference assignment is atomic.*/
    /* ********************** BEGIN ATOMIC ********************** */
    previousPointerRecord.nextRecordInPage = pointerRecordToBeInserted;
    /* ********************** END ATOMIC ********************** */

    /* modify previous pointer record's value */
    previousPointerRecord.primaryKeys =
        Arrays.copyOf(
            newPreviousPointerRecordValue.primaryKeys,
            newPreviousPointerRecordValue.primaryKeys.length);
    ValueWrapper[] tmpValue =
        new ValueWrapper[newPreviousPointerRecordValue.primaryKeyValues.length];
    for (int i = 0; i < newPreviousPointerRecordValue.primaryKeyValues.length; i++) {
      tmpValue[i] = new ValueWrapper(newPreviousPointerRecordValue.primaryKeyValues[i]);
    }
    previousPointerRecord.primaryKeyValues = tmpValue;

    /* write to bytes */
    writeIndexHeader(transactionId);
    pointerRecordToBeInserted.write(transactionId, this, pointerRecordToBeInserted.myOffset);
    previousPointerRecord.write(transactionId, this, previousPointerRecord.myOffset);

    //    ServerRuntime.father.put(concat(this.spaceId, childPageToPoint),
    // pointerRecordToBeInserted);
  }

  /**
   * split root. The records in the root node are divided equally into two new pages. After
   * splitting, the root node has two records that point to these two new pages. {@code
   * this.bLinkTreeLatch} is held before and for the entire time.
   *
   * <p>Since we make sure the root is split when the first record is inserted into b link tree.
   * Therefore, we shall only handle situations when the root is pointer page.
   *
   * @param transactionId transaction
   * @return left of newly made two pages.
   */
  public IndexPage splitRoot(long transactionId) {
    Table.TableMetadata metadata = ServerRuntime.tableMetadata.get(this.spaceId);

    ArrayList<RecordInPage> recordInPage = new ArrayList<>();
    RecordInPage oldSupremeRecord = getRecordInPageAndReturnSupreme(recordInPage);
    if (recordInPage.size() < 2) {
      System.out.println("shall not split a node which only have two records.");
      exit(123);
    }

    int leftPageId = 0;
    int rightPageId = 0;
    try {
      OverallPage overallPage =
          (OverallPage) IO.read(this.spaceId, ServerRuntime.config.overallPageIndex);
      leftPageId = overallPage.allocatePage(transactionId);
      rightPageId = overallPage.allocatePage(transactionId);
    } catch (Exception e) {
      exit(8);
    }

    /* set left page.*/
    IndexPage leftPage = createIndexPage(transactionId, this.spaceId, leftPageId);
    RecordInPage leftPageSupremeRecord = leftPage.infimumRecord.nextRecordInPage;
    RecordInPage maxRecordInLeft =
        prepareHalfPageRecordList(leftPage, metadata, recordInPage, 0, recordInPage.size() / 2);
    leftPageSupremeRecord.nextAbsoluteOffset = rightPageId;
    leftPageSupremeRecord.unsetRightest();
    leftPage.pageReadAndWriteLatch = null;
    leftPage.freespaceStart.set(
        maxRecordInLeft.myOffset
            + metadata.getNonPrimaryKeyLength()
            + metadata.getPrimaryKeyLength()
            + 15);

    /* set right page.*/
    IndexPage rightPage = createIndexPage(transactionId, this.spaceId, rightPageId);
    RecordInPage rightPageSupremeRecord = rightPage.infimumRecord.nextRecordInPage;
    RecordInPage maxRecordInRight =
        prepareHalfPageRecordList(
            rightPage, metadata, recordInPage, recordInPage.size() / 2, recordInPage.size());
    /* set link to the right of next layer. */
    rightPageSupremeRecord.nextAbsoluteOffset = oldSupremeRecord.nextAbsoluteOffset;
    rightPageSupremeRecord.setRightest();
    rightPage.pageReadAndWriteLatch = null;
    rightPage.freespaceStart.set(
        maxRecordInRight.myOffset
            + metadata.getNonPrimaryKeyLength()
            + metadata.getPrimaryKeyLength()
            + 15);

    RecordInPage leftPointerRecord = makePointerRecord(maxRecordInLeft, leftPageId);
    leftPointerRecord.myOffset = 64 + 4 + metadata.getNullBitmapLengthInByte();

    RecordInPage rightPointerRecord = makePointerRecord(maxRecordInRight, rightPageId);
    rightPointerRecord.myOffset =
        leftPointerRecord.myOffset + metadata.getMaxRecordLength(RecordInPage.USER_POINTER_RECORD);

    RecordInPage newSupremeRecord =
        RecordInPage.createRecordInPageEntry(
            RecordInPage.SYSTEM_SUPREME_RECORD, 0, 0, 0, rightPageId, 0);
    newSupremeRecord.setRightest();

    /* build root link list. */
    leftPointerRecord.setNextRecordInPage(rightPointerRecord);
    rightPointerRecord.setNextRecordInPage(newSupremeRecord);
    this.freespaceStart.set(rightPointerRecord.myOffset + metadata.getPrimaryKeyNumber() + 4);

    infimumRecord.nextAbsoluteOffset = leftPointerRecord.myOffset;
    /* ******************************** BEGIN ATOMIC ********************* */
    infimumRecord.nextRecordInPage = leftPointerRecord;
    /* ******************************** END ATOMIC ********************* */

    //    ServerRuntime.father.put(concat(this.spaceId, leftPageId), leftPointerRecord);
    //    ServerRuntime.father.put(concat(this.spaceId, rightPageId), rightPointerRecord);

    leftPage.writeAll(transactionId);
    rightPage.writeAll(transactionId);
    this.writeAll(transactionId);
    return leftPage;
  }

  /**
   * split myself into two pages. The records in the current node are divided equally into two
   * pages. The smaller half of the record is left in the original page, and the larger half is
   * placed in the new page. The new page is exactly to the right of the original page. Suitable
   * pointer record is added to the parent node as well. <br>
   * If the join involves a node split, it completes recursively. {@code this.bLinkTreeLatch} is
   * held before and during the entire process.
   *
   * @param transactionId transaction
   * @param ancestors a stack containing the rightmost page of each layer above
   * @return the newly made page, which is the right of {@code this}.
   */
  private IndexPage splitMyself(
      long transactionId, Stack<IndexPage> ancestors, boolean requireLock) {

    if (isRoot()) {
      return splitRoot(transactionId);
    }

    Table.TableMetadata metadata = ServerRuntime.tableMetadata.get(this.spaceId);
    ArrayList<RecordInPage> originalRecordsInPage = new ArrayList<>();
    RecordInPage oldSupremeRecord = getRecordInPageAndReturnSupreme(originalRecordsInPage);
    if (originalRecordsInPage.size() < 2) {
      System.out.println("split myself but I have less than 2 records.");
      exit(9);
      return null;
    }

    int rightPageId;
    try {
      OverallPage overallPage =
          (OverallPage) IO.read(this.spaceId, ServerRuntime.config.overallPageIndex);
      rightPageId = overallPage.allocatePage(transactionId);
    } catch (Exception e) {

      e.printStackTrace();
      System.out.println("splitMyself: allocate new page.");
      exit(10);
      return null;
    }

    IndexPage rightPage = createIndexPage(transactionId, this.spaceId, rightPageId);
    RecordInPage rightPageSupremeRecord = rightPage.infimumRecord.nextRecordInPage;
    RecordInPage maxRecordInRight =
        prepareHalfPageRecordList(
            rightPage,
            metadata,
            originalRecordsInPage,
            originalRecordsInPage.size() / 2,
            originalRecordsInPage.size());
    if (maxRecordInRight == null) {
      System.out.println("splitMyself: maxRecordInRight is null.");
      exit(11);
      return null;
    }
    maxRecordInRight.setNextRecordInPage(rightPageSupremeRecord);
    rightPage.freespaceStart.set(
        maxRecordInRight.myOffset
            + metadata.getMaxRecordLength(maxRecordInRight.recordType)
            - 4
            - maxRecordInRight.nullBitmap.length);

    if (this.pageReadAndWriteLatch == null) {
      rightPage.pageReadAndWriteLatch = null;
      // TODO: update suite
    } else {
      ServerRuntime.getWriteLock(transactionId, rightPage.pageReadAndWriteLatch, rightPage);
    }

    /* make new supreme record */
    RecordInPage newLeftPageSupremeRecord =
        makeNewSupremeRecordInLeft(oldSupremeRecord, rightPageId, rightPageSupremeRecord);

    RecordInPage maxRecordInLeft =
        originalRecordsInPage.get((originalRecordsInPage.size() / 2) - 1);

    maxRecordInLeft.nextAbsoluteOffset = newLeftPageSupremeRecord.myOffset;
    /* ********************** BEGIN ATOMIC ********************** */
    maxRecordInLeft.nextRecordInPage = newLeftPageSupremeRecord;
    /* ********************** END ATOMIC ********************** */

    /* replace records' position in left */
    RecordInPage previousRecord = infimumRecord;
    RecordInPage reposRecord = infimumRecord.nextRecordInPage;
    int currentPos = 64 + metadata.getNullBitmapLengthInByte() + 4;
    while (reposRecord.recordType != RecordInPage.SYSTEM_SUPREME_RECORD) {
      reposRecord.myOffset = currentPos;
      previousRecord.nextAbsoluteOffset = reposRecord.myOffset;

      previousRecord = reposRecord;
      reposRecord = reposRecord.nextRecordInPage;

      currentPos += metadata.getMaxRecordLength(reposRecord.recordType);
    }

    this.freespaceStart.set(currentPos - 4 - metadata.getNullBitmapLengthInByte());

    int maxLength = metadata.getMaxRecordLength(RecordInPage.USER_POINTER_RECORD);
    IndexPage maybeParent = ancestors.pop();
    if (!maybeParent.moveRightAndInsertPointer(
        transactionId,
        ancestors,
        maxLength,
        this.pageId,
        rightPageId,
        maxRecordInLeft,
        maxRecordInRight)) {
      System.out.println("The pointer record is missing for splitting process.");
      exit(31);
    }

    this.writeAll(transactionId);
    rightPage.writeAll(transactionId);

    return rightPage;
  }

  /**
   * get all the records in page and return the supreme record at the time. {@code
   * this.bLinkTreeLatch} is not necessary.
   *
   * <p>Deleted records are also returned.
   *
   * @param recordInPage Array List of records
   * @return supreme record at the time
   */
  private RecordInPage getRecordInPageAndReturnSupreme(ArrayList<RecordInPage> recordInPage) {
    RecordInPage record = this.infimumRecord;
    while (record.recordType != RecordInPage.SYSTEM_SUPREME_RECORD) {
      if (record != infimumRecord) recordInPage.add(record);
      record = record.nextRecordInPage;
    }
    return record;
  }

  /**
   * put {@code recordInPage[begin, end)} into {@code halfPage} under the context of {@code
   * (table)metadata}. Specifically, the records are arranged in a proper linked list that is
   * connected to {@code halfPage.infimumRecord}. <br>
   * {@code this.bLinkTreeLatch} is not necessary. Since this method will only be called when
   * splitting. No other transactions have access to this page until the page is fully constructed.
   *
   * @param halfPage halfPage to construct
   * @param metadata metadata of table
   * @param recordInPage records
   * @param begin begin index
   * @param end end index
   * @return the rightmost record in the page (before supremeRecord)
   */
  private static RecordInPage prepareHalfPageRecordList(
      IndexPage halfPage,
      Table.TableMetadata metadata,
      ArrayList<RecordInPage> recordInPage,
      int begin,
      int end) {
    RecordInPage prevRecord = halfPage.infimumRecord;
    RecordInPage supremeRecord = halfPage.infimumRecord.nextRecordInPage;
    RecordInPage shadowRecord = null;
    int currentPos = 64 + metadata.getNullBitmapLengthInByte() + 4;
    for (int i = begin; i < end; i++) {
      shadowRecord = new RecordInPage(recordInPage.get(i));
      shadowRecord.myOffset = currentPos;

      prevRecord.setNextRecordInPage(shadowRecord);
      prevRecord = shadowRecord;

      currentPos = currentPos + metadata.getMaxRecordLength(shadowRecord.recordType);
    }
    prevRecord.setNextRecordInPage(supremeRecord);
    return shadowRecord;
  }

  /**
   * make newSupremeRecord in left half of the page. This method shall be called when splitting a
   * page. This method constructs the appropriate supreme record, adjusts it and the flags on the
   * page to its right, and ensures the correctness of the link pointer.
   *
   * @param oldSupremeRecord old supremeRecord
   * @param rightPageId rightPageId
   * @param rightPageSupremeRecord rightPage supremeRecord
   * @return newly made supreme record
   */
  private static RecordInPage makeNewSupremeRecordInLeft(
      RecordInPage oldSupremeRecord, int rightPageId, RecordInPage rightPageSupremeRecord) {
    RecordInPage newSupremeRecord =
        RecordInPage.createRecordInPageEntry(
            RecordInPage.SYSTEM_SUPREME_RECORD, 0, 0, 0, rightPageId, 0);
    rightPageSupremeRecord.nextAbsoluteOffset = oldSupremeRecord.nextAbsoluteOffset;
    if (oldSupremeRecord.isRightest()) {
      newSupremeRecord.unsetRightest();
      rightPageSupremeRecord.setRightest();
    }
    return newSupremeRecord;
  }

  /**
   * make pointer record which has the value of {@code maxRecordInRight} and points to {@code
   * childPageToPoint}.
   *
   * @param maxRecordInRight value of record
   * @param childPageToPoint child page to point
   * @return pointer record
   */
  private static RecordInPage makePointerRecord(
      RecordInPage maxRecordInRight, int childPageToPoint) {
    RecordInPage record;
    record =
        RecordInPage.createRecordInPageEntry(
            RecordInPage.USER_POINTER_RECORD,
            maxRecordInRight.primaryKeys.length,
            0,
            0,
            0,
            childPageToPoint);
    System.arraycopy(
        maxRecordInRight.primaryKeys,
        0,
        record.primaryKeys,
        0,
        maxRecordInRight.primaryKeys.length);
    record.primaryKeyValues = new ValueWrapper[maxRecordInRight.primaryKeyValues.length];
    for (int i = 0; i < maxRecordInRight.primaryKeyValues.length; i++) {
      record.primaryKeyValues[i] = new ValueWrapper(maxRecordInRight.primaryKeyValues[i]);
    }
    return record;
  }

  /**
   * move right until find the proper page to insert pointer record, which is the parent of current
   * page. The parent holds a pointer record (not supreme record) exactly pointing to {@code this}.
   * The pointer record to be inserted has the value of maxRecordInRight and points to {@code
   * childPageToPoint}. It's not checked, but the {@code childPageToPoint} should be exactly the one
   * on {@code this} right.
   *
   * <p>{@code ancestors} are provided for possible splitting. It is a stack containing the
   * rightmost page of each layer above. This method implements {@code moveRight} in the paper. <br>
   * *
   *
   * @param transactionId transaction
   * @param ancestors a stack containing the rightmost page of each layer above
   * @param maxLength max length of pointer record
   * @param childPageToPoint child page to point
   * @param maxRecordInRight value of pointer record
   * @return true if succeed
   */
  private boolean moveRightAndInsertPointer(
      long transactionId,
      Stack<IndexPage> ancestors,
      int maxLength,
      int leftOfChildPageId,
      int childPageToPoint,
      RecordInPage maxRecordInLeft,
      RecordInPage maxRecordInRight) {
    IndexPage maybeParent = this;
    Pair<Boolean, RecordInPage> insertResult;
    if (maybeParent.infimumRecord.nextRecordInPage.recordType != RecordInPage.USER_POINTER_RECORD) {
      System.out.println("never shall happen!");
      exit(15);
    }

    do {
      maybeParent.bLinkTreeLatch.lock();
      insertResult = maybeParent.scanInternalForPage(transactionId, leftOfChildPageId);
      if (insertResult.left) {

        if (maybeParent.notSafeToInsert(maxLength)) {
          IndexPage candidateParent = maybeParent.splitMyself(transactionId, ancestors, false);
          if (candidateParent == null) {
            exit(12);
            return false;
          }
          if (maybeParent.isRoot()) {
            ancestors.push(maybeParent);
          } else {
            insertResult = maybeParent.scanInternalForPage(transactionId, leftOfChildPageId);
            if (insertResult.left) {
              /* split and the pointer record is in the left. */
              maybeParent.insertPointerRecordInternal(
                  transactionId,
                  maxRecordInRight,
                  childPageToPoint,
                  insertResult.right,
                  maxRecordInLeft);
              maybeParent.bLinkTreeLatch.unlock();
              return true;
            }
          }
          maybeParent.bLinkTreeLatch.unlock();
          maybeParent = candidateParent;
        } else {
          maybeParent.insertPointerRecordInternal(
              transactionId,
              maxRecordInRight,
              childPageToPoint,
              insertResult.right,
              maxRecordInLeft);
          maybeParent.bLinkTreeLatch.unlock();
          return true;
        }
      } else {
        IndexPage previousPage = maybeParent;
        try {
          maybeParent = (IndexPage) IO.read(this.spaceId, insertResult.right.nextAbsoluteOffset);
        } catch (Exception neverShallHappen) {
          System.out.println(maybeParent.isRightest() + " " + insertResult);
          System.out.println(neverShallHappen);
          System.out.println("move right and insert pointer: IO.read");
          // TODO: insertResult.right.nextAbsoluteOffset = 0? ???? tOFIX
          exit(13);
        }
        previousPage.bLinkTreeLatch.unlock();
      }
    } while (true);
  }

  /**
   * scan tree for search key
   *
   * @param transactionId transaction id
   * @param searchKey search key
   * @return (true, RecordInPage with proper search key) if found. Otherwise, return (false, largest
   *     RecordInPage less than searchKey). It returns the maximum record that is not larger than
   *     the search key. In particular, if the largest record in the tree is not larger than the
   *     search key, that largest record will be returned.
   * @throws Exception IO error
   */
  public Pair<Boolean, RecordInPage> scanTreeAndReturnRecord(
      long transactionId, ValueWrapper[] searchKey) throws Exception {
    Pair<Boolean, RecordInPage> result;
    IndexPage currentPage = this;
    do {
      result = currentPage.scanInternal(transactionId, searchKey);
      if (result.right.recordType == RecordInPage.USER_POINTER_RECORD) {
        currentPage = (IndexPage) IO.read(this.spaceId, result.right.childPageId);
      } else if (result.right.recordType == RecordInPage.SYSTEM_SUPREME_RECORD) {
        currentPage = (IndexPage) IO.read(this.spaceId, result.right.nextAbsoluteOffset);
      } else break;
    } while (true);

    do {
      ServerRuntime.getReadLock(transactionId, currentPage.pageReadAndWriteLatch, currentPage);
      result = currentPage.scanInternal(transactionId, searchKey);
      ServerRuntime.releaseReadLock(currentPage.pageReadAndWriteLatch);
      if (result.right.recordType == RecordInPage.USER_POINTER_RECORD) {
        currentPage = (IndexPage) IO.read(this.spaceId, result.right.childPageId);
      } else if (result.right.recordType == RecordInPage.SYSTEM_SUPREME_RECORD) {
        currentPage = (IndexPage) IO.read(this.spaceId, result.right.nextAbsoluteOffset);
      } else break;
    } while (true);

    return result;
  }
  /**
   * scan tree for search key and return page
   *
   * @param transactionId transaction id
   * @param searchKey search key
   * @return see as scanTreeAndReturnRecord. The nextPageId and record lists are returned.
   * @throws Exception IO error
   */
  public Pair<Integer, ArrayList<RecordLogical>> scanTreeAndReturnPage(
      long transactionId, ValueWrapper[] searchKey) throws Exception {
    Pair<Boolean, RecordInPage> result;

    IndexPage currentPage = this;
    do {
      result = currentPage.scanInternal(transactionId, searchKey);
      if (result.right.recordType == RecordInPage.USER_POINTER_RECORD) {
        currentPage = (IndexPage) IO.read(this.spaceId, result.right.childPageId);
      } else if (result.right.recordType == RecordInPage.SYSTEM_SUPREME_RECORD) {
        currentPage = (IndexPage) IO.read(this.spaceId, result.right.nextAbsoluteOffset);
      } else break;
    } while (true);

    return currentPage.getAllRecordLogical(transactionId);
  }

  /**
   * scan for primaryKey ? searchKey in the page.
   *
   * <p>If the page includes pointer records, then it returns the record that should theoretically
   * contain that key. This record may be a normal pointer record, or it may be a horizontal pointer
   * in a supremeRecord.
   *
   * <p>Otherwise, the page includes data records. If there is a record larger than search key, it
   * returns the maximum record that is not larger than the search key. When such a record does not
   * exist, it returns the horizontal pointer in the supremeRecord. (This means that the record
   * should probably be in one of the pages on the right.)
   *
   * <p>In particular, if the current page is the rightmost and bottom of the tree, then even if the
   * largest record in the page is not larger than the search key, that largest record will be
   * returned.
   *
   * @param transactionId transactionKey
   * @param searchKey value of primary key
   * @return a pair of boolean and recordInPage. The boolean indicates whether the searchKey is
   *     exactly the same as what we found. The details of returning {@code RecordInPage} are
   *     explained above.
   */
  public Pair<Boolean, RecordInPage> scanInternal(long transactionId, ValueWrapper[] searchKey) {
    RecordInPage record = infimumRecord.nextRecordInPage;
    RecordInPage previousRecord = infimumRecord;
    while (record.recordType != RecordInPage.SYSTEM_SUPREME_RECORD) {
      int compareResult = ValueWrapper.compareArray(searchKey, record.primaryKeyValues);
      if (compareResult == 0) {
        if (record.isNotDeleted()) {
          return new Pair<>(true, record);
        } else {
          return new Pair<>(false, previousRecord);
        }
      } else if (compareResult < 0) {
        if (record.recordType == RecordInPage.USER_POINTER_RECORD) return new Pair<>(false, record);
        else return new Pair<>(false, previousRecord);
      }
      previousRecord = record;
      record = record.nextRecordInPage;
    }
    if (record.nextAbsoluteOffset == 0) {
      return new Pair<>(false, previousRecord);
    } else {
      return new Pair<>(false, record);
    }
  }

  /**
   * scan for pointer record which points to {@code pageToFind}
   *
   * @param transactionId transaction
   * @param pageToFind page to find
   * @return a pair of boolean and record in page. True if the proper record is found.
   */
  public Pair<Boolean, RecordInPage> scanInternalForPage(long transactionId, int pageToFind) {
    RecordInPage record = infimumRecord.nextRecordInPage;
    while (record.recordType != RecordInPage.SYSTEM_SUPREME_RECORD) {
      if (record.recordType == RecordInPage.USER_DATA_RECORD) {
        exit(55);
      }
      if (record.recordType == RecordInPage.USER_POINTER_RECORD) {
        if (record.childPageId == pageToFind) return new Pair<>(true, record);
      }
      record = record.nextRecordInPage;
    }
    return new Pair<>(false, record);
  }

  /**
   * This method is only for test!
   *
   * @return if the page is rightest.
   */
  public boolean isRightest() {
    RecordInPage record = infimumRecord;
    while (record.recordType != RecordInPage.SYSTEM_SUPREME_RECORD) {
      record = record.nextRecordInPage;
    }
    return record.isRightest();
  }

  public interface recordCondition {
    Boolean isSatisfied(RecordInPage record);
  }

  public int deleteFromLeftmostDataPage(
      long transactionId, recordCondition condition, ArrayList<RecordInPage> recordDeleted) {
    if (this.pageReadAndWriteLatch == null) return ServerRuntime.config.indexLeftmostLeafIndex;
    firstSplitLock.lock();
    if (this.infimumRecord.nextRecordInPage.recordType != RecordInPage.SYSTEM_SUPREME_RECORD) {
      firstSplitLock.unlock();
      return ServerRuntime.config.indexLeftmostLeafIndex;
    }
    try {
      IndexPage leftPage =
          (IndexPage) IO.read(this.spaceId, ServerRuntime.config.indexLeftmostLeafIndex);
      ServerRuntime.getWriteLock(transactionId, leftPage.pageReadAndWriteLatch, leftPage);
      firstSplitLock.unlock();
    } catch (Exception e) {
      System.out.println(e);
      exit(24);
    }
    return 0;
  }

  /**
   * delete all records that satisfies the condition in this page.
   *
   * @param transactionId transaction Id
   * @param condition condition
   */
  public int deleteWithCondition(
      long transactionId, recordCondition condition, ArrayList<RecordInPage> recordsDeleted) {

    ServerRuntime.getWriteLock(transactionId, this.pageReadAndWriteLatch, this);
    bLinkTreeLatch.lock();

    RecordInPage record = infimumRecord;
    while (record.recordType != RecordInPage.SYSTEM_SUPREME_RECORD) {
      if (record.recordType != RecordInPage.SYSTEM_INFIMUM_RECORD) {
        if (condition.isSatisfied(record)) {
          if (record.isNotDeleted()) {
            record.setDeleted();
            if (recordsDeleted != null) recordsDeleted.add(record);
          }
        }
      }
      record = record.nextRecordInPage;
    }

    bLinkTreeLatch.unlock();

    return record.nextAbsoluteOffset;
  }

  /**
   * delete all records that satisfies the condition in this page.
   *
   * @param transactionId transaction Id
   * @param condition condition
   */
  public Pair<Integer, Integer> deleteWithPrimaryCondition(
      long transactionId,
      recordCondition condition,
      ValueWrapper[] searchKey,
      ArrayList<RecordInPage> recordDeleted) {
    System.out.println("enter delete with primary condition");
    /* This must be a data page. */

    if (this.infimumRecord.nextRecordInPage.recordType == RecordInPage.SYSTEM_SUPREME_RECORD) {
      firstSplitLock.lock();
      if (this.infimumRecord.nextRecordInPage.recordType == RecordInPage.SYSTEM_SUPREME_RECORD) {
        // TODO
        IndexPage leftPage = null;
        try {
          leftPage = (IndexPage) IO.read(this.spaceId, ServerRuntime.config.indexLeftmostLeafIndex);
        } catch (Exception e) {
          e.printStackTrace();
          exit(62);
        }
        ServerRuntime.getWriteLock(transactionId, leftPage.pageReadAndWriteLatch, leftPage);
        firstSplitLock.unlock();
        return new Pair<>(0, 0);
      }
      firstSplitLock.unlock();
    }

    ServerRuntime.getWriteLock(transactionId, this.pageReadAndWriteLatch, this);
    this.bLinkTreeLatch.lock();

    RecordInPage record = this.infimumRecord.nextRecordInPage;
    RecordInPage previousRecord = this.infimumRecord;
    while (record.recordType != RecordInPage.SYSTEM_SUPREME_RECORD) {
      if (record.recordType != RecordInPage.SYSTEM_INFIMUM_RECORD) {
        if (condition.isSatisfied(record)) {
          if (record.isNotDeleted()) {
            record.setDeleted();
            if (recordDeleted != null) recordDeleted.add(record);
            record.write(transactionId, this, record.myOffset);
          }
        }
      }
      record = record.nextRecordInPage;
      previousRecord = record;
    }

    int compareResult = ValueWrapper.compareArray(previousRecord.primaryKeyValues, searchKey);
    this.bLinkTreeLatch.unlock();

    return new Pair<>(record.nextAbsoluteOffset, compareResult);
  }

  /**
   * scan the whole tree and find the page where the record with search key resides. Delete that
   * record.
   *
   * @param transactionId transaction Id
   * @param searchKey search key
   * @return if the value is deleted
   */
  public RecordLogical scanTreeAndDeleteRecordWithKey(long transactionId, ValueWrapper[] searchKey)
      throws Exception {

    if (this.infimumRecord.nextRecordInPage.recordType == RecordInPage.SYSTEM_SUPREME_RECORD) {
      firstSplitLock.lock();
      if (this.infimumRecord.nextRecordInPage.recordType == RecordInPage.SYSTEM_SUPREME_RECORD) {
        // TODO
        IndexPage leftPage = null;
        try {
          leftPage = (IndexPage) IO.read(this.spaceId, ServerRuntime.config.indexLeftmostLeafIndex);
        } catch (Exception e) {
          e.printStackTrace();
          exit(62);
        }
        ServerRuntime.getWriteLock(transactionId, leftPage.pageReadAndWriteLatch, leftPage);
        firstSplitLock.unlock();
        return null;
      }
      firstSplitLock.unlock();
    }

    Pair<Boolean, RecordInPage> result;
    IndexPage currentPage = this;
    do {
      result = currentPage.scanInternal(transactionId, searchKey);
      if (result.right.recordType == RecordInPage.USER_POINTER_RECORD) {
        currentPage = (IndexPage) IO.read(this.spaceId, result.right.childPageId);
      } else if (result.right.recordType == RecordInPage.SYSTEM_SUPREME_RECORD) {
        currentPage = (IndexPage) IO.read(this.spaceId, result.right.nextAbsoluteOffset);
      } else break;
    } while (true);

    RecordLogical recordDeleted = null;

    do {
      ServerRuntime.getWriteLock(transactionId, currentPage.pageReadAndWriteLatch, currentPage);
      currentPage.bLinkTreeLatch.lock();

      RecordInPage record = currentPage.infimumRecord.nextRecordInPage;
      boolean notExistOrFound = false;
      while (record.recordType != RecordInPage.SYSTEM_SUPREME_RECORD) {
        if (record.recordType != RecordInPage.SYSTEM_INFIMUM_RECORD) {
          int compareResult = ValueWrapper.compareArray(record.primaryKeyValues, searchKey);
          if (compareResult >= 0) {
            if (compareResult > 0) {
              if (record.recordType == RecordInPage.USER_DATA_RECORD) {
                notExistOrFound = true;
              }
            } else {
              if (record.recordType == RecordInPage.USER_DATA_RECORD) {
                if (record.isNotDeleted()) {
                  record.setDeleted();
                  recordDeleted = new RecordLogical(record);
                  record.write(transactionId, currentPage, record.myOffset);
                  //                  System.out.println("record delete" + record);
                }
                notExistOrFound = true;
              }
            }
            break;
          }
        }
        record = record.nextRecordInPage;
      }

      currentPage.bLinkTreeLatch.unlock();
      if (notExistOrFound) break;

      if (record.recordType == RecordInPage.USER_POINTER_RECORD) {
        currentPage = (IndexPage) IO.read(this.spaceId, record.childPageId);
      } else if (record.recordType == RecordInPage.SYSTEM_SUPREME_RECORD) {
        currentPage = (IndexPage) IO.read(this.spaceId, record.nextAbsoluteOffset);
      } else break;
    } while (true);

    return recordDeleted;
  }

  /**
   * scan the whole tree and find the page where the record with search key resides. Delete all
   * records that satisfies the condition.
   *
   * @param transactionId transaction Id
   * @param searchKey search key
   * @param condition condition
   * @return left is the next page id, right is the result of {@code
   *     maxRecordInPage.compareTo(searchKey)};
   */
  public Pair<Integer, Integer> scanTreeAndDeleteFromPage(
      long transactionId,
      ValueWrapper[] searchKey,
      recordCondition condition,
      ArrayList<RecordInPage> recordsDeleted)
      throws Exception {

    Pair<Boolean, RecordInPage> result;
    IndexPage currentPage = this;
    do {
      result = currentPage.scanInternal(transactionId, searchKey);
      if (result.right.recordType == RecordInPage.USER_POINTER_RECORD) {
        currentPage = (IndexPage) IO.read(this.spaceId, result.right.childPageId);
      } else if (result.right.recordType == RecordInPage.SYSTEM_SUPREME_RECORD) {
        currentPage = (IndexPage) IO.read(this.spaceId, result.right.nextAbsoluteOffset);
      } else break;
    } while (true);

    int compareResult = 0;

    ServerRuntime.getWriteLock(transactionId, currentPage.pageReadAndWriteLatch, currentPage);
    currentPage.bLinkTreeLatch.lock();

    RecordInPage record = currentPage.infimumRecord;
    RecordInPage previousRecord = null;
    while (record.recordType != RecordInPage.SYSTEM_SUPREME_RECORD) {
      if (record.recordType != RecordInPage.SYSTEM_INFIMUM_RECORD) {

        if (condition.isSatisfied(record)) {
          if (record.isNotDeleted()) {
            record.setDeleted();
            if (recordsDeleted != null) recordsDeleted.add(record);
            record.write(transactionId, this, record.myOffset);
            //            System.out.println("record delete" + record);
          } else {
            System.out.println("record is already deleted: " + record);
          }
        }
      }
      previousRecord = record;
      record = record.nextRecordInPage;
    }

    if (previousRecord != null)
      compareResult = ValueWrapper.compareArray(previousRecord.primaryKeyValues, searchKey);

    currentPage.bLinkTreeLatch.unlock();

    return new Pair<>(record.nextAbsoluteOffset, compareResult);
  }

  /**
   * delete all records in the pages.
   *
   * @param transactionId transaction Id
   * @return pair left is the next page id, pair right is the result of {@code
   *     maxRecordInPage.compareTo(searchKey)}
   */
  public int deleteAll(long transactionId, ArrayList<RecordInPage> recordDeleted) {

    ServerRuntime.getWriteLock(transactionId, this.pageReadAndWriteLatch, this);
    bLinkTreeLatch.lock();

    RecordInPage record = infimumRecord;
    while (record.recordType != RecordInPage.SYSTEM_SUPREME_RECORD) {
      if (record.recordType != RecordInPage.SYSTEM_INFIMUM_RECORD) {
        if (record.isNotDeleted()) {
          record.setDeleted();
          record.write(transactionId, this, record.myOffset);
        }
      }
      record = record.nextRecordInPage;
    }

    bLinkTreeLatch.unlock();

    return record.nextAbsoluteOffset;
  }
}
