package cn.edu.thssdb.schema;

import cn.edu.thssdb.communication.IO;
import cn.edu.thssdb.runtime.ServerRuntime;
import cn.edu.thssdb.storage.DiskBuffer;
import cn.edu.thssdb.storage.page.IndexPage;
import cn.edu.thssdb.storage.page.OverallPage;
import cn.edu.thssdb.utils.Pair;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Table class. The lifetime of this {@code Table} class object shall be only within the transaction
 * that requested the Table. Multiple transactions may run on same Table. They obtain different
 * Table Object which share the same TableMetadata. The shared TableMetadata Object is under the
 * control of ServerRuntime.
 */
public class Table {

  public static class TableMetadata {
    // TODO: Lock is allocated at the unit of transaction.

    /** spaceId is a positive integer. */
    public int spaceId;

    public String name;
    public String tablespaceFilename;

    /** if the table is temporary (e.g. may be created for join operation. ) */
    public boolean temporary = false;

    /** if the table is inited (The relevant tablespace file is already in disk buffer or disk.) */
    public boolean inited = false;
    /** ColumnName to column index */
    public HashMap<String, Integer> columns = new HashMap<>();
    /** column index to column object (detail) */
    public ArrayList<Column> columnDetails = new ArrayList<>();
    /** Json object of overall table metadata */
    public JSONObject object;
    /** Json arrays of column object */
    public JSONArray columnObjectArray;

    public int getPrimaryKeyNumber() {
      // TODO: optimization
      int count = 0;
      for (Column columnDetail : columnDetails) {
        if (columnDetail.primary >= 0) {
          count++;
        }
      }
      return count;
    }

    public int getNonPrimaryKeyNumber() {
      // TODO: optimization
      int count = 0;
      for (Column columnDetail : columnDetails) {
        if (columnDetail.primary < 0) {
          count++;
        }
      }
      return count;
    }

    public int getPrimaryKeyLength() {
      // TODO: optimization
      int count = 0;
      for (Column columnDetail : columnDetails) {
        if (columnDetail.primary >= 0) {
          count += columnDetail.getLength();
        }
      }
      return count;
    }

    public int getNonPrimaryKeyLength() {
      // TODO: optimization
      int count = 0;
      for (Column columnDetail : columnDetails) {
        if (columnDetail.primary < 0) {
          count += columnDetail.getLength();
        }
      }
      return count;
    }

    /**
     * get the length of nullBitmap for this table
     *
     * @return number of bytes
     */
    public int getNullBitmapLengthInByte() {
      // TODO: optimization
      int count = 0;
      for (Column columnDetail : columnDetails) {
        if (!columnDetail.notNull) {
          count++;
        }
      }

      /* ceil */
      return ((count + 7) / 8);
    }

    /**
     * get the length or record in page
     *
     * @param type record type
     * @return max length
     */
    public int getMaxRecordLength(int type) {
      switch (type) {
        case IndexPage.RecordInPage.USER_POINTER_RECORD:
          return 4 + 4 + getPrimaryKeyLength() + getNullBitmapLengthInByte();
        case IndexPage.RecordInPage.USER_DATA_RECORD:
        default:
          return 4
              + 15
              + getPrimaryKeyLength()
              + getNonPrimaryKeyLength()
              + getNullBitmapLengthInByte();
      }
    }

    /**
     * get offset of each primaryKey TODO: optimization
     *
     * @return ArrayList<Integer> the offset of {@code ith} primaryKey
     */
    public ArrayList<Integer> getPrimaryOffsetInOrder() {
      HashMap<Integer, Column> primaryKeyColumn = new HashMap<>();
      ArrayList<Integer> offsetList = new ArrayList<>();
      for (Column column : columnDetails) {
        if (column.primary >= 0) {
          primaryKeyColumn.put(column.primary, column);
        }
      }
      int pOffset = 0;
      for (int i = 0; i < primaryKeyColumn.size(); i++) {
        Column column = primaryKeyColumn.get(i);
        offsetList.add(pOffset);
        pOffset += column.getLength();
      }
      return offsetList;
    }

    /**
     * Prepare a table metadata object with name, spaceId, tablespaceFilename. This table currently
     * does not exist on disk. Therefore, {@code inited} is set to false. Proper tablespace filename
     * will be allocated.
     *
     * @param name table name
     * @param spaceId tablespaceId
     */
    public void prepare(String name, int spaceId) {
      this.object = new JSONObject();
      this.columnObjectArray = new JSONArray();
      this.object.put("columns", this.columnObjectArray);
      this.name = name;
      this.object.put("tableName", name);
      this.spaceId = spaceId;
      this.object.put("tablespaceId", spaceId);
      this.tablespaceFilename = ServerRuntime.getTablespaceFile(spaceId);
      this.object.put("tablespaceFile", tablespaceFilename);
      temporary = false; // TODO: temporary table.
      inited = false;
    }

    public static TableMetadata parse(JSONObject object) throws Exception {
      TableMetadata metadata = new TableMetadata();
      metadata.object = object;
      metadata.name = object.getString("tableName");
      metadata.spaceId = object.getInt("tablespaceId");
      metadata.tablespaceFilename = object.getString("tablespaceFile");

      metadata.columnObjectArray = object.getJSONArray("columns");
      for (int i = 0; i < metadata.columnObjectArray.length(); i++) {
        Column column = Column.parse(metadata.columnObjectArray.getJSONObject(i));
        String name = metadata.columnObjectArray.getJSONObject(i).getString("columnName");
        metadata.columns.put(name, i);
        metadata.columnDetails.add(column);
      }

      /* since the tableMetadata is formed according to an existed json object, it must be on disk. */
      metadata.temporary = false;
      metadata.inited = true;
      return metadata;
    }

    /**
     * init this tablespace on disk (buffer). Set up file format and necessary information. Both
     * Metadata and Tablespace Data. A suitable tablespaceId will be allocated automatically.
     *
     * @throws Exception init failed.
     */
    public void initTablespaceFile(long transactionId) throws Exception {
      /* Tablespace File Creation */
      String tablespaceFilename = ServerRuntime.getTablespaceFile(spaceId);
      System.out.println(tablespaceFilename);

      File tablespaceFile = new File(tablespaceFilename);
      // TODO: delete file if already existed.
      tablespaceFile.createNewFile();
      if (!tablespaceFile.exists()) {
        throw new Exception("create tablespace file failed.");
      }

      OverallPage.createOverallPage(transactionId, spaceId, 0);
      System.out.println("overall page over.");
      IndexPage indexPage = IndexPage.createIndexPage(transactionId, spaceId, 2);
      System.out.println("index root page over.");
    }

    public void addColumn(String name, Column column) {
      int size = columnDetails.size();
      this.columns.put(name, size);
      this.columnDetails.add(column);
      columnObjectArray.put(column.object);
    }

    /**
     * insert values into this table
     *
     * @param transactionId transaction
     * @param values value (in the format of ('string', 1234, null))
     * @throws Exception the primary key is existed.
     */
    public void insertRecord(long transactionId, ArrayList<String> values) throws Exception {
      RecordLogical recordToBeInserted = new RecordLogical(this);
      int index = 0;
      int npIndex = 0;
      for (Column column : columnDetails) {
        ValueWrapper valueWrapper = new ValueWrapper(column);
        valueWrapper.setWithNull(values.get(index));
        ++index;
        if (column.primary >= 0) {
          recordToBeInserted.primaryKeyValues[column.primary] = valueWrapper;
        } else {
          recordToBeInserted.nonPrimaryKeyValues[npIndex] = valueWrapper;
          ++npIndex;
        }
      }

      // TODO: validation (check for constraint)

      /* B-LINK TREE */
      IndexPage rootPage =
          (IndexPage) IO.read(this.spaceId, ServerRuntime.config.indexRootPageIndex);

      boolean result = rootPage.insertDataRecordIntoTree(transactionId, recordToBeInserted);
      if (!result) {
        throw new Exception("The primary key value is already existed.");
      }

      Pair<Integer, ArrayList<RecordLogical>> records = rootPage.getAllRecordLogical(transactionId);
      System.out.println("******************* values currently in rootPage:");
      for (RecordLogical recordLogical : records.right) {
        System.out.println(recordLogical);
      }
      System.out.println("**************************************************");
    }
  }

  public TableMetadata metadata;

  public Table(TableMetadata tableMetadata, boolean isolated) {
    if (isolated) {
      /* This table is only temporary. */
      // TODO: deep copy of tableMetadata
    } else {
      this.metadata = tableMetadata;
    }
  }

  private void recover() {
    // TODO
  }

  public void delete() {
    // TODO
  }

  public void update() {
    // TODO
  }

  private void serialize() {
    // TODO
  }

  private ArrayList<RecordLogical> deserialize() {
    // TODO
    return null;
  }
}
