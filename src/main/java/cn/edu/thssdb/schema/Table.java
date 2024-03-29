package cn.edu.thssdb.schema;

import cn.edu.thssdb.communication.IO;
import cn.edu.thssdb.runtime.ServerRuntime;
import cn.edu.thssdb.storage.page.IndexPage;
import cn.edu.thssdb.storage.page.OverallPage;
import cn.edu.thssdb.type.DataType;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

import static java.lang.System.exit;

/**
 * Table class. The lifetime of this {@code Table} class object shall be only within the transaction
 * that requested the Table. Multiple transactions may run on same Table. They obtain different
 * Table Object which share the same TableMetadata. The shared TableMetadata Object is under the
 * control of ServerRuntime.
 */
public class Table {

  public static class TableMetadata {

    /** spaceId is a positive integer. */
    public int spaceId;

    public String name;
    public String tablespaceFilename;

    /** column name to column primary field */
    private HashMap<String, Integer> columnNames = new HashMap<>();

    /** <b>Aligned</b> column primary field to column detail
     *  {@code [0, nonPrimaryKeyNumber)} stores column whose primary field ranges from -nonPrimaryKeyNumber to -1.
     *  {@code [nonPrimaryKeyNumber, primaryKeyNumber + nonPrimaryKeyNumber) stores column whose primary field ranges from 0 to primaryKeyNumber - 1.
     */
    private ArrayList<Column> columnDetails = new ArrayList<>();

    /** column creating order (in create table statement) to column primary field */
    private ArrayList<Integer> columnCreatingOrder = new ArrayList<>();

    /** Json object of overall table metadata */
    public JSONObject object;

    /** Json arrays of column object * <b> saved in the creating order </b> */
    public JSONArray columnObjectArray;

    private int primaryKeyNumber = 0;
    private int primaryKeyLength = 0;
    private final ArrayList<Integer> primaryKeyOffset = new ArrayList<>();

    private int nonPrimaryKeyNumber = 0;
    private int nonPrimaryKeyLength = 0;
    private final ArrayList<Integer> nonPrimaryKeyOffset = new ArrayList<>();

    private int nullableKeyNumber = 0;

    public int getColumnNumber() {
      return nonPrimaryKeyNumber + primaryKeyNumber;
    }

    public Integer getPrimaryFieldByName(String name) {
      return columnNames.get(name);
    }

    public Integer getPrimaryFieldByCreatingOrder(int creatingOrder) {
      return columnCreatingOrder.get(creatingOrder);
    }

    public Column getColumnDetailByOrderInType(int order, boolean primaryField) {
      if (primaryField) return getColumnDetailByPrimaryField(order);
      else return getColumnDetailByPrimaryField(-1 - order);
    }

    public Column getColumnDetailByPrimaryField(int primaryField) {
      return columnDetails.get(nonPrimaryKeyNumber + primaryField);
    }

    public Column getColumnDetailByName(String name) {
      return getColumnDetailByPrimaryField(columnNames.get(name));
    }

    public void setColumnsAndCompute(
        ArrayList<String> _names,
        ArrayList<Column> _columns,
        ArrayList<Integer> _columnOrder,
        int primaryKeyNumber,
        int nonPrimaryKeyNumber) {
      this.primaryKeyNumber = primaryKeyNumber;
      this.nonPrimaryKeyNumber = nonPrimaryKeyNumber;
      this.nullableKeyNumber = nonPrimaryKeyNumber;
      for (int i = 0; i < primaryKeyNumber + nonPrimaryKeyNumber; i++) {
        columnDetails.add(null);
      }
      for (int i = 0; i < _names.size(); i++) {
        Column column = _columns.get(i);
        columnDetails.set(nonPrimaryKeyNumber + column.primary, column);
        columnNames.put(_names.get(i), column.primary);
      }
      this.primaryKeyLength = 0;
      for (int i = 0; i < primaryKeyNumber; i++) {
        primaryKeyOffset.add(primaryKeyLength);
        primaryKeyLength += getColumnDetailByPrimaryField(i).getLength();
      }
      this.nonPrimaryKeyLength = 0;
      for (int i = -1; i >= -nonPrimaryKeyNumber; i--) {
        nonPrimaryKeyOffset.add(nonPrimaryKeyLength);
        nonPrimaryKeyLength += getColumnDetailByPrimaryField(i).getLength();
      }
      this.columnCreatingOrder = _columnOrder;
      for (Integer index : this.columnCreatingOrder) {
        columnObjectArray.put(columnDetails.get(nonPrimaryKeyNumber + index).object);
      }
    }

    public int getPrimaryKeyNumber() {
      return primaryKeyNumber;
    }

    public int getNonPrimaryKeyNumber() {
      return nonPrimaryKeyNumber;
    }

    public int getPrimaryKeyLength() {
      return primaryKeyLength;
    }

    /**
     * get primary key name list
     *
     * @return name list of primary keys
     */
    public ArrayList<String> getPrimaryKeyList() {
      ArrayList<String> keys = new ArrayList<>();
      for (int i = 0; i < this.primaryKeyNumber; i++) {
        keys.add(getColumnDetailByOrderInType(i, true).getName());
      }
      return keys;
    }

    public int getNonPrimaryKeyLength() {
      return nonPrimaryKeyLength;
    }

    /**
     * get the length of nullBitmap for this table
     *
     * @return number of bytes
     */
    public int getNullBitmapLengthInByte() {
      return ((nullableKeyNumber + 7) / 8); /* ceiling to full byte */
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
          return 4 + getPrimaryKeyLength() + getNonPrimaryKeyLength() + getNullBitmapLengthInByte();
      }
    }

    /**
     * get offset of each primaryKey (0, 1, ...., n)
     *
     * @return ArrayList<Integer> the offset of {@code ith} primaryKey
     */
    public ArrayList<Integer> getPrimaryOffsetInOrder() {
      return primaryKeyOffset;
    }

    /**
     * get offset of non primary key (-1, ...., -m)
     *
     * @return ArrayList<Integer> the offset of {@code ith} nonPrimaryKey
     */
    public ArrayList<Integer> getNonPrimaryKeyOffsetInOrder() {
      return nonPrimaryKeyOffset;
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
    }

    public static TableMetadata parse(JSONObject object) throws Exception {
      TableMetadata metadata = new TableMetadata();
      metadata.object = object;
      metadata.name = object.getString("tableName");
      metadata.spaceId = object.getInt("tablespaceId");
      metadata.tablespaceFilename = object.getString("tablespaceFile");

      metadata.columnObjectArray = object.getJSONArray("columns");
      metadata.primaryKeyNumber = 0;
      metadata.nonPrimaryKeyNumber = 0;
      for (int i = 0; i < metadata.columnObjectArray.length(); i++) {
        int primary = metadata.columnObjectArray.getJSONObject(i).getInt("primaryKey");
        if (primary >= 0) metadata.primaryKeyNumber += 1;
        else metadata.nonPrimaryKeyNumber += 1;
        metadata.columnCreatingOrder.add(primary);
      }
      metadata.nullableKeyNumber = metadata.nonPrimaryKeyNumber;

      for (int i = 0; i < metadata.primaryKeyNumber + metadata.nonPrimaryKeyNumber; i++) {
        metadata.columnDetails.add(null);
      }

      for (int i = 0; i < metadata.columnObjectArray.length(); i++) {
        Column column = Column.parse(metadata.columnObjectArray.getJSONObject(i));
        if (column == null) exit(2);
        String name = metadata.columnObjectArray.getJSONObject(i).getString("columnName");

        metadata.columnNames.put(name, column.primary);
        metadata.columnDetails.set(column.primary + metadata.nonPrimaryKeyNumber, column);
      }

      metadata.primaryKeyLength = 0;
      for (int i = 0; i < metadata.primaryKeyNumber; i++) {
        metadata.primaryKeyOffset.add(metadata.primaryKeyLength);
        metadata.primaryKeyLength += metadata.getColumnDetailByPrimaryField(i).getLength();
      }

      metadata.nonPrimaryKeyLength = 0;
      for (int i = -1; i >= -metadata.nonPrimaryKeyNumber; i--) {
        metadata.nonPrimaryKeyOffset.add(metadata.nonPrimaryKeyLength);
        metadata.nonPrimaryKeyLength += metadata.getColumnDetailByPrimaryField(i).getLength();
      }

      /* since the tableMetadata is formed according to an existed json object, it must be on disk. */
      return metadata;
    }

    /**
     * init this tablespace on disk (buffer). Set up file format and necessary information. Both
     * Metadata and Tablespace Data. A suitable tablespaceId will be allocated automatically.
     */
    public void initTablespaceFile(long transactionId) {
      /* Tablespace File Creation */
      String tablespaceFilename = ServerRuntime.getTablespaceFile(spaceId);
      //      System.out.println(tablespaceFilename);

      File tablespaceFile = new File(tablespaceFilename);
      if (tablespaceFile.exists()) {
        tablespaceFile.delete();
      }
      try {
        tablespaceFile.createNewFile();
        if (!tablespaceFile.exists()) {
          throw new Exception("create tablespace file failed.");
        }
      } catch (Exception e) {
        e.printStackTrace();
        exit(64);
      }

      OverallPage.createOverallPage(transactionId, spaceId, ServerRuntime.config.overallPageIndex);
      IndexPage.createIndexPage(transactionId, spaceId, ServerRuntime.config.indexRootPageIndex);
      IndexPage.createIndexPage(
          transactionId, spaceId, ServerRuntime.config.indexLeftmostLeafIndex);
    }

    /**
     * insert values into this table
     *
     * @param transactionId transaction
     * @param values value (in the format of ('string', 1234, null)), in the order of (primaryKeys,
     *     nonPrimaryKeys).
     */
    public boolean insertRecord(long transactionId, ArrayList<String> values) {
      RecordLogical recordToBeInserted = new RecordLogical(this);

      int primaryKeyNumber = getPrimaryKeyNumber();
      int nonPrimaryKeyNumber = getNonPrimaryKeyNumber();

      for (int i = 0; i < primaryKeyNumber; i++) {
        Column column = getColumnDetailByOrderInType(i, true);
        if (values == null) {
          ValueWrapper valueWrapper = new ValueWrapper(column);
          if (column.type != DataType.STRING) valueWrapper.setWithNull("0");
          else valueWrapper.setWithNull("''");
          recordToBeInserted.primaryKeyValues[i] = valueWrapper;
        } else {
          if (values.get(i).equals("null") && column.isNotNull()) return false;
          ValueWrapper valueWrapper = new ValueWrapper(column);
          valueWrapper.setWithNull(values.get(i));
          recordToBeInserted.primaryKeyValues[i] = valueWrapper;
        }
      }

      for (int i = 0; i < nonPrimaryKeyNumber; i++) {
        Column column = getColumnDetailByOrderInType(i, false);
        if (values == null) {
          ValueWrapper valueWrapper = new ValueWrapper(column);
          valueWrapper.setWithNull("null");
          recordToBeInserted.nonPrimaryKeyValues[i] = valueWrapper;
        } else {
          if (values.get(primaryKeyNumber + i).equals("null") && column.isNotNull()) return false;
          ValueWrapper valueWrapper = new ValueWrapper(column);
          valueWrapper.setWithNull(values.get(primaryKeyNumber + i));
          recordToBeInserted.nonPrimaryKeyValues[i] = valueWrapper;
        }
      }

      IndexPage rootPage;
      try {
        rootPage = (IndexPage) IO.read(this.spaceId, ServerRuntime.config.indexRootPageIndex);
        return rootPage.insertDataRecordIntoTree(transactionId, recordToBeInserted);
      } catch (Exception e) {
        e.printStackTrace();
        exit(65);
      }
      return false;
    }
  }
}
