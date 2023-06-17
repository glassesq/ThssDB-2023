package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Table;

import java.util.ArrayList;
import java.util.HashMap;

public class InsertPlan extends LogicalPlan {

  public boolean broken = false;

  private int valueSize;
  private int columnSize;

  public String tableName;
  public ArrayList<String> columnNamesToBeInserted = null;
  public ArrayList<ArrayList<String>> values;

  public InsertPlan(String tableName, boolean broken) {
    super(LogicalPlanType.INSERT);
    this.tableName = tableName;
    this.broken = broken;
  }

  public InsertPlan(
      String tableName, ArrayList<String> columnName, ArrayList<ArrayList<String>> values) {
    super(LogicalPlanType.INSERT);
    this.tableName = tableName;
    this.values = values;
    this.columnNamesToBeInserted = columnName;
    this.columnSize = this.columnNamesToBeInserted.size();
    this.valueSize = -1;
    for (ArrayList<String> valueToBeInserted : values) {
      if (valueSize == -1) {
        valueSize = valueToBeInserted.size();
      } else if (valueSize != valueToBeInserted.size()) {
        this.broken = true;
      }
    }
    if (this.valueSize != this.columnSize && this.columnSize != 0) this.broken = true;
  }

  /**
   * get values to be inserted in the order of {@code metadata.columns}
   *
   * @param metadata table metadata
   * @return values to be inserted in the order of (primaryKeys, nonPrimaryKeys).
   */
  public ArrayList<ArrayList<String>> getValues(Table.TableMetadata metadata) throws Exception {
    ArrayList<ArrayList<String>> results = new ArrayList<>();

    if (this.valueSize > metadata.getColumnNumber()) {
      throw new Exception(
          "The number of values " + this.valueSize + " is larger than that of columns.");
    }

    HashMap<Integer, Integer> columnPrimaryFields = new HashMap<>();
    if (this.columnSize == 0) {
      for (int i = 0; i < this.valueSize; i++) {
        columnPrimaryFields.put(metadata.getPrimaryFieldByCreatingOrder(i), i);
      }
    } else {
      for (int i = 0; i < this.columnSize; i++) {
        Integer primary = metadata.getPrimaryFieldByName(columnNamesToBeInserted.get(i));
        if (primary == null)
          throw new Exception(
              "inserted column name " + columnNamesToBeInserted.get(i) + " not exists");
        if (columnPrimaryFields.containsKey(primary))
          throw new Exception(
              "inserted column name " + columnNamesToBeInserted.get(i) + " duplicates.");
        columnPrimaryFields.put(primary, i);
      }
    }

    for (ArrayList<String> valueToBeInserted : values) {
      int primaryKeyNumber = metadata.getPrimaryKeyNumber();
      int nonPrimaryKeyNumber = metadata.getNonPrimaryKeyNumber();

      ArrayList<String> result = new ArrayList<>();

      for (int i = 0; i < primaryKeyNumber; i++) {
        Integer index = columnPrimaryFields.get(i);
        String value = "null";
        if (index != null) value = valueToBeInserted.get(index);

        Column column = metadata.getColumnDetailByOrderInType(i, true);
        if (column.isNotNull() && value.equals("null")) {
          throw new Exception("column " + column.getName() + " can not be null but value is null");
        }

        result.add(value);
      }

      for (int i = -1; i >= -nonPrimaryKeyNumber; i--) {
        Integer index = columnPrimaryFields.get(i);
        String value = "null";
        if (index != null) value = valueToBeInserted.get(index);

        Column column = metadata.getColumnDetailByPrimaryField(i);
        if (column.isNotNull() && value.equals("null")) {
          throw new Exception("column " + column.getName() + " can not be null but value is null");
        }

        result.add(value);
      }

      results.add(result);
    }
    return results;
  }

  @Override
  public String toString() {
    return "InsertPlan{" + "tableName='" + tableName + "'}";
  }
}
