package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
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
    if (this.valueSize != this.columnSize) this.broken = true;
  }

  /**
   * get values to be inserted in the order of {@code metadata.columns}
   *
   * @param metadata table metadata
   * @return values to be inserted in the order of {@code metadata.columns}
   */
  public ArrayList<ArrayList<String>> getValues(Table.TableMetadata metadata) {
    ArrayList<ArrayList<String>> results = new ArrayList<>();

    HashMap<Integer, Integer> columnPrimaryFields = new HashMap<>();
    if (this.columnSize == 0) {
      for (int i = 0; i < this.valueSize; i++) {
        columnPrimaryFields.put(metadata.getPrimaryFieldByCreatingOrder(i), i);
      }
    } else {
      for (int i = 0; i < this.columnSize; i++) {
        columnPrimaryFields.put(
            metadata.getColumnDetailByName(columnNamesToBeInserted.get(i)).primary, i);
      }
    }

    for (ArrayList<String> valueToBeInserted : values) {
      int primaryKeyNumber = metadata.getPrimaryKeyNumber();
      int nonPrimaryKeyNumber = metadata.getNonPrimaryKeyNumber();

      ArrayList<String> result = new ArrayList<>();

      for (int i = 0; i < primaryKeyNumber; i++) {
        Integer index = columnPrimaryFields.get(i);
        if (index != null) result.add(valueToBeInserted.get(index));
        else result.add("null");
      }

      for (int i = -1; i >= nonPrimaryKeyNumber; i--) {
        Integer index = columnPrimaryFields.get(i);
        if (index != null) result.add(valueToBeInserted.get(index));
        else result.add("null");
      }

      // TODO: check for NOT NULL constraint

      results.add(result);
    }
    return results;
  }

  @Override
  public String toString() {
    return "InsertPlan{" + "tableName='" + tableName + "'}";
  }
}
