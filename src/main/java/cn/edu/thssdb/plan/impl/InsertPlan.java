package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.schema.Table;

import java.util.ArrayList;
import java.util.HashMap;

public class InsertPlan extends LogicalPlan {

  public boolean broken = false;

  public String tableName;
  public ArrayList<String> columnName = null;
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
    this.columnName = columnName;
  }

  /**
   * get values to be inserted in the order of {@code metadata.columns}
   *
   * @param metadata table metadata
   * @return values to be inserted in the order of {@code metadata.columns}
   */
  public ArrayList<ArrayList<String>> getValues(Table.TableMetadata metadata) {
    ArrayList<ArrayList<String>> results = new ArrayList<>();
    HashMap<Integer, Integer> setValue = new HashMap<>();
    for (int i = 0; i < columnName.size(); i++) {
      setValue.put(metadata.columns.get(columnName.get(i)), i);
    }
    for (ArrayList<String> value : values) {
      ArrayList<String> result = new ArrayList<>();
      for (int cnt = 0; cnt < metadata.columnDetails.size(); cnt++) {
        Integer index = setValue.get(cnt);
        if (index != null) result.add(value.get(index));
        else if (columnName.size() == 0) {
          if (cnt < value.size()) result.add(value.get(cnt));
          else result.add("null");
        } else result.add("null");
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
