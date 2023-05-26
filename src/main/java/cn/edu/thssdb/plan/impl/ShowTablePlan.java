package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Table;

public class ShowTablePlan extends LogicalPlan {

  public String tableName;
  public StringBuilder result;

  public ShowTablePlan(String tableName) {
    super(LogicalPlanType.SHOW_TABLE);
    this.tableName = tableName;
    result = new StringBuilder();
  }

  public void getValue(Table.TableMetadata table) {
    int nonPrimaryKeyNumber = table.getNonPrimaryKeyNumber();
    int primaryKeyNumber = table.getPrimaryKeyNumber();
    result.append(tableName + " (");
    for (int i = primaryKeyNumber - 1; i >= -nonPrimaryKeyNumber; --i) {
      Column col = table.getColumnDetailByPrimaryField(i);
      result.append(col.getName() + ' ' + col.type);
      if (i != -nonPrimaryKeyNumber) result.append(',');
      else result.append(')');
    }
  }

  public String toString() {
    return result.toString();
  }
}
