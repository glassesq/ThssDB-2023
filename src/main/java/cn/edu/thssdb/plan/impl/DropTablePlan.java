package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;

public class DropTablePlan extends LogicalPlan {

  public boolean broken = false;
  public String tableName;

  public DropTablePlan(boolean broken) {
    super(LogicalPlanType.DROP_TABLE);
    this.broken = broken;
  }

  public DropTablePlan(String tableName) {
    super(LogicalPlanType.DROP_TABLE);
    this.tableName = tableName;
  }

  @Override
  public String toString() {
    return "DropTableStmt{}";
  }
}
