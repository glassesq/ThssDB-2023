package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.schema.Table;

public class CreateTablePlan extends LogicalPlan {

  public boolean broken = false;
  public Table.TableMetadata tableMetadata;

  public CreateTablePlan(boolean broken) {
    super(LogicalPlanType.CREATE_TABLE);
    this.broken = broken;
  }

  public CreateTablePlan(Table.TableMetadata metadata) {
    super(LogicalPlanType.CREATE_TABLE);
    this.tableMetadata = metadata;
  }

  @Override
  public String toString() {
    return "CreateTablePlan{}";
  }
}
