package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;

public class UseDatabasePlan extends LogicalPlan {

  private String databaseName;

  public UseDatabasePlan(String databaseName) {
    super(LogicalPlan.LogicalPlanType.USE_DATABASE);
    this.databaseName = databaseName;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  @Override
  public String toString() {
    return "UseDatabasePlan{" + "databaseName='" + databaseName + '\'' + '}';
  }
}
