package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;

public class BeginTransactionPlan extends LogicalPlan {

  public BeginTransactionPlan() {
    super(LogicalPlanType.BEGIN_TRANSACTION);
  }

  @Override
  public String toString() {
    return "BeginTransaction{}";
  }
}
