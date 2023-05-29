package cn.edu.thssdb.plan;

public abstract class LogicalPlan {

  protected LogicalPlanType type;

  public LogicalPlan(LogicalPlanType type) {
    this.type = type;
  }

  public LogicalPlanType getType() {
    return type;
  }

  public enum LogicalPlanType {
    // TODO: add more LogicalPlanType
    CREATE_DATABASE,
    USE_DATABASE,
    COMMIT,
    CREATE_TABLE,
    SHOW_TABLE,
    INSERT,
    SELECT,
    DELETE,
    UPDATE,
    DROP_DATABASE,
    BEGIN_TRANSACTION
  }
}
