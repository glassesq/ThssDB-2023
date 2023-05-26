package cn.edu.thssdb.benchmark.transaction;

public enum OperationType {
  INSERT("Insert"),
  UPDATE("Update"),
  DELETE("Delete"),
  QUERY("Query"),
  JOIN("Join");

  private final String type;

  OperationType(String type) {
    this.type = type;
  }

  public String getType() {
    return type;
  }
}
