package cn.edu.thssdb.benchmark.common;

public enum DataType {
  INT("Int"),
  LONG("Long"),
  FLOAT("Float"),
  DOUBLE("Double"),
  STRING("String(5)");

  private final String type;

  DataType(String type) {
    this.type = type;
  }

  public String getType() {
    return type;
  }
}
