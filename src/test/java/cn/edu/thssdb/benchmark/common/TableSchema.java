package cn.edu.thssdb.benchmark.common;

import java.util.List;

public class TableSchema {

  public String tableName;
  public List<String> columns;
  public List<DataType> types;
  public List<Boolean> notNull;
  public int primaryKeyColumnIndex;

  public TableSchema(
      String tableName,
      List<String> columns,
      List<DataType> types,
      List<Boolean> notNull,
      int primaryKeyColumnIndex) {
    this.tableName = tableName;
    this.columns = columns;
    this.types = types;
    this.notNull = notNull;
    this.primaryKeyColumnIndex = primaryKeyColumnIndex;
  }
}
