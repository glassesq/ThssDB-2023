package cn.edu.thssdb.benchmark.generator;

import cn.edu.thssdb.benchmark.common.Constants;
import cn.edu.thssdb.benchmark.common.DataType;
import cn.edu.thssdb.benchmark.common.TableSchema;

import java.util.ArrayList;
import java.util.List;

public class SimpleDataGenerator extends BaseDataGenerator {
  @Override
  protected void initTableSchema() {
    for (int tableId = 0; tableId < Constants.tableCount; tableId++) {
      String tableName = "test_table" + tableId;
      List<String> columns = new ArrayList<>();
      List<DataType> types = new ArrayList<>();
      List<Boolean> notNull = new ArrayList<>();
      for (int columnId = 0; columnId < Constants.columnCount; columnId++) {
        columns.add("column" + columnId);
        types.add(Constants.columnTypes[columnId % Constants.columnTypes.length]);
        // TODO: primary key column should be not null
        notNull.add(false);
      }
      schemaMap.put(tableName, new TableSchema(tableName, columns, types, notNull, tableId));
    }
  }

  @Override
  public Object generateValue(String tableName, int rowId, int columnId) {
    switch (schemaMap.get(tableName).types.get(columnId)) {
      case INT:
        return rowId;
      case LONG:
        return (long) rowId;
      case DOUBLE:
        return (double) rowId;
      case FLOAT:
        return (float) rowId;
      case STRING:
        return String.format(stringFormat, rowId);
    }
    return null;
  }
}
