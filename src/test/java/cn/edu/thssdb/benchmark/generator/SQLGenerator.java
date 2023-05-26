package cn.edu.thssdb.benchmark.generator;

import cn.edu.thssdb.benchmark.common.PreparedStatement;
import cn.edu.thssdb.benchmark.common.TableSchema;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class SQLGenerator {
  BaseDataGenerator dataGenerator;
  List<TableSchema> tableSchemaList;
  int tableNum;
  Random random;
  ConcurrentMap<String, AtomicInteger> rowNumberMap = new ConcurrentHashMap<>();

  public SQLGenerator(BaseDataGenerator dataGenerator, int dataSeed) {
    this.random = new Random(dataSeed);
    this.dataGenerator = dataGenerator;
    this.tableSchemaList = new ArrayList<>(dataGenerator.getSchemaMap().values());
    this.tableNum = tableSchemaList.size();
    for (TableSchema tableSchema : tableSchemaList) {
      rowNumberMap.put(tableSchema.tableName, new AtomicInteger(1));
    }
  }

  public String generateInsertSQL() {
    TableSchema tableSchema = tableSchemaList.get(random.nextInt(tableSchemaList.size()));
    String tableName = tableSchema.tableName;
    int rowNumber = rowNumberMap.get(tableName).getAndIncrement();
    List<String> columns = new ArrayList<>();
    List<Object> data = new ArrayList<>();
    for (int columnId = 0; columnId < tableSchema.columns.size(); columnId++) {
      Object dataItem = dataGenerator.generateValue(tableName, rowNumber, columnId);
      if (dataItem != null) {
        columns.add(tableSchema.columns.get(columnId));
        data.add(dataItem);
      }
    }
    String insertSQL =
        "insert into "
            + tableName
            + "("
            + StringUtils.join(columns, ",")
            + ") values ("
            + StringUtils.join(data, ",")
            + ");";
    System.out.println(insertSQL);
    return insertSQL;
  }

  public String generateUpdateSQL() {
    TableSchema tableSchema = tableSchemaList.get(random.nextInt(tableSchemaList.size()));
    PreparedStatement updateStatement = new PreparedStatement("update ? set ? = ? where id = ?");
    int rowNumber = rowNumberMap.get(tableSchema.tableName).getAndIncrement();
    int columnId = random.nextInt(tableSchema.columns.size() - 1) + 1;
    String column = tableSchema.columns.get(columnId);
    Object dataItem = dataGenerator.generateValue(tableSchema.tableName, 0, columnId);
    updateStatement.setString(0, tableSchema.tableName);
    updateStatement.setString(1, column);
    updateStatement.setString(2, dataItem.toString());
    updateStatement.setString(3, String.valueOf(random.nextInt(rowNumber)));
    System.out.println(updateStatement.getSQL());
    return updateStatement.getSQL();
  }

  public String generateDeleteSQL() {
    TableSchema tableSchema = tableSchemaList.get(random.nextInt(tableSchemaList.size()));
    PreparedStatement deleteStatement = new PreparedStatement("delete from ? where id = ?");
    int rowNumber = rowNumberMap.get(tableSchema.tableName).getAndIncrement();
    deleteStatement.setString(0, tableSchema.tableName);
    deleteStatement.setString(1, String.valueOf(random.nextInt(rowNumber)));
    System.out.println(deleteStatement.getSQL());
    return deleteStatement.getSQL();
  }

  public String generateQuerySQL() {
    TableSchema tableSchema = tableSchemaList.get(random.nextInt(tableSchemaList.size()));
    PreparedStatement queryStatement = new PreparedStatement("select ? from ? where id = ?");
    String column = tableSchema.columns.get(random.nextInt(tableSchema.columns.size()));

    queryStatement.setString(0, column);
    queryStatement.setString(1, tableSchema.tableName);
    int rowNumber = rowNumberMap.get(tableSchema.tableName).getAndIncrement();
    queryStatement.setString(2, String.valueOf(random.nextInt(rowNumber)));
    System.out.println(queryStatement.getSQL());
    return queryStatement.getSQL();
  }

  public String generateJoinSQL() {
    TableSchema tableSchema1 = tableSchemaList.get(random.nextInt(tableSchemaList.size()));
    TableSchema tableSchema2 = tableSchemaList.get(random.nextInt(tableSchemaList.size()));
    while (Objects.equals(tableSchema1.tableName, tableSchema2.tableName)) {
      tableSchema2 = tableSchemaList.get(random.nextInt(tableSchemaList.size()));
    }

    PreparedStatement joinStatement =
        new PreparedStatement("select ? from ? join ? on ? = ? where ? = ?");
    String column = tableSchema1.columns.get(random.nextInt(tableSchema1.columns.size()));
    int rowNumber = rowNumberMap.get(tableSchema1.tableName).getAndIncrement();
    joinStatement.setString(0, tableSchema1.tableName + "." + column);
    joinStatement.setString(1, tableSchema1.tableName);
    joinStatement.setString(2, tableSchema2.tableName);
    joinStatement.setString(3, tableSchema1.tableName + ".id");
    joinStatement.setString(4, tableSchema2.tableName + ".id");
    joinStatement.setString(5, tableSchema1.tableName + ".id");
    joinStatement.setString(6, String.valueOf(random.nextInt(rowNumber)));
    System.out.println(joinStatement.getSQL());
    return joinStatement.getSQL();
  }
}
