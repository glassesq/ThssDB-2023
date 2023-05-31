package cn.edu.thssdb.benchmark.executor;

import cn.edu.thssdb.benchmark.common.Client;
import cn.edu.thssdb.benchmark.common.Constants;
import cn.edu.thssdb.benchmark.common.DataType;
import cn.edu.thssdb.benchmark.common.TableSchema;
import cn.edu.thssdb.benchmark.generator.BaseDataGenerator;
import cn.edu.thssdb.benchmark.generator.SimpleDataGenerator;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class CRUDTestExecutor extends TestExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(CRUDTestExecutor.class);

  private BaseDataGenerator dataGenerator;
  private Map<String, TableSchema> schemaMap;
  private Map<String, Set<List<Object>>> dataMap; // index by primary key
  private Client client;

  public CRUDTestExecutor() throws TException {
    dataGenerator = new SimpleDataGenerator();
    schemaMap = dataGenerator.getSchemaMap();
    dataMap = new HashMap<>();
    client = new Client();
  }

  public void createAndUseDB() throws TException {
    client.executeStatement("drop database db1;");
    // make sure database not exist, it's ok to ignore the error
    ExecuteStatementResp resp = client.executeStatement("create database db1;");
    Assert.assertEquals(Constants.SUCCESS_STATUS_CODE, resp.status.code);
    LOGGER.info("Create database db1 finished");
    resp = client.executeStatement("use db1;");
    Assert.assertEquals(Constants.SUCCESS_STATUS_CODE, resp.status.code);
    LOGGER.info("Use db1 finished");
  }

  /*
   CREATE TABLE test_table1 (
   column1 int,
   column2 long PRIMARY KEY,
   column3 double,
   column4 float,
   column5 string,
   column6 int,
   column7 long,
   column8 double,
   column9 float,
   column10 string
     );
  */
  public void createTable() throws TException {
    for (TableSchema tableSchema : schemaMap.values()) {
      createTable(tableSchema, client);
    }
  }

  public void insertData() throws TException {
    for (Map.Entry<String, TableSchema> tableSchemaEntry : schemaMap.entrySet()) {
      String tableName = tableSchemaEntry.getKey();
      Set<List<Object>> tableData = new HashSet<>();
      for (int rowid = 0; rowid < Constants.rowCount; rowid++) {
        List<Object> rowData = new ArrayList<>();
        StringBuilder sb = new StringBuilder("Insert into ");
        sb.append(tableName);
        sb.append(" values(");

        for (int columnId = 0; columnId < Constants.columnCount; columnId++) {
          Object dataItem = dataGenerator.generateValue(tableName, rowid, columnId);
          rowData.add(dataItem);
          sb.append(dataItem);
          if (columnId != Constants.columnCount - 1) {
            sb.append(',');
          }
        }
        sb.append(");");
        client.executeStatement(sb.toString());
        tableData.add(rowData);
      }
      dataMap.put(tableName, tableData);
    }
  }

  public void queryData() throws TException {
    // test 1: query without filter
    String querySql = "select column0,column1 from test_table1;";
    ExecuteStatementResp resp = client.executeStatement(querySql);
    TableSchema tableSchema = dataGenerator.getTableSchema("test_table1");
    List<DataType> resultTypes = new ArrayList<>();
    resultTypes.add(tableSchema.types.get(0));
    resultTypes.add(tableSchema.types.get(1));
    Set<List<Object>> queryResult = convertData(resp.rowList, resultTypes);
    List<Integer> columnList = new ArrayList<>();
    columnList.add(0);
    columnList.add(1);
    Set<List<Object>> tableData = extractData(dataMap.get("test_table1"), columnList);

    Assert.assertTrue(equals(queryResult, tableData));

    // test2: query with filter for non primary key
    querySql = "select column1,column2 from test_table2 where column0 = 5;";
    resp = client.executeStatement(querySql);
    tableSchema = dataGenerator.getTableSchema("test_table2");
    resultTypes.clear();
    resultTypes.add(tableSchema.types.get(1));
    resultTypes.add(tableSchema.types.get(2));
    queryResult = convertData(resp.rowList, resultTypes);
    tableData = dataMap.get("test_table2");

    Set<List<Object>> expectedResult = new HashSet<>();
    for (List<Object> rowData : tableData) {
      if (Objects.equals(rowData.get(0), 5)) {
        expectedResult.add(rowData);
      }
    }
    columnList.clear();
    columnList.add(1);
    columnList.add(2);
    expectedResult = extractData(expectedResult, columnList);
    Assert.assertTrue(equals(queryResult, expectedResult));

    // test3: query with filter for primary key
    querySql = "select column0,column3 from test_table3 where column3 < 5;";
    resp = client.executeStatement(querySql);
    tableSchema = dataGenerator.getTableSchema("test_table3");
    resultTypes.clear();
    resultTypes.add(tableSchema.types.get(0));
    resultTypes.add(tableSchema.types.get(3));
    queryResult = convertData(resp.rowList, resultTypes);
    tableData = dataMap.get("test_table3");

    expectedResult = new HashSet<>();
    for (List<Object> rowData : tableData) {
      if ((int) rowData.get(0) < 5) {
        expectedResult.add(rowData);
      }
    }
    columnList.clear();
    columnList.add(0);
    columnList.add(3);
    expectedResult = extractData(expectedResult, columnList);
    Assert.assertTrue(equals(queryResult, expectedResult));

    // query join on column1
    querySql =
        "select test_table3.column1,test_table3.column2,test_table4.column2 from test_table3 join test_table4 on test_table3.column1 = test_table4.column1;";
    resp = client.executeStatement(querySql);
    tableSchema = dataGenerator.getTableSchema("test_table3");
    resultTypes.clear();
    resultTypes.add(tableSchema.types.get(1));
    resultTypes.add(tableSchema.types.get(2));
    tableSchema = dataGenerator.getTableSchema("test_table4");
    resultTypes.add(tableSchema.types.get(2));
    queryResult = convertData(resp.rowList, resultTypes);

    Set<List<Object>> leftTableData = dataMap.get("test_table3");
    Set<List<Object>> rightTableData = dataMap.get("test_table4");

    expectedResult = new HashSet<>();
    for (List<Object> leftRowData : leftTableData) {
      for (List<Object> rightRowData : rightTableData) {
        if (leftRowData.get(1).equals(rightRowData.get(1))) {
          List<Object> resultRowData = new ArrayList<>();
          resultRowData.add(leftRowData.get(1));
          resultRowData.add(leftRowData.get(2));
          resultRowData.add(rightRowData.get(2));
          expectedResult.add(resultRowData);
        }
      }
    }

    Assert.assertTrue(equals(queryResult, expectedResult));

    // query join on column1 where column1 = 5;
    querySql =
        "select test_table3.column1,test_table3.column2,test_table4.column2 from test_table3 join test_table4 on test_table3.column1 = test_table4.column1 where test_table3.column1 = 5;";
    resp = client.executeStatement(querySql);
    tableSchema = dataGenerator.getTableSchema("test_table3");
    resultTypes.clear();
    resultTypes.add(tableSchema.types.get(1));
    resultTypes.add(tableSchema.types.get(2));
    tableSchema = dataGenerator.getTableSchema("test_table4");
    resultTypes.add(tableSchema.types.get(2));
    queryResult = convertData(resp.rowList, resultTypes);

    leftTableData = dataMap.get("test_table3");
    rightTableData = dataMap.get("test_table4");

    expectedResult = new HashSet<>();
    for (List<Object> leftRowData : leftTableData) {
      for (List<Object> rightRowData : rightTableData) {
        if (leftRowData.get(1).equals(rightRowData.get(1)) && (long) leftRowData.get(1) == 5) {
          List<Object> resultRowData = new ArrayList<>();
          resultRowData.add(leftRowData.get(1));
          resultRowData.add(leftRowData.get(2));
          resultRowData.add(rightRowData.get(2));
          expectedResult.add(resultRowData);
        }
      }
    }

    Assert.assertTrue(equals(queryResult, expectedResult));
    // query join on column1 where column3 = 5;
    querySql =
        "select test_table3.column1,test_table3.column3,test_table4.column2 from test_table3 join test_table4 on test_table3.column1 = test_table4.column1 where test_table3.column3 = 5;";
    resp = client.executeStatement(querySql);
    tableSchema = dataGenerator.getTableSchema("test_table3");
    resultTypes.clear();
    resultTypes.add(tableSchema.types.get(1));
    resultTypes.add(tableSchema.types.get(3));
    tableSchema = dataGenerator.getTableSchema("test_table4");
    resultTypes.add(tableSchema.types.get(2));
    queryResult = convertData(resp.rowList, resultTypes);

    leftTableData = dataMap.get("test_table3");
    rightTableData = dataMap.get("test_table4");

    expectedResult = new HashSet<>();
    for (List<Object> leftRowData : leftTableData) {
      for (List<Object> rightRowData : rightTableData) {
        if (leftRowData.get(1).equals(rightRowData.get(1)) && (double) leftRowData.get(3) == 5) {
          List<Object> resultRowData = new ArrayList<>();
          resultRowData.add(leftRowData.get(1));
          resultRowData.add(leftRowData.get(3));
          resultRowData.add(rightRowData.get(2));
          expectedResult.add(resultRowData);
        }
      }
    }
    Assert.assertTrue(equals(queryResult, expectedResult));
  }

  public void updateAndQueryData() throws TException {
    // update column2 to 100 where column2 = 50;
    String updateSql = "update test_table2 set column2 = 100 where column2 = 50;";
    client.executeStatement(updateSql);

    Set<List<Object>> tableData = dataMap.get("test_table2");
    for (List<Object> rowData : tableData) {
      if ((float) rowData.get(2) == 50) {
        rowData.set(2, 100f);
      }
    }

    String querySql = "select column1,column2 from test_table2;";
    ExecuteStatementResp resp = client.executeStatement(querySql);
    TableSchema tableSchema = dataGenerator.getTableSchema("test_table2");
    List<DataType> resultTypes = new ArrayList<>();
    resultTypes.add(tableSchema.types.get(1));
    resultTypes.add(tableSchema.types.get(2));
    Set<List<Object>> queryResult = convertData(resp.rowList, resultTypes);
    List<Integer> columnList = new ArrayList<>();
    columnList.add(1);
    columnList.add(2);
    Set<List<Object>> expectedResult = extractData(dataMap.get("test_table1"), columnList);

    Assert.assertTrue(equals(queryResult, expectedResult));

    // update column3 to 100 where column2 = 200;
    updateSql = "update test_table3 set column3 = 100 where column2 = 50;";
    client.executeStatement(updateSql);

    tableData = dataMap.get("test_table3");
    for (List<Object> rowData : tableData) {
      if ((float) rowData.get(2) == 50) {
        rowData.set(3, 100d);
      }
    }

    querySql = "select column2,column3 from test_table3;";
    resp = client.executeStatement(querySql);
    tableSchema = dataGenerator.getTableSchema("test_table3");
    resultTypes.clear();
    resultTypes.add(tableSchema.types.get(2));
    resultTypes.add(tableSchema.types.get(3));
    queryResult = convertData(resp.rowList, resultTypes);
    columnList.clear();
    columnList.add(2);
    columnList.add(3);
    expectedResult = extractData(dataMap.get("test_table1"), columnList);

    Assert.assertTrue(equals(queryResult, expectedResult));
  }

  public void deleteAndQueryData() throws TException {
    // delete where column1=100
    String deleteSql = "delete from test_table4 where column0 = 5;";
    client.executeStatement(deleteSql);

    Set<List<Object>> tableData = dataMap.get("test_table4");
    tableData.removeIf(rowData -> (int) rowData.get(0) == 5);

    String querySql = "select column0,column5 from test_table4;";
    ExecuteStatementResp resp = client.executeStatement(querySql);
    TableSchema tableSchema = dataGenerator.getTableSchema("test_table4");
    List<DataType> resultTypes = new ArrayList<>();
    resultTypes.add(tableSchema.types.get(0));
    resultTypes.add(tableSchema.types.get(5));
    Set<List<Object>> queryResult = convertData(resp.rowList, resultTypes);
    List<Integer> columnList = new ArrayList<>();
    columnList.add(0);
    columnList.add(5);
    Set<List<Object>> expectedResult = extractData(dataMap.get("test_table4"), columnList);
    Assert.assertTrue(equals(queryResult, expectedResult));
  }

  private boolean check(String type, Object expectValue, String actualValue) {
    switch (type) {
      case "int":
        return expectValue.equals(Integer.valueOf(actualValue));
      case "long":
        return expectValue.equals(Long.valueOf(actualValue));
      case "float":
        return Objects.equals(expectValue, Float.valueOf(actualValue));
      case "double":
        return Objects.equals(expectValue, Double.valueOf(actualValue));
      default:
        // string
        return Objects.equals(expectValue, actualValue);
    }
  }

  private static Set<List<Object>> convertData(List<List<String>> data, List<DataType> type) {
    Set<List<Object>> result = new HashSet<>();
    int rowSize = data.size();
    int colSize = type.size();

    for (int rowId = 0; rowId < rowSize; rowId++) {
      List<String> stringRowData = data.get(rowId);
      List<Object> rowData = new ArrayList<>();
      for (int columnId = 0; columnId < colSize; columnId++) {
        switch (type.get(columnId)) {
          case INT:
            rowData.add(Integer.valueOf(stringRowData.get(columnId)));
            break;
          case LONG:
            rowData.add(Long.valueOf(stringRowData.get(columnId)));
            break;
          case FLOAT:
            rowData.add(Float.valueOf(stringRowData.get(columnId)));
            break;
          case DOUBLE:
            rowData.add(Double.valueOf(stringRowData.get(columnId)));
            break;
          case STRING:
            rowData.add(stringRowData.get(columnId));
            break;
        }
      }
      result.add(rowData);
    }
    return result;
  }

  private static Set<List<Object>> extractData(
      Set<List<Object>> tableData, List<Integer> columnList) {
    Set<List<Object>> result = new HashSet<>();
    for (List<Object> allRowData : tableData) {
      List<Object> resultRowData = new ArrayList<>();
      for (int column : columnList) {
        resultRowData.add(allRowData.get(column));
      }
      result.add(resultRowData);
    }
    return result;
  }

  public static boolean equals(Set<List<Object>> set1, Set<List<Object>> set2) {
    if (set1 == null || set2 == null) return set1 == set2;

    if (set1.size() != set2.size()) return false;

    for (List<Object> list : set1) {
      boolean found = false;

      for (List<Object> anotherList : set2) {
        if (list.size() == anotherList.size()
            && list.containsAll(anotherList)
            && anotherList.containsAll(list)) {
          found = true;
          break;
        }
      }

      if (!found) {
        return false;
      }
    }

    return true;
  }

  @Override
  public void close() {
    if (client != null) {
      try {
        client.executeStatement("drop database db1;");
      } catch (TException e) {
        LOGGER.error("{}", e.getMessage(), e);
      }
      client.close();
    }
  }
}
