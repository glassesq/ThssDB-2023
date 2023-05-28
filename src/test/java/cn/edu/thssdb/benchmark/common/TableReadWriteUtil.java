package cn.edu.thssdb.benchmark.common;

import cn.edu.thssdb.benchmark.generator.BaseDataGenerator;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class TableReadWriteUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableReadWriteUtil.class);

  public static void insertData(
      TableSchema tableSchema, Client client, BaseDataGenerator dataGenerator, int rowNum)
      throws TException {
    String tableName = tableSchema.tableName;
    for (int rowid = 0; rowid < rowNum; rowid++) {
      List<String> columns = new ArrayList<>();
      List<Object> datas = new ArrayList<>();
      for (int columnId = 0; columnId < tableSchema.columns.size(); columnId++) {
        Object dataItem = dataGenerator.generateValue(tableName, rowid, columnId);
        if (dataItem != null) {
          columns.add(tableSchema.columns.get(columnId));
          datas.add(dataItem);
        }
      }
      String sql =
          "Insert into "
              + tableName
              + "("
              + join(columns, ",")
              + ")"
              + " values("
              + join(datas, ",")
              + ");";
      client.executeStatement(sql);
    }
  }

  public static void queryAndCheckData(
      TableSchema tableSchema, Client client, BaseDataGenerator dataGenerator, int rowNum)
      throws TException {
    String querySql =
        "select " + String.join(",", tableSchema.columns) + " from " + tableSchema.tableName + ";";
    ExecuteStatementResp resp = client.executeStatement(querySql);
    LOGGER.info(querySql);
    Assert.assertEquals(Constants.SUCCESS_STATUS_CODE, resp.status.code);
    // check row size
    Assert.assertEquals(rowNum, resp.rowList.size());
    // check column
    List<Integer> resultColumnToSchemaColumnIndex = new ArrayList<>();
    Assert.assertEquals(tableSchema.columns.size(), resp.getColumnsList().size());
    for (int i = 0; i < tableSchema.columns.size(); i++) {
      String[] columnSplitByDot = tableSchema.columns.get(i).split("\\.");
      String column = columnSplitByDot[columnSplitByDot.length - 1];
      if (!tableSchema.columns.contains(column)) {
        Assert.fail(String.format("Column [%s] not in schema!%n", resp.getColumnsList().get(i)));
      }
      resultColumnToSchemaColumnIndex.add(tableSchema.columns.indexOf(column));
    }
    // check result set
    for (int i = 0; i < resp.rowList.size(); i++) {
      for (int j = 0; j < tableSchema.columns.size(); j++) {
        int rowId =
            Integer.parseInt(
                resp.rowList
                    .get(i)
                    .get(
                        resultColumnToSchemaColumnIndex.indexOf(
                            tableSchema.primaryKeyColumnIndex)));
        Object dataItem =
            dataGenerator.generateValue(
                tableSchema.tableName, rowId, resultColumnToSchemaColumnIndex.get(j));
        if (dataItem == null) {
          Assert.assertEquals("null", resp.rowList.get(i).get(j));
        } else {
          if (dataItem instanceof String) {
            Assert.assertEquals(dataItem, "'" + resp.rowList.get(i).get(j) + "'");
          } else {
            Assert.assertEquals(dataItem.toString(), resp.rowList.get(i).get(j));
          }
        }
      }
    }
  }

  public static String join(Iterable<?> iterable, String separator) {
    return iterable == null ? null : join(iterable.iterator(), separator);
  }

  public static String join(Iterator<?> iterator, String separator) {
    if (iterator == null) {
      return null;
    } else if (!iterator.hasNext()) {
      return "";
    } else {
      Object first = iterator.next();
      if (!iterator.hasNext()) {
        return Objects.toString(first, "");
      } else {
        StringBuilder buf = new StringBuilder(256);
        if (first != null) {
          buf.append(first);
        }
        while (iterator.hasNext()) {
          if (separator != null) {
            buf.append(separator);
          }
          Object obj = iterator.next();
          if (obj != null) {
            buf.append(obj);
          }
        }
        return buf.toString();
      }
    }
  }
}
