package cn.edu.thssdb.benchmark.executor;

import cn.edu.thssdb.benchmark.common.Client;
import cn.edu.thssdb.benchmark.common.Constants;
import cn.edu.thssdb.benchmark.common.TableSchema;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TestExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestExecutor.class);

  public abstract void close();

  protected void createTable(TableSchema tableSchema, Client client) throws TException {
    StringBuilder sb = new StringBuilder("CREATE TABLE " + tableSchema.tableName + " (");
    for (int columnId = 0; columnId < tableSchema.columns.size(); columnId++) {
      sb.append(tableSchema.columns.get(columnId))
          .append(" ")
          .append(tableSchema.types.get(columnId).getType());
      if (tableSchema.notNull.get(columnId)) {
        sb.append(" NOT NULL");
      }
      sb.append(",");
    }
    sb.append("primary key(")
        .append(tableSchema.columns.get(tableSchema.primaryKeyColumnIndex))
        .append(")");
    sb.append(");");
    LOGGER.info(sb.toString());
    ExecuteStatementResp resp = client.executeStatement(sb.toString());
    Assert.assertEquals(Constants.SUCCESS_STATUS_CODE, resp.status.code);
    LOGGER.info("create table " + tableSchema.tableName + " finished!");
  }
}
