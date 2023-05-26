package cn.edu.thssdb.benchmark.executor;

import cn.edu.thssdb.benchmark.common.Client;
import cn.edu.thssdb.benchmark.common.Constants;
import cn.edu.thssdb.benchmark.common.TableReadWriteUtil;
import cn.edu.thssdb.benchmark.common.TableSchema;
import cn.edu.thssdb.benchmark.generator.BaseDataGenerator;
import cn.edu.thssdb.benchmark.generator.TransactionDataGenerator;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class TransactionTestExecutor extends TestExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransactionTestExecutor.class);

  private BaseDataGenerator dataGenerator;
  private Map<String, TableSchema> schemaMap;
  private Client client1;
  private Client client2;

  public TransactionTestExecutor() throws TException {
    dataGenerator = new TransactionDataGenerator();
    schemaMap = dataGenerator.getSchemaMap();
    client1 = new Client();
    client2 = new Client();
  }

  public void createAndUseDB() throws TException {
    client1.executeStatement("drop database db_transaction;");
    // make sure database not exist, it's ok to ignore the error
    ExecuteStatementResp resp = client1.executeStatement("create database db_transaction;");
    Assert.assertEquals(Constants.SUCCESS_STATUS_CODE, resp.status.code);
    LOGGER.info("Create database db_transaction finished");
    ExecuteStatementResp resp1 = client1.executeStatement("use db_transaction;");
    Assert.assertEquals(Constants.SUCCESS_STATUS_CODE, resp1.status.code);
    ExecuteStatementResp resp2 = client2.executeStatement("use db_transaction;");
    Assert.assertEquals(Constants.SUCCESS_STATUS_CODE, resp2.status.code);
    LOGGER.info("Use db_transaction finished");
  }

  public void prepareData() throws TException {
    createTable(schemaMap.get("tx"), client1);
    LOGGER.info("Create table tx finished");
    TableReadWriteUtil.insertData(schemaMap.get("tx"), client1, dataGenerator, 5);
    TableReadWriteUtil.queryAndCheckData(schemaMap.get("tx"), client1, dataGenerator, 5);
    LOGGER.info("Insert data into tx and query finished");
  }

  public void testLostUpdate() throws Exception {
    CompletableFuture<Void> future;
    client1.executeStatement("begin transaction");
    client2.executeStatement("begin transaction");
    try {
      client1.executeStatement("update tx set count = 1 where id = 0;");
      future =
          CompletableFuture.runAsync(
              () -> {
                try {
                  client2.executeStatement("update tx set count = 2 where id = 0;");
                } catch (TException e) {
                  e.printStackTrace();
                  throw new RuntimeException(e);
                } finally {
                  try {
                    client2.executeStatement("commit;");
                  } catch (TException e) {
                    throw new RuntimeException(e);
                  }
                }
              });
      Thread.sleep(1000);
      queryAndCheckOneColumn(
          client1, "select count from tx where id = 0;", Collections.singletonList("1"));
      LOGGER.info("Lost update test success!");

    } finally {
      client1.executeStatement("commit;");
    }
    future.get();
  }

  public void testDirtyRead() throws Exception {
    CompletableFuture<Void> future;
    client1.executeStatement("begin transaction");
    client2.executeStatement("begin transaction");
    try {
      queryAndCheckOneColumn(
          client1, "select count from tx where id = 1;", Collections.singletonList("1"));
      LOGGER.info("Init value of table1(id=1) is 1");
      client1.executeStatement("update tx set count = 100 where id = 0;");
      future =
          CompletableFuture.runAsync(
              () -> {
                try {
                  queryAndCheckOneColumn(
                      client2,
                      "select count from tx where id = 1;",
                      Collections.singletonList("1"));
                  LOGGER.info("Select value of table1(id=1) is 1. Dirty read test success!");
                } catch (TException e) {
                  e.printStackTrace();
                  throw new RuntimeException(e);
                } finally {
                  try {
                    client2.executeStatement("commit;");
                  } catch (TException e) {
                    throw new RuntimeException(e);
                  }
                }
              });
      Thread.sleep(1000);
      client1.executeStatement("update tx set count = 1 where id = 1;");
    } finally {
      client1.executeStatement("commit;");
    }
    future.get();
  }

  private void queryAndCheckOneColumn(Client client, String sql, List<String> result)
      throws TException {
    LOGGER.info(sql);
    ExecuteStatementResp resp = client.executeStatement(sql);
    Assert.assertEquals(Constants.SUCCESS_STATUS_CODE, resp.status.code);
    // check row size
    Assert.assertEquals(result.size(), resp.rowList.size());
    // check column
    Assert.assertEquals(1, resp.getColumnsList().size());
    // check result set
    for (int i = 0; i < resp.rowList.size(); i++) {
      Assert.assertEquals(result.get(i), resp.rowList.get(i).get(0));
    }
  }

  @Override
  public void close() {
    try {
      client1.executeStatement("drop database db_transaction");
    } catch (TException e) {
      LOGGER.error("{}", e.getMessage(), e);
    }
    client1.close();
    client2.close();
  }
}
