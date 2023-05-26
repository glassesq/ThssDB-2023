package cn.edu.thssdb.benchmark.executor;

import cn.edu.thssdb.benchmark.common.Client;
import cn.edu.thssdb.benchmark.common.Constants;
import cn.edu.thssdb.benchmark.common.TableSchema;
import cn.edu.thssdb.benchmark.generator.BaseDataGenerator;
import cn.edu.thssdb.benchmark.generator.PerformanceDataGenerator;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import com.clearspring.analytics.stream.quantile.TDigest;
import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class PerformanceTestExecutor extends TestExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PerformanceTestExecutor.class);
  private static final int CONCURRENT_NUMBER = Constants.tableCount;
  private static final int WRITE_ROW_NUMBER = 10;
  private static final int COMPRESSION = 3000;
  private static final int DATA_SEED = 667;
  // 性能测试执行次数
  public static final int LOOP = 10;
  // 读写混合比例
  public static final double WRITE_RATIO = 0.5;
  private BaseDataGenerator dataGenerator;
  private Map<String, TableSchema> schemaMap;
  private List<Client> clients;
  private Random random = new Random(DATA_SEED);
  private static final Map<Operation, TDigest> operationLatencyDigest =
      new EnumMap<>(Operation.class);
  private Map<Integer, Measurement> measurements = new HashMap<>();

  public PerformanceTestExecutor() throws TException {
    dataGenerator = new PerformanceDataGenerator();
    schemaMap = dataGenerator.getSchemaMap();
    clients = new ArrayList<>();
    for (Operation operation : Operation.values()) {
      operationLatencyDigest.put(operation, new TDigest(COMPRESSION, new Random(DATA_SEED)));
    }
    for (int number = 0; number < CONCURRENT_NUMBER; number++) {
      clients.add(new Client());
      measurements.put(number, new Measurement());
    }
  }

  public void test() throws TException {
    // create database and use database
    Client schemaClient = clients.get(0);
    schemaClient.executeStatement("drop database db_performance;");
    // make sure database not exist, it's ok to ignore the error
    ExecuteStatementResp resp1 = schemaClient.executeStatement("create database db_performance;");
    Assert.assertEquals(Constants.SUCCESS_STATUS_CODE, resp1.status.code);
    LOGGER.info("Create database db_concurrent finished");
    for (int i = 0; i < CONCURRENT_NUMBER; i++) {
      Client client = clients.get(i);
      ExecuteStatementResp resp2 = client.executeStatement("use db_performance;");
      Assert.assertEquals(Constants.SUCCESS_STATUS_CODE, resp2.status.code);
      LOGGER.info("Client-" + i + " use db_performance finished");
      createTable(schemaMap.get("test_table" + i), client);
    }
    // performance test
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (int i = 0; i < CONCURRENT_NUMBER; i++) {
      int index = i;
      CompletableFuture<Void> completableFuture =
          CompletableFuture.runAsync(
              () -> {
                try {
                  LOGGER.info("Start Performance Test for Client-" + index);
                  Client client = clients.get(index);
                  Measurement measurement = measurements.get(index);
                  TableSchema tableSchema = schemaMap.get("test_table" + index);
                  for (int m = 0; m < LOOP; m++) {
                    double randomValue = random.nextDouble();
                    if (randomValue < WRITE_RATIO) {
                      doWriteOperation(measurement, m, tableSchema, client);
                    } else {
                      doQueryOperation(measurement, tableSchema, client);
                    }
                  }
                  LOGGER.info("Finish Performance Test for Client-" + index);
                } catch (Exception e) {
                  e.printStackTrace();
                }
              });
      futures.add(completableFuture);
    }
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    // calculate measurement
    List<Double> quantiles = Arrays.asList(0.0, 0.25, 0.5, 0.75, 0.9, 0.99, 1.0);
    for (Operation operation : Operation.values()) {
      TDigest digest = operationLatencyDigest.get(operation);
      double totalLatency = 0.0;
      long totalNumber = 0;
      long totalPoint = 0;
      for (Double quantile : quantiles) {
        LOGGER.info(operation + "-" + quantile + ": " + (digest.quantile(quantile) / 1e6) + " ms");
      }
      for (Measurement measurement : measurements.values()) {
        totalLatency += measurement.getOperationLatencySumThisClient().get(operation);
        totalPoint += measurement.getOkPointNumMap().get(operation);
        totalNumber += measurement.getOkOperationNumMap().get(operation);
      }
      LOGGER.info(operation + " per second: " + (totalNumber / (totalLatency / 1e9)));
      LOGGER.info(operation + " points per second: " + (totalPoint / (totalLatency / 1e9)));
    }
  }

  private void doWriteOperation(
      Measurement measurement, int loop, TableSchema tableSchema, Client client) throws TException {
    String tableName = tableSchema.tableName;
    for (int rowid = 0; rowid < WRITE_ROW_NUMBER; rowid++) {
      List<String> columns = new ArrayList<>();
      List<Object> data = new ArrayList<>();
      for (int columnId = 0; columnId < tableSchema.columns.size(); columnId++) {
        Object dataItem =
            dataGenerator.generateValue(tableName, loop * WRITE_ROW_NUMBER + rowid, columnId);
        if (dataItem != null) {
          columns.add(tableSchema.columns.get(columnId));
          data.add(dataItem);
        }
      }
      String sql =
          "Insert into "
              + tableName
              + "("
              + StringUtils.join(columns, ",")
              + ")"
              + " values("
              + StringUtils.join(data, ",")
              + ");";
      long startTime = System.nanoTime();
      ExecuteStatementResp resp = client.executeStatement(sql);
      long finishTime = System.nanoTime();
      Assert.assertEquals(Constants.SUCCESS_STATUS_CODE, resp.status.code);
      measurement.record(
          Operation.WRITE, WRITE_ROW_NUMBER * tableSchema.columns.size(), finishTime - startTime);
    }
  }

  private void doQueryOperation(Measurement measurement, TableSchema tableSchema, Client client)
      throws TException {
    String querySql = "select * from " + tableSchema.tableName + ";";
    long startTime = System.nanoTime();
    ExecuteStatementResp resp = client.executeStatement(querySql);
    long finishTime = System.nanoTime();
    Assert.assertEquals(Constants.SUCCESS_STATUS_CODE, resp.status.code);
    measurement.record(
        Operation.QUERY, resp.rowList.size() * tableSchema.columns.size(), finishTime - startTime);
  }

  @Override
  public void close() {
    try {
      clients.get(0).executeStatement("drop database db_performance");
    } catch (TException e) {
      LOGGER.error("{}", e.getMessage(), e);
    }
    if (clients != null) {
      for (Client client : clients) {
        client.close();
      }
    }
  }

  private class Measurement {
    private final Map<Operation, Double> operationLatencySumThisClient;
    private final Map<Operation, Long> okOperationNumMap;
    private final Map<Operation, Long> okPointNumMap;

    public Measurement() {
      operationLatencySumThisClient = new EnumMap<>(Operation.class);
      okOperationNumMap = new EnumMap<>(Operation.class);
      okPointNumMap = new EnumMap<>(Operation.class);
      for (Operation operation : Operation.values()) {
        operationLatencySumThisClient.put(operation, 0.0);
        okOperationNumMap.put(operation, 0L);
        okPointNumMap.put(operation, 0L);
      }
    }

    public void record(Operation operation, int size, long latency) {
      synchronized (operationLatencyDigest.get(operation)) {
        operationLatencyDigest.get(operation).add(latency);
      }
      operationLatencySumThisClient.put(
          operation, operationLatencySumThisClient.get(operation) + latency);
      okOperationNumMap.put(operation, okOperationNumMap.get(operation) + 1);
      okPointNumMap.put(operation, okPointNumMap.get(operation) + size);
    }

    public Map<Operation, Double> getOperationLatencySumThisClient() {
      return operationLatencySumThisClient;
    }

    public Map<Operation, Long> getOkOperationNumMap() {
      return okOperationNumMap;
    }

    public Map<Operation, Long> getOkPointNumMap() {
      return okPointNumMap;
    }
  }

  private enum Operation {
    WRITE,
    QUERY
  }
}
