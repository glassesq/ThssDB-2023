package cn.edu.thssdb.benchmark.executor;

import cn.edu.thssdb.benchmark.common.Client;
import cn.edu.thssdb.benchmark.common.Constants;
import cn.edu.thssdb.benchmark.common.TableSchema;
import cn.edu.thssdb.benchmark.config.PerformanceTestConfig;
import cn.edu.thssdb.benchmark.generator.BaseDataGenerator;
import cn.edu.thssdb.benchmark.generator.OperationGenerator;
import cn.edu.thssdb.benchmark.generator.PerformanceDataGenerator;
import cn.edu.thssdb.benchmark.transaction.ITransaction;
import cn.edu.thssdb.benchmark.transaction.OperationType;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import com.clearspring.analytics.stream.quantile.TDigest;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

public class PerformanceTestExecutor extends TestExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(PerformanceTestExecutor.class);
  private static final int DATA_SEED = PerformanceTestConfig.DATA_SEED;
  private static final Map<OperationType, TDigest> transactionLatencyDigest =
      new EnumMap<>(OperationType.class);
  private List<Client> clients;
  private Map<Integer, PerformanceTestExecutor.Measurement> measurements = new HashMap<>();
  private int CLIENT_NUMBER = PerformanceTestConfig.CLIENT_NUMBER;
  private int TABLE_NUMBER = PerformanceTestConfig.TABLE_NUMBER;
  private int OPERATION_NUMBER = PerformanceTestConfig.OPERATION_NUMBER;
  private String OPERATION_RATIO = PerformanceTestConfig.OPERATION_RATIO;

  BaseDataGenerator dataGenerator;
  WeightedRandomPicker weightedRandomPicker;

  public PerformanceTestExecutor() throws TException {
    dataGenerator = new PerformanceDataGenerator(TABLE_NUMBER);
    weightedRandomPicker =
        new WeightedRandomPicker(
            Arrays.stream(OPERATION_RATIO.split(":")).mapToInt(Integer::parseInt).toArray());
    clients = new ArrayList<>();
    for (OperationType type : OperationType.values()) {
      transactionLatencyDigest.put(type, new TDigest(3000, new Random(DATA_SEED)));
    }
    for (int number = 0; number < CLIENT_NUMBER; number++) {
      clients.add(new Client());
      measurements.put(number, new PerformanceTestExecutor.Measurement());
    }
  }

  public void test() throws TException {
    // create database and use database
    Client schemaClient = clients.get(0);
    OperationGenerator operationGenerator = new OperationGenerator(dataGenerator, DATA_SEED);
    schemaClient.executeStatement("drop database db_performance;");
    // make sure database not exist, it's ok to ignore the error
    ExecuteStatementResp resp1 = schemaClient.executeStatement("create database db_performance;");
    Assert.assertEquals(Constants.SUCCESS_STATUS_CODE, resp1.status.code);
    LOGGER.info("Create database db_performance finished");
    for (int i = 0; i < CLIENT_NUMBER; i++) {
      Client client = clients.get(i);
      ExecuteStatementResp resp2 = client.executeStatement("use db_performance;");
      Assert.assertEquals(Constants.SUCCESS_STATUS_CODE, resp2.status.code);
      LOGGER.info("Client-" + i + " use db_performance finished");
    }
    for (TableSchema tableSchema : dataGenerator.getSchemaMap().values()) {
      createTable(tableSchema, clients.get(0));
    }

    // performance test
    long startTestTime = System.currentTimeMillis();
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (int i = 0; i < CLIENT_NUMBER; i++) {
      int index = i;
      CompletableFuture<Void> completableFuture =
          CompletableFuture.runAsync(
              () -> {
                try {
                  LOGGER.info("Start Performance Test for Client-" + index);
                  Client client = clients.get(index);
                  PerformanceTestExecutor.Measurement measurement = measurements.get(index);
                  for (int m = 0; m < OPERATION_NUMBER; m++) {
                    OperationType type = OperationType.values()[weightedRandomPicker.getNext()];
                    ITransaction transaction = operationGenerator.generateTransaction(type);
                    long startTime = System.nanoTime();
                    transaction.execute(client);
                    long finishTime = System.nanoTime();
                    // Assert.assertEquals(Constants.SUCCESS_STATUS_CODE, resp.status.code);
                    measurement.record(
                        type, transaction.getTransactionSize(), finishTime - startTime);
                  }
                  LOGGER.info("Finish Performance Test for Client-" + index);
                } catch (Exception e) {
                  e.printStackTrace();
                }
              });
      futures.add(completableFuture);
    }
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    long endTestTime = System.currentTimeMillis();
    LOGGER.info("Finish performance test after {} ms", endTestTime - startTestTime);

    // calculate measurement
    List<Double> quantiles = Arrays.asList(0.0, 0.25, 0.5, 0.75, 0.9, 0.99, 1.0);

    long allOperationNumber = 0;
    long allLatencyNumber = 0;
    for (OperationType type : OperationType.values()) {
      TDigest digest = transactionLatencyDigest.get(type);
      double totalLatency = 0.0;
      long totalNumber = 0;
      for (PerformanceTestExecutor.Measurement measurement : measurements.values()) {
        totalLatency += measurement.gettransactionLatencySumThisClient().get(type);
        totalNumber += measurement.getOktransactionNumMap().get(type);
      }
      allOperationNumber += totalNumber;
      allLatencyNumber += totalLatency;
      LOGGER.info(type + " operation count: " + totalNumber);
      LOGGER.info(type + " per second: " + (totalNumber / (totalLatency / 1e9)));
      try {
        List<Double> results = new ArrayList<>();
        for (Double quantile : quantiles) {
          results.add(digest.quantile(quantile));
        }
        for (int i = 0; i < quantiles.size(); i++) {
          LOGGER.info(type + "-" + quantiles.get(i) + ": " + (results.get(i) / 1e6) + " ms");
        }
      } catch (Exception ignored) {
      }
    }
    LOGGER.info("Total operation count: {}", allOperationNumber);
    LOGGER.info("Avg latency of all operations:{} ns", (allLatencyNumber / (allOperationNumber)));
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
    private final Map<OperationType, Double> transactionLatencySumThisClient;
    private final Map<OperationType, Long> oktransactionNumMap;
    private final Map<OperationType, Long> okSQLNumMap;

    public Measurement() {
      transactionLatencySumThisClient = new EnumMap<>(OperationType.class);
      oktransactionNumMap = new EnumMap<>(OperationType.class);
      okSQLNumMap = new EnumMap<>(OperationType.class);
      for (OperationType transaction : OperationType.values()) {
        transactionLatencySumThisClient.put(transaction, 0.0);
        oktransactionNumMap.put(transaction, 0L);
        okSQLNumMap.put(transaction, 0L);
      }
    }

    public void record(OperationType transaction, int size, long latency) {
      synchronized (transactionLatencyDigest.get(transaction)) {
        transactionLatencyDigest.get(transaction).add(latency);
      }
      transactionLatencySumThisClient.put(
          transaction, transactionLatencySumThisClient.get(transaction) + latency);
      oktransactionNumMap.put(transaction, oktransactionNumMap.get(transaction) + 1);
      okSQLNumMap.put(transaction, okSQLNumMap.get(transaction) + size);
    }

    public Map<OperationType, Double> gettransactionLatencySumThisClient() {
      return transactionLatencySumThisClient;
    }

    public Map<OperationType, Long> getOktransactionNumMap() {
      return oktransactionNumMap;
    }

    public Map<OperationType, Long> getOkSQLNumMap() {
      return okSQLNumMap;
    }
  }

  public class WeightedRandomPicker {

    private final int[] weightCounter;
    private final Random random;
    private int weightSum;

    public WeightedRandomPicker(int[] weights) {
      // this.weights = weights;
      this.random = new Random();

      for (int weight : weights) {
        weightSum += weight;
      }

      this.weightCounter = new int[weights.length];
      weightCounter[0] = weights[0];
      for (int i = 1; i < weights.length; i++) {
        weightCounter[i] += weightCounter[i - 1] + weights[i];
      }
    }

    public int getNext() {
      float randomNumber = random.nextFloat() * weightSum;
      for (int i = 0; i < weightCounter.length; i++) {
        if (randomNumber <= weightCounter[i]) {
          return i;
        }
      }

      return weightCounter.length - 1;
    }
  }
}
