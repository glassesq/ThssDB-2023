package cn.edu.thssdb.benchmark.generator;

import cn.edu.thssdb.benchmark.transaction.OperationType;
import cn.edu.thssdb.benchmark.transaction.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperationGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(OperationGenerator.class);
  private SQLGenerator sqlGenerator;
  private OperationFactory operationFactory;

  public OperationGenerator(BaseDataGenerator dataGenerator, int dataSeed) {
    sqlGenerator = new SQLGenerator(dataGenerator, dataSeed);
    operationFactory = new OperationFactory(sqlGenerator);
  }

  public Transaction generateTransaction(OperationType type) {
    return operationFactory.newOperation(type);
  }

  private static class OperationFactory {
    private SQLGenerator sqlGenerator;

    public OperationFactory(SQLGenerator sqlGenerator) {
      this.sqlGenerator = sqlGenerator;
    }

    public Transaction newOperation(OperationType type) {
      switch (type) {
        case INSERT:
          return generateInsertTransaction();
        case UPDATE:
          return generateUpdateTransaction();
        case DELETE:
          return generateDeleteTransaction();
        case QUERY:
          return generateQueryTransaction();
        case JOIN:
          return generateJoinTransaction();
        default:
          LOGGER.error("Invalid transaction type.");
      }
      return null;
    }

    private Transaction generateInsertTransaction() {
      String[] sqlList = new String[1];
      sqlList[0] = sqlGenerator.generateInsertSQL();
      return new Transaction(sqlList);
    }

    private Transaction generateUpdateTransaction() {
      String[] sqlList = new String[1];
      sqlList[0] = sqlGenerator.generateUpdateSQL();
      return new Transaction(sqlList);
    }

    private Transaction generateDeleteTransaction() {
      String[] sqlList = new String[1];
      sqlList[0] = sqlGenerator.generateDeleteSQL();
      return new Transaction(sqlList);
    }

    private Transaction generateQueryTransaction() {
      String[] sqlList = new String[1];
      sqlList[0] = sqlGenerator.generateQuerySQL();
      return new Transaction(sqlList);
    }

    private Transaction generateJoinTransaction() {
      String[] sqlList = new String[1];
      sqlList[0] = sqlGenerator.generateJoinSQL();
      return new Transaction(sqlList);
    }
  }
}
