package cn.edu.thssdb.benchmark.generator;

import cn.edu.thssdb.benchmark.transaction.ITransaction;
import cn.edu.thssdb.benchmark.transaction.OperationType;
import cn.edu.thssdb.benchmark.transaction.SingleSQLTransaction;
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

  public ITransaction generateTransaction(OperationType type) {
    return operationFactory.newOperation(type);
  }

  private static class OperationFactory {
    private SQLGenerator sqlGenerator;

    public OperationFactory(SQLGenerator sqlGenerator) {
      this.sqlGenerator = sqlGenerator;
    }

    public ITransaction newOperation(OperationType type) {
      switch (type) {
        case INSERT:
          return new SingleSQLTransaction(sqlGenerator.generateInsertSQL());
        case UPDATE:
          return new SingleSQLTransaction(sqlGenerator.generateUpdateSQL());
        case DELETE:
          return new SingleSQLTransaction(sqlGenerator.generateDeleteSQL());
        case QUERY:
          return new SingleSQLTransaction(sqlGenerator.generateQuerySQL());
        case JOIN:
          return new SingleSQLTransaction(sqlGenerator.generateJoinSQL());
        default:
          LOGGER.error("Invalid operation type.");
      }
      return null;
    }
  }
}
