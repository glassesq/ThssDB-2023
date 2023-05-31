package cn.edu.thssdb.benchmark.transaction;

import cn.edu.thssdb.benchmark.common.Client;
import org.apache.thrift.TException;

public class Transaction implements ITransaction {
  String[] sqlList;

  public Transaction(String[] sqlList) {
    this.sqlList = sqlList;
  }

  public void execute(Client client) throws TException {
    client.executeStatement("begin transaction;");
    for (String sql : sqlList) {
      client.executeStatement(sql);
    }
    client.executeStatement("commit;");
  }

  @Override
  public int getTransactionSize() {
    return sqlList.length;
  }
}
