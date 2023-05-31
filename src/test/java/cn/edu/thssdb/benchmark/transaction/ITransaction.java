package cn.edu.thssdb.benchmark.transaction;

import cn.edu.thssdb.benchmark.common.Client;
import org.apache.thrift.TException;

public interface ITransaction {

  void execute(Client client) throws TException;

  int getTransactionSize();
}
