package cn.edu.thssdb.benchmark;

import cn.edu.thssdb.benchmark.executor.ConcurrentTestExecutor;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConcurrentTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentTest.class);

  private static ConcurrentTestExecutor concurrentTestExecutor;

  @BeforeClass
  public static void setUp() throws Exception {
    concurrentTestExecutor = new ConcurrentTestExecutor();
  }

  @AfterClass
  public static void tearDown() {
    concurrentTestExecutor.close();
  }

  @Test
  public void concurrentTest() throws TException {
    // create database
    LOGGER.info("======================== Create database  ======================== ");
    concurrentTestExecutor.createAndUseDB();
    // create table
    LOGGER.info("======================== Concurrent create table ========================");
    concurrentTestExecutor.concurrentCreateTable();
    // insert data:
    LOGGER.info(
        "======================== Concurrent insert and query data ========================");
    concurrentTestExecutor.concurrentInsertAndQuery();
  }
}
