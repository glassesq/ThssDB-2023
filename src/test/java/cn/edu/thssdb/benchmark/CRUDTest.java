package cn.edu.thssdb.benchmark;

import cn.edu.thssdb.benchmark.executor.CRUDTestExecutor;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CRUDTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(CRUDTest.class);

  private static CRUDTestExecutor CRUDTestExecutor;

  @BeforeClass
  public static void setUp() throws Exception {
    CRUDTestExecutor = new CRUDTestExecutor();
  }

  @AfterClass
  public static void tearDown() {
    CRUDTestExecutor.close();
  }

  @Test
  public void crudTest() throws TException {
    // create database
    LOGGER.info("======================== Create database  ======================== ");
    CRUDTestExecutor.createAndUseDB();
    // create table
    LOGGER.info("======================== Create table ========================");
    CRUDTestExecutor.createTable();
    // insert data:
    LOGGER.info("======================== Insert data ========================");
    CRUDTestExecutor.insertData();
    // query data:
    LOGGER.info("======================== Query data ========================");
    CRUDTestExecutor.queryData();
    // update and query data
    LOGGER.info("======================== Update and re-query data ========================");
    CRUDTestExecutor.updateAndQueryData();
    // delete and query data
    LOGGER.info("======================== Delete and re-query data ========================");
    CRUDTestExecutor.deleteAndQueryData();
  }
}
