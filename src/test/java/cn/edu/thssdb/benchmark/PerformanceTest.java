package cn.edu.thssdb.benchmark;

import cn.edu.thssdb.benchmark.executor.PerformanceTestExecutor;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerformanceTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(PerformanceTest.class);
  private static PerformanceTestExecutor performanceTestExecutor;

  @BeforeClass
  public static void setUp() throws Exception {
    performanceTestExecutor = new PerformanceTestExecutor();
  }

  @AfterClass
  public static void tearDown() {
    performanceTestExecutor.close();
  }

  @Test
  public void simpleTest() throws TException {
    // create database
    LOGGER.info("======================== Performance test  ======================== ");
    performanceTestExecutor.test();
  }
}
