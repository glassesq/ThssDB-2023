package cn.edu.thssdb.schema;

import cn.edu.thssdb.runtime.ServerRuntime;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertNotNull;

public class ValueWrapperTest {
  File testDir;
  String databaseName = "testValueWrapperDatabase";
  Database.DatabaseMetadata currentDatabase = null;

  @Before
  public void setup() throws Exception {
    System.out.println("maximum memory: " + Runtime.getRuntime().maxMemory());
    System.out.println(" ##### START TEST");
    ServerRuntime.config.testPath = "./testOnly";
    ServerRuntime.config.MetadataFilename = ServerRuntime.config.testPath + "/example.json";
    ServerRuntime.config.WALFilename = ServerRuntime.config.testPath + "/WAL.log";

    testDir = new File(ServerRuntime.config.testPath);
    try {
      FileUtils.deleteDirectory(testDir);
    } catch (Exception ignore) {
    }
    testDir.mkdirs();
    ServerRuntime.setup();
    /* create an empty database for testing purpose. */
    int transactionId = ServerRuntime.newTablespace();
    Database.DatabaseMetadata.createDatabase(transactionId, databaseName);
    currentDatabase =
        ServerRuntime.databaseMetadata.get(ServerRuntime.databaseNameLookup.get(databaseName));
    assertNotNull(currentDatabase);
    System.out.println("START TEST : metadata info setup");
  }

  @After
  public void cleanup() {
    System.out.println("\n ##### END TEST");
    try {
      FileUtils.deleteDirectory(testDir);
    } catch (Exception ignore) {
    }
    System.out.println("END TEST : metadata clean up \n\n");
  }

  public String createStringRandomly(int length) {
    String pattern =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789甲乙丙丁戊己庚辛赵钱孙李周吴郑王";
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < length; i++) {
      int index = ThreadLocalRandom.current().nextInt(pattern.length());
      result.append(pattern.charAt(index));
    }
    return result.toString();
  }

  public ValueWrapper[] createValueWrapperListRandomly(Column[] column) {
    ValueWrapper[] vwList = new ValueWrapper[column.length];
    for (int i = 0; i < column.length; i++) {
      vwList[i] = createSingleValueWrapperRandomly(column[i]);
    }
    return vwList;
  }

  public ValueWrapper createSingleValueWrapperRandomly(Column column) {
    ValueWrapper vw = new ValueWrapper(column);
    /*
     if( ThreadLocalRandom.current().nextDouble() < 0.2 ) {
       vw.setWithNull("null");
       return vw;
     }
    */
    switch (column.type) {
      case INT:
        vw.setWithNull(String.valueOf(ThreadLocalRandom.current().nextInt()));
      case LONG:
        vw.setWithNull(String.valueOf(ThreadLocalRandom.current().nextLong()));
      case FLOAT:
        vw.setWithNull(String.valueOf(ThreadLocalRandom.current().nextFloat()));
      case DOUBLE:
        vw.setWithNull(String.valueOf(ThreadLocalRandom.current().nextDouble()));
      case STRING:
        vw.setWithNull(
            "'"
                + createStringRandomly(ThreadLocalRandom.current().nextInt(1 + column.getLength()))
                + "'");
    }
    return vw;
  }

  @Test
  public void testValueWrapper() {}
}
