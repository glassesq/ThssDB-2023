package cn.edu.thssdb.storage.page;

import cn.edu.thssdb.runtime.ServerRuntime;
import cn.edu.thssdb.schema.Database;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertNotNull;

public class IndexPageTest {

  File testDir;
  String databaseName = "testIndexPageDatabase";
  Database.DatabaseMetadata currentDatabase = null;

  @Before
  public void setup() throws Exception {
    System.out.println("maximum memory: " + Runtime.getRuntime().maxMemory());
    System.out.println(" ##### START TEST");
    ServerRuntime.config.testPath = "/Users/rongyi/Desktop/testOnly";
    ServerRuntime.config.MetadataFilename = ServerRuntime.config.testPath + "/example.json";
    ServerRuntime.config.WALFilename = ServerRuntime.config.testPath + "/WAL.log";

    testDir = new File(ServerRuntime.config.testPath);
    if (testDir.exists()) testDir.delete();
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
    testDir.delete();
    System.out.println("END TEST : metadata clean up \n\n");
  }

  public void fillWithDataRecords(Page page) {}

  @Test
  public void testIndexPageInsertInRoot_primaryKeyIntOnly() {}
}
