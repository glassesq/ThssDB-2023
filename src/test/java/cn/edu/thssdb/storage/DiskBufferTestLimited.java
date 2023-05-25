package cn.edu.thssdb.storage;

import cn.edu.thssdb.communication.IO;
import cn.edu.thssdb.runtime.ServerRuntime;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.storage.page.IndexPage;
import cn.edu.thssdb.storage.page.Page;
import cn.edu.thssdb.type.DataType;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

import static cn.edu.thssdb.storage.DiskBuffer.concat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;

public class DiskBufferTestLimited {

  String databaseName = "databaseForDiskBufferTest";
  Database.DatabaseMetadata currentDatabase = null;

  File testDir;

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

    ServerRuntime.config.pageSize = (int) (Runtime.getRuntime().maxMemory() / 40);
    System.out.println(
        "#### Page size is set to " + ServerRuntime.config.pageSize + " for disk buffer test.");
    boolean result = ServerRuntime.config.pageSize > 1024 * 1024;
    assumeFalse(result);

    ServerRuntime.setup();
    /* create an empty database for testing purpose. */
    int transactionId = ServerRuntime.newTablespace();
    Database.DatabaseMetadata.createDatabase(transactionId, databaseName);
    currentDatabase =
        ServerRuntime.databaseMetadata.get(ServerRuntime.databaseNameLookup.get(databaseName));
    assumeNotNull(currentDatabase);
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

  @Test
  public void testSoftReferencedBuffer() throws Exception {

    Table.TableMetadata tableMetadata = new Table.TableMetadata();
    tableMetadata.prepare("testSoftReferencedBufferTable", ServerRuntime.newTablespace());
    Column column = new Column();
    String columnName = "columnOne";
    column.prepare(columnName, DataType.INT, 0);
    column.setPrimaryKey(0);

    ArrayList<String> names = new ArrayList<>();
    names.add(columnName);
    ArrayList<Column> columns = new ArrayList<>();
    columns.add(column);
    ArrayList<Integer> orders = new ArrayList<>();
    orders.add(0);

    tableMetadata.setColumnsAndCompute(names, columns, orders, 1, 0);
    //    tableMetadata.addColumn(columnName, column);
    currentDatabase.createTable(-1, tableMetadata);

    int spaceId = tableMetadata.spaceId;
    ;

    ArrayList<Page> keepPages = new ArrayList<>();
    int maxTestPageSize = 512;
    int maxKeepPageSize = 10;
    for (int i = 3; i < maxTestPageSize; i++) {
      IndexPage page = IndexPage.createIndexPage(-1, spaceId, i);
      page.writeAll(-1);
      IO.pushTransactionCommit(-1);
    }
    DiskBuffer.buffer.invalidateAll();

    for (int i = 3; i < maxTestPageSize; i++) {
      if (ThreadLocalRandom.current().nextDouble() < 0.05 && keepPages.size() <= maxKeepPageSize) {
        Page page = IO.read(spaceId, i);
        keepPages.add(page);
      }
    }

    Page page;
    for (int i = 3; i < maxTestPageSize; i++) {
      {
        page = IO.read(spaceId, i);
        assertEquals(page.pageId, i);
        assertEquals(page.spaceId, spaceId);
      }
      if (ThreadLocalRandom.current().nextDouble() < 0.1) {
        for (Page tempPage : keepPages) {
          Page inBufferPage =
              DiskBuffer.buffer.getIfPresent(concat(tempPage.spaceId, tempPage.pageId));
          assertNotNull(inBufferPage);
        }
      }
    }
  }
}
