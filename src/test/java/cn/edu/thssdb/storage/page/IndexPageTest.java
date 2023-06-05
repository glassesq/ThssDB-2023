package cn.edu.thssdb.storage.page;

import cn.edu.thssdb.communication.IO;
import cn.edu.thssdb.runtime.ServerRuntime;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.type.DataType;
import cn.edu.thssdb.utils.Pair;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;
import static org.junit.Assert.assertFalse;

public class IndexPageTest {

  static File testDir;
  String databaseName = "testIndexPageDatabase";
  Database.DatabaseMetadata currentDatabase = null;

  @Before
  public void setup() throws Exception {
    System.out.println("maximum memory: " + Runtime.getRuntime().maxMemory());
    System.out.println(" ##### START TEST");
    ServerRuntime.config.testPath = "./testOnly" + ThreadLocalRandom.current().nextInt();
    ServerRuntime.config.MetadataFilename = ServerRuntime.config.testPath + "/example.json";
    ServerRuntime.config.WALFilename = ServerRuntime.config.testPath + "/WAL.log";
    ServerRuntime.config.DummyLogFilename = ServerRuntime.config.testPath + "/dummy.log";
    ServerRuntime.config.tablespacePath = ServerRuntime.config.testPath + "/" + "base";
    ServerRuntime.config.testPathRecover = ServerRuntime.config.testPath + "/" + "checkpoint";
    ServerRuntime.config.DummyLogRecoverFilename =
        ServerRuntime.config.testPath + "/" + "DummyLogRecover.log";

    testDir = new File(ServerRuntime.config.testPath);
    try {
      FileUtils.deleteDirectory(testDir);
    } catch (Exception ignore) {
    }

    testDir.mkdirs();
    ServerRuntime.setup();
    /* create an empty database for testing purpose. */
    long transactionId = ServerRuntime.newTransaction();
    Database.DatabaseMetadata.createDatabase(transactionId, databaseName);
    currentDatabase =
        ServerRuntime.databaseMetadata.get(ServerRuntime.databaseNameLookup.get(databaseName));
    assertNotNull(currentDatabase);
    System.out.println("START TEST : metadata info setup");
  }

  @After
  public void cleanup() {
    System.out.println("\n ##### END TEST");
    //    try {
    //      FileUtils.deleteDirectory(testDir);
    //    } catch (Exception ignore) {
    //      ignore.printStackTrace();
    //    }
    System.out.println("END TEST : metadata clean up \n\n");
  }

  @Test
  public void testSplitDataPageAfterRootIsSplitSingleThread() throws Exception {
    DataType[] types = new DataType[5];
    types[0] = DataType.INT;
    types[1] = DataType.STRING;
    types[2] = DataType.FLOAT;
    types[3] = DataType.LONG;
    types[4] = DataType.DOUBLE;

    Table.TableMetadata tableMetadata = new Table.TableMetadata();
    tableMetadata.prepare("A", ServerRuntime.newTablespace());
    ArrayList<Column> columns = new ArrayList<>();
    ArrayList<String> names = new ArrayList<>();
    ArrayList<Integer> orders = new ArrayList<>();
    int primaryKeyNumber = 1;
    int nonPrimaryKeyNumber = 5;

    for (int i = 0; i < primaryKeyNumber + nonPrimaryKeyNumber; i++) {
      Column column = new Column();
      String columnName = "column" + String.valueOf(i - nonPrimaryKeyNumber);
      column.prepare(
          columnName,
          types[ThreadLocalRandom.current().nextInt(types.length)],
          ThreadLocalRandom.current().nextInt(5) + 1);
      column.setPrimaryKey(i - nonPrimaryKeyNumber);
      names.add(columnName);
      columns.add(column);
      orders.add(i - nonPrimaryKeyNumber);
    }
    tableMetadata.setColumnsAndCompute(
        names, columns, orders, primaryKeyNumber, nonPrimaryKeyNumber);

    System.out.println(
        "max record size:"
            + tableMetadata.getMaxRecordLength(IndexPage.RecordInPage.USER_DATA_RECORD));
    assertTrue(
        2 * tableMetadata.getMaxRecordLength(IndexPage.RecordInPage.USER_DATA_RECORD)
            < ServerRuntime.config.pageSize - 64);

    long transactionId = ServerRuntime.newTransaction();
    currentDatabase.createTable(transactionId, tableMetadata);

    IndexPage rootPage =
        (IndexPage) IO.read(tableMetadata.spaceId, ServerRuntime.config.indexRootPageIndex);

    ArrayList<RecordLogical> recordsInRoot = new ArrayList<>();

    //    BufferedWriter writer = new BufferedWriter(new
    // FileWriter("/Users/rongyi/Desktop/metadata/wrong.txt", true));
    //    dummyLogOutputLock.unlock();

    //    File targetFile = new File("/Users/rongyi/Desktop/wrong2.txt");
    //    LineIterator iterator = FileUtils.lineIterator(targetFile, "UTF-8");
    for (int time = 0; time < 1000; time++) {
      RecordLogical record = new RecordLogical(tableMetadata);
      for (int i = 0; i < record.primaryKeyValues.length; i++) {
        Pair<String, ValueWrapper> r =
            ValueWrapperTest.createSingleValueWrapperRandomly(
                tableMetadata.getColumnDetailByOrderInType(i, true));
        while (r.right.isNull)
          r =
              ValueWrapperTest.createSingleValueWrapperRandomly(
                  tableMetadata.getColumnDetailByOrderInType(i, true));
        record.primaryKeyValues[i] = r.right;
      }
      for (int i = 0; i < record.nonPrimaryKeyValues.length; i++) {
        Pair<String, ValueWrapper> r =
            ValueWrapperTest.createSingleValueWrapperRandomly(
                tableMetadata.getColumnDetailByOrderInType(i, false));
        record.nonPrimaryKeyValues[i] = r.right;
      }

      //      if (iterator.hasNext()) record.primaryKeyValues[0].setWithNull(iterator.nextLine());
      //      else break;

      //            if( time % 2 == 0 )
      // record.primaryKeyValues[0].setWithNull(String.valueOf(time));
      //      else record.primaryKeyValues[0].setWithNull(String.valueOf(-time));

      IndexPage.RecordInPage recordInPage =
          IndexPage.makeRecordInPageFromLogical(record, tableMetadata);

      //      transactionId = ServerRuntime.newTransaction();
      //      Pair<Boolean, IndexPage.RecordInPage> insertPos =
      // rootPage.scanTreeAndReturnRecord(transactionId, recordInPage.primaryKeyValues);
      //      ServerRuntime.releaseAllLocks(transactionId);
      //      if (insertPos.left) continue;

      transactionId = ServerRuntime.newTransaction();
      //      writer.write(record.primaryKeyValues[0].toString() + "\n");
      System.out.println(record);
      System.out.println("record size:" + recordsInRoot.size());
      boolean r = rootPage.insertDataRecordIntoTree(transactionId, record);
      //      writer.flush();
      System.out.println(r);

      boolean checkR = true;
      for (RecordLogical recordAlready : recordsInRoot) {
        if (ValueWrapper.compareArray(recordAlready.primaryKeyValues, record.primaryKeyValues)
            == 0) {
          checkR = false;
          break;
        }
      }

      assertEquals(r, checkR);
      if (!r) continue;

      //      r = rootPage.insertDataRecordIntoTree(transactionId, record);
      ServerRuntime.releaseAllLocks(transactionId);

      recordsInRoot.add(record);

      IndexPage testPage;
      ArrayList<RecordLogical> recordsParseData = new ArrayList<>();
      transactionId = ServerRuntime.newTransaction();
      Pair<Integer, ArrayList<RecordLogical>> dataResult =
          rootPage.getLeftmostDataPage(transactionId);
      System.out.println("leftmost data page: " + dataResult.left + " " + dataResult.right.size());
      assertNotEquals(dataResult.left.intValue(), 0);
      while (dataResult.left.intValue() != 0) {
        testPage = (IndexPage) IO.read(tableMetadata.spaceId, dataResult.left);
        dataResult = testPage.getAllRecordLogical(transactionId);
        for (int i = 0; i < dataResult.right.size(); i++) {
          recordsParseData.add(dataResult.right.get(i));
          //        System.out.println(dataResult.right.get(i));
        }
      }
      assertEquals(recordsParseData.size(), recordsInRoot.size());
      ServerRuntime.releaseAllLocks(transactionId);

      dataResult = rootPage.getLeftmostDataPage(transactionId);
      if (dataResult.left.intValue() == 0) {
        continue;
      }
      ServerRuntime.releaseAllLocks(transactionId);
      assertNotEquals(dataResult.left.intValue(), 0);

      transactionId = ServerRuntime.newTransaction();
      Pair<Boolean, IndexPage.RecordInPage> insertPos =
          rootPage.scanTreeAndReturnRecord(transactionId, record.primaryKeyValues);
      ServerRuntime.releaseAllLocks(transactionId);

      assertTrue(insertPos.left);
    }
    recordsInRoot.sort((a, b) -> ValueWrapper.compareArray(a.primaryKeyValues, b.primaryKeyValues));

    transactionId = ServerRuntime.newTransaction();
    ServerRuntime.releaseAllLocks(transactionId);

    IndexPage testPage;
    ArrayList<RecordLogical> recordsParseData = new ArrayList<>();
    transactionId = ServerRuntime.newTransaction();
    Pair<Integer, ArrayList<RecordLogical>> dataResult =
        rootPage.getLeftmostDataPage(transactionId);
    System.out.println("leftmost data page: " + dataResult.left + " " + dataResult.right.size());
    assertNotEquals(dataResult.left.intValue(), 0);
    while (dataResult.left.intValue() != 0) {
      testPage = (IndexPage) IO.read(tableMetadata.spaceId, dataResult.left);
      dataResult = testPage.getAllRecordLogical(transactionId);
      for (int i = 0; i < dataResult.right.size(); i++) {
        recordsParseData.add(dataResult.right.get(i));
        //        System.out.println(dataResult.right.get(i));
      }
    }
    assertEquals(recordsParseData.size(), recordsInRoot.size());

    ServerRuntime.releaseAllLocks(transactionId);

    recordsInRoot.sort((a, b) -> ValueWrapper.compareArray(a.primaryKeyValues, b.primaryKeyValues));

    assertEquals(recordsParseData.size(), recordsInRoot.size());
    for (int i = 0; i < recordsParseData.size(); i++) {
      assertEquals(
          ValueWrapper.compareArray(
              recordsParseData.get(i).primaryKeyValues, recordsInRoot.get(i).primaryKeyValues),
          0);
      assertEquals(
          ValueWrapper.compareArray(
              recordsParseData.get(i).nonPrimaryKeyValues,
              recordsInRoot.get(i).nonPrimaryKeyValues),
          0);
      transactionId = ServerRuntime.newTransaction();
      Pair<Boolean, IndexPage.RecordInPage> scanResult =
          rootPage.scanTreeAndReturnRecord(transactionId, recordsParseData.get(i).primaryKeyValues);
      ServerRuntime.releaseAllLocks(transactionId);
      assertTrue(scanResult.left);
      assertEquals(
          ValueWrapper.compareArray(
              scanResult.right.primaryKeyValues, recordsInRoot.get(i).primaryKeyValues),
          0);
      assertEquals(
          ValueWrapper.compareArray(
              scanResult.right.nonPrimaryKeyValues, recordsInRoot.get(i).nonPrimaryKeyValues),
          0);

      transactionId = ServerRuntime.newTransaction();
      Pair<Integer, ArrayList<RecordLogical>> scanPageResult =
          rootPage.scanTreeAndReturnPage(transactionId, recordsParseData.get(i).primaryKeyValues);
      ServerRuntime.releaseAllLocks(transactionId);

      boolean found = false;
      for (int j = 0; j < scanPageResult.right.size(); j++) {
        RecordLogical scanRecordInPage = scanPageResult.right.get(j);
        if (ValueWrapper.compareArray(
                    scanRecordInPage.primaryKeyValues, recordsParseData.get(i).primaryKeyValues)
                == 0
            && ValueWrapper.compareArray(
                    scanRecordInPage.nonPrimaryKeyValues,
                    recordsParseData.get(i).nonPrimaryKeyValues)
                == 0) {
          found = true;
          break;
        }
      }
      assertTrue(found);
    }
  }

  @Test
  public void testSplitRootWithoutLockSingleThread() throws Exception {

    DataType[] types = new DataType[5];
    types[0] = DataType.INT;
    types[1] = DataType.STRING;
    types[2] = DataType.FLOAT;
    types[3] = DataType.LONG;
    types[4] = DataType.DOUBLE;

    Table.TableMetadata tableMetadata = new Table.TableMetadata();
    tableMetadata.prepare("B", ServerRuntime.newTablespace());
    ArrayList<Column> columns = new ArrayList<>();
    ArrayList<String> names = new ArrayList<>();
    ArrayList<Integer> orders = new ArrayList<>();
    int primaryKeyNumber = 1;
    int nonPrimaryKeyNumber = 10;
    for (int i = 0; i < primaryKeyNumber + nonPrimaryKeyNumber; i++) {
      Column column = new Column();
      String columnName = "column" + String.valueOf(i - nonPrimaryKeyNumber);
      column.prepare(
          columnName,
          types[ThreadLocalRandom.current().nextInt(types.length)],
          ThreadLocalRandom.current().nextInt(20));
      column.setPrimaryKey(i - nonPrimaryKeyNumber);
      names.add(columnName);
      columns.add(column);
      orders.add(i - nonPrimaryKeyNumber);
    }
    tableMetadata.setColumnsAndCompute(
        names, columns, orders, primaryKeyNumber, nonPrimaryKeyNumber);

    long transactionId = ServerRuntime.newTransaction();

    System.out.println(
        "max record size:"
            + tableMetadata.getMaxRecordLength(IndexPage.RecordInPage.USER_DATA_RECORD));
    assertTrue(
        2 * tableMetadata.getMaxRecordLength(IndexPage.RecordInPage.USER_DATA_RECORD)
            < ServerRuntime.config.pageSize - 64);

    currentDatabase.createTable(transactionId, tableMetadata);

    IndexPage rootPage =
        (IndexPage) IO.read(tableMetadata.spaceId, ServerRuntime.config.indexRootPageIndex);

    ArrayList<RecordLogical> recordsInRoot = new ArrayList<>();

    for (int time = 0; time < 256; time++) {
      RecordLogical record = new RecordLogical(tableMetadata);
      for (int i = 0; i < record.primaryKeyValues.length; i++) {
        Pair<String, ValueWrapper> r =
            ValueWrapperTest.createSingleValueWrapperRandomly(
                tableMetadata.getColumnDetailByOrderInType(i, true));
        while (r.right.isNull)
          r =
              ValueWrapperTest.createSingleValueWrapperRandomly(
                  tableMetadata.getColumnDetailByOrderInType(i, true));
        record.primaryKeyValues[i] = r.right;
      }
      for (int i = 0; i < record.nonPrimaryKeyValues.length; i++) {
        Pair<String, ValueWrapper> r =
            ValueWrapperTest.createSingleValueWrapperRandomly(
                tableMetadata.getColumnDetailByOrderInType(i, false));
        record.nonPrimaryKeyValues[i] = r.right;
      }

      IndexPage.RecordInPage recordInPage =
          IndexPage.makeRecordInPageFromLogical(record, tableMetadata);

      Pair<Boolean, IndexPage.RecordInPage> insertPos =
          rootPage.scanInternal(transactionId, recordInPage.primaryKeyValues);
      if (insertPos.left) continue;

      if (rootPage.freespaceStart.get()
              + tableMetadata.getMaxRecordLength(IndexPage.RecordInPage.USER_DATA_RECORD)
          >= ServerRuntime.config.pageSize - 10) {
        break;
      }
      rootPage.insertDataRecordIntoTree(transactionId, record);
      recordsInRoot.add(record);

      insertPos = rootPage.scanTreeAndReturnRecord(transactionId, record.primaryKeyValues);
      assertTrue(insertPos.left);
    }

    recordsInRoot.sort((a, b) -> ValueWrapper.compareArray(a.primaryKeyValues, b.primaryKeyValues));
  }

  @Test
  public void testRecordInPageNotSplitSingleThread() throws Exception {

    DataType[] types = new DataType[5];
    types[0] = DataType.INT;
    types[1] = DataType.STRING;
    types[2] = DataType.FLOAT;
    types[3] = DataType.LONG;
    types[4] = DataType.DOUBLE;

    Table.TableMetadata tableMetadata = new Table.TableMetadata();
    tableMetadata.prepare(
        "C" + ThreadLocalRandom.current().nextInt(), ServerRuntime.newTablespace());
    ArrayList<Column> columns = new ArrayList<>();
    ArrayList<String> names = new ArrayList<>();
    ArrayList<Integer> orders = new ArrayList<>();
    int primaryKeyNumber = 1;
    int nonPrimaryKeyNumber = 5;
    for (int i = 0; i < primaryKeyNumber + nonPrimaryKeyNumber; i++) {
      Column column = new Column();
      String columnName = "column" + String.valueOf(i - nonPrimaryKeyNumber);
      column.prepare(
          columnName,
          types[ThreadLocalRandom.current().nextInt(types.length)],
          ThreadLocalRandom.current().nextInt(4));
      column.setPrimaryKey(i - nonPrimaryKeyNumber);
      names.add(columnName);
      columns.add(column);
      orders.add(i - nonPrimaryKeyNumber);
    }
    tableMetadata.setColumnsAndCompute(
        names, columns, orders, primaryKeyNumber, nonPrimaryKeyNumber);

    long transactionId = ServerRuntime.newTransaction();

    currentDatabase.createTable(transactionId, tableMetadata);

    IndexPage rootPage =
        (IndexPage) IO.read(tableMetadata.spaceId, ServerRuntime.config.indexRootPageIndex);

    ArrayList<RecordLogical> recordsInRoot = new ArrayList<>();

    for (int time = 0; time < 256; time++) {
      RecordLogical record = new RecordLogical(tableMetadata);
      ArrayList<String> primaryString = new ArrayList<>();
      ArrayList<String> nonPrimaryString = new ArrayList<>();
      for (int i = 0; i < record.primaryKeyValues.length; i++) {
        Pair<String, ValueWrapper> r =
            ValueWrapperTest.createSingleValueWrapperRandomly(
                tableMetadata.getColumnDetailByOrderInType(i, true));
        while (r.right.isNull)
          r =
              ValueWrapperTest.createSingleValueWrapperRandomly(
                  tableMetadata.getColumnDetailByOrderInType(i, true));
        record.primaryKeyValues[i] = r.right;
        primaryString.add(r.left);
      }
      for (int i = 0; i < record.nonPrimaryKeyValues.length; i++) {
        Pair<String, ValueWrapper> r =
            ValueWrapperTest.createSingleValueWrapperRandomly(
                tableMetadata.getColumnDetailByOrderInType(i, false));
        record.nonPrimaryKeyValues[i] = r.right;
        nonPrimaryString.add(r.left);
      }

      IndexPage.RecordInPage recordInPage =
          IndexPage.makeRecordInPageFromLogical(record, tableMetadata);

      RecordLogical recordSecond = new RecordLogical(recordInPage, tableMetadata);

      for (int i = 0; i < record.primaryKeyValues.length; i++) {
        assert (recordSecond.primaryKeyValues[i].toString().equals(primaryString.get(i)));
      }
      for (int i = 0; i < record.nonPrimaryKeyValues.length; i++) {
        assert (recordSecond.nonPrimaryKeyValues[i].toString().equals(nonPrimaryString.get(i)));
      }

      RecordLogical recordThird =
          new RecordLogical(new IndexPage.RecordInPage(recordInPage), tableMetadata);

      for (int i = 0; i < record.primaryKeyValues.length; i++) {
        assert (recordThird.primaryKeyValues[i].toString().equals(primaryString.get(i)));
      }
      for (int i = 0; i < record.nonPrimaryKeyValues.length; i++) {
        assert (recordThird.nonPrimaryKeyValues[i].toString().equals(nonPrimaryString.get(i)));
      }

      Pair<Boolean, IndexPage.RecordInPage> insertPos =
          rootPage.scanInternal(transactionId, recordInPage.primaryKeyValues);
      if (insertPos.left) continue;

      System.out.println(recordThird);
      rootPage.insertDataRecordIntoTree(transactionId, recordThird);
      System.out.println("insert ok.");
      recordsInRoot.add(record);

      insertPos = rootPage.scanTreeAndReturnRecord(transactionId, recordThird.primaryKeyValues);
      // TODO: to fix ! there will be occassionally bug.
      assertTrue(insertPos.left);

      IndexPage pageIndex =
          IndexPage.createIndexPage(
              transactionId,
              tableMetadata.spaceId,
              ServerRuntime.config.indexRootPageIndex + time + 1);
      assertEquals(
          IO.read(tableMetadata.spaceId, ServerRuntime.config.indexRootPageIndex + time + 1).pageId,
          ServerRuntime.config.indexRootPageIndex + time + 1);

      insertPos = pageIndex.scanInternal(transactionId, recordInPage.primaryKeyValues);
      assertFalse(insertPos.left);
    }
  }
}
