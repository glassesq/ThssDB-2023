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

  File testDir;
  String databaseName = "testIndexPageDatabase";
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

  @Test
  public void testRecordInPageNotSplit() throws Exception {

    DataType[] types = new DataType[5];
    types[0] = DataType.INT;
    types[1] = DataType.STRING;
    types[2] = DataType.FLOAT;
    types[3] = DataType.LONG;
    types[4] = DataType.DOUBLE;

    Table.TableMetadata tableMetadata = new Table.TableMetadata();
    tableMetadata.prepare("testRecordInPageCopy", ServerRuntime.newTablespace());
    ArrayList<Column> columns = new ArrayList<>();
    ArrayList<String> names = new ArrayList<>();
    ArrayList<Integer> orders = new ArrayList<>();
    int primaryKeyNumber = 10;
    int nonPrimaryKeyNumber = 10;
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
        System.out.println(
            recordSecond.primaryKeyValues[i].toString() + " " + primaryString.get(i));
        assert (recordSecond.primaryKeyValues[i].toString().equals(primaryString.get(i)));
      }
      for (int i = 0; i < record.nonPrimaryKeyValues.length; i++) {
        System.out.println(
            recordSecond.nonPrimaryKeyValues[i].toString() + " " + nonPrimaryString.get(i));
        assert (recordSecond.nonPrimaryKeyValues[i].toString().equals(nonPrimaryString.get(i)));
      }

      RecordLogical recordThird =
          new RecordLogical(new IndexPage.RecordInPage(recordInPage), tableMetadata);

      for (int i = 0; i < record.primaryKeyValues.length; i++) {
        System.out.println(recordThird.primaryKeyValues[i].toString() + " " + primaryString.get(i));
        assert (recordThird.primaryKeyValues[i].toString().equals(primaryString.get(i)));
      }
      for (int i = 0; i < record.nonPrimaryKeyValues.length; i++) {
        System.out.println(
            recordThird.nonPrimaryKeyValues[i].toString() + " " + nonPrimaryString.get(i));
        assert (recordThird.nonPrimaryKeyValues[i].toString().equals(nonPrimaryString.get(i)));
      }

      Pair<Boolean, IndexPage.RecordInPage> insertPos =
          rootPage.scanInternal(transactionId, recordInPage.primaryKeyValues);
      if (insertPos.left) continue;

      if (rootPage.freespaceStart
              + tableMetadata.getMaxRecordLength(IndexPage.RecordInPage.USER_DATA_RECORD)
          > ServerRuntime.config.pageSize) {
        break;
      }
      rootPage.insertDataRecordIntoTree(transactionId, recordThird);

      recordsInRoot.add(record);

      insertPos = rootPage.scanTreeAndReturnRecord(transactionId, recordThird.primaryKeyValues);
      System.out.println(rootPage.freespaceStart);
      assertTrue(insertPos.left);

      System.out.println("start test pointer **********");
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
      pageIndex.insertPointerRecordInternal(
          transactionId, recordInPage, pageIndex.pageId, insertPos.right);
      insertPos = pageIndex.scanInternalForPage(transactionId, pageIndex.pageId);
      assertTrue(insertPos.left);

      IndexPage.RecordInPage readRecordPointer = new IndexPage.RecordInPage();

      System.out.println("myOffset:" + insertPos.right.nextRecordInPage.myOffset);

      readRecordPointer.parseDeeplyInPage(pageIndex, insertPos.right.myOffset, tableMetadata);

      System.out.println(readRecordPointer);

      for (int i = 0; i < record.primaryKeyValues.length; i++) {
        System.out.println(
            readRecordPointer.primaryKeyValues[i].toString() + " " + primaryString.get(i));
        assert (readRecordPointer.primaryKeyValues[i].toString().equals(primaryString.get(i)));
      }

      System.out.println("end test pointer **********");
    }

    System.out.println("start test root!");
    rootPage.parseAllRecords();
    Pair<Integer, ArrayList<RecordLogical>> recordInRootParsed =
        rootPage.getAllRecordLogical(transactionId);
    assertEquals(recordInRootParsed.right.size(), recordsInRoot.size());

    recordsInRoot.sort((a, b) -> ValueWrapper.compareArray(a.primaryKeyValues, b.primaryKeyValues));

    for (int i = 0; i < recordsInRoot.size(); i++) {
      assertEquals(
          ValueWrapper.compareArray(
              recordInRootParsed.right.get(i).primaryKeyValues,
              recordsInRoot.get(i).primaryKeyValues),
          0);
      assertEquals(
          ValueWrapper.compareArray(
              recordInRootParsed.right.get(i).nonPrimaryKeyValues,
              recordsInRoot.get(i).nonPrimaryKeyValues),
          0);
    }
    System.out.println("end test root!");

    IndexPage shadowPage = new IndexPage(rootPage.bytes);
    Pair<Integer, ArrayList<RecordLogical>> shadowRecords =
        shadowPage.getAllRecordLogical(transactionId);

    assertEquals(shadowRecords.right.size(), recordsInRoot.size());

    recordsInRoot.sort((a, b) -> ValueWrapper.compareArray(a.primaryKeyValues, b.primaryKeyValues));

    for (int i = 0; i < recordsInRoot.size(); i++) {
      assertEquals(
          ValueWrapper.compareArray(
              shadowRecords.right.get(i).primaryKeyValues, recordsInRoot.get(i).primaryKeyValues),
          0);
      assertEquals(
          ValueWrapper.compareArray(
              shadowRecords.right.get(i).nonPrimaryKeyValues,
              recordsInRoot.get(i).nonPrimaryKeyValues),
          0);
    }
  }
}
