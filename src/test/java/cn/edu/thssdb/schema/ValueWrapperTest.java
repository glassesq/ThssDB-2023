package cn.edu.thssdb.schema;

import cn.edu.thssdb.runtime.ServerRuntime;
import cn.edu.thssdb.type.DataType;
import cn.edu.thssdb.utils.Pair;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.ThreadLocalRandom;

import static cn.edu.thssdb.schema.ValueWrapper.compareArray;
import static org.junit.Assert.*;

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

  public static String createStringRandomly(int length) {
    String pattern =
        " \n\r\tabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789甲乙丙丁戊己庚辛赵钱孙李周吴郑王";
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < length; i++) {
      int index = ThreadLocalRandom.current().nextInt(pattern.length());
      result.append(pattern.charAt(index));
    }
    return result.toString();
  }

  public static Pair<String, ValueWrapper> createSingleValueWrapperRandomly(Column column) {
    ValueWrapper vw = new ValueWrapper(column);
    String realValue = null;
    if (ThreadLocalRandom.current().nextDouble() < 0.2) {
      vw.setWithNull("null");
      return new Pair<>("null", vw);
    }
    switch (column.type) {
      case INT:
        realValue = String.valueOf(ThreadLocalRandom.current().nextInt());
        vw.setWithNull(realValue);
        break;
      case LONG:
        realValue = String.valueOf(ThreadLocalRandom.current().nextLong());
        vw.setWithNull(realValue);
        break;
      case FLOAT:
        if (ThreadLocalRandom.current().nextDouble() < 0.5) {
          realValue = String.valueOf(ThreadLocalRandom.current().nextFloat());
        } else {
          Float f = Float.intBitsToFloat(ThreadLocalRandom.current().nextInt());
          while (f.isNaN()) f = Float.intBitsToFloat(ThreadLocalRandom.current().nextInt());
          realValue = String.valueOf(f);
        }
        vw.setWithNull(realValue);
        break;
      case DOUBLE:
        if (ThreadLocalRandom.current().nextDouble() < 0.5) {
          realValue = String.valueOf(ThreadLocalRandom.current().nextDouble());
        } else {
          Double d = Double.longBitsToDouble(ThreadLocalRandom.current().nextLong());
          while (d.isNaN()) d = Double.longBitsToDouble(ThreadLocalRandom.current().nextLong());
          realValue = String.valueOf(d);
        }
        vw.setWithNull(realValue);
        break;
      case STRING:
        realValue =
            createStringRandomly(ThreadLocalRandom.current().nextInt(1 + column.getStringLength()));
        vw.setWithNull("'" + realValue + "'");
        break;
    }
    return new Pair<>(realValue, vw);
  }

  @Test
  public void testSet() {

    DataType[] types = new DataType[5];
    types[0] = DataType.INT;
    types[1] = DataType.STRING;
    types[2] = DataType.FLOAT;
    types[3] = DataType.LONG;
    types[4] = DataType.DOUBLE;
    for (int i = 0; i < 128; i++) {
      Column column = new Column();
      column.prepare(
          "test",
          types[ThreadLocalRandom.current().nextInt(types.length)],
          ThreadLocalRandom.current().nextInt(16));

      Pair<String, ValueWrapper> A = createSingleValueWrapperRandomly(column); /* A is not Null */
      while (A.right.isNull) A = createSingleValueWrapperRandomly(column);
      Pair<String, ValueWrapper> B = createSingleValueWrapperRandomly(column); /* B is Null */
      while (!B.right.isNull) B = createSingleValueWrapperRandomly(column);
      assertEquals(A.right.compareTo(B.right), null);

      for (int j = 0; j < 128; j++) {
        Pair<String, ValueWrapper> result = createSingleValueWrapperRandomly(column);
        while (result.right.isNull) result = createSingleValueWrapperRandomly(column);
        assertEquals(result.left, result.right.toString());

        byte[] byteWithZero = new byte[result.right.bytes.length + 10];
        System.arraycopy(result.right.bytes, 0, byteWithZero, 0, result.right.bytes.length);
        ValueWrapper fromByteValueWrapper;
        if (!result.right.isNull)
          fromByteValueWrapper =
              new ValueWrapper(byteWithZero, column.type, column.getLength(), false);
        else fromByteValueWrapper = new ValueWrapper(true, result.right.type);

        assertEquals(result.left, fromByteValueWrapper.toString());
        ValueWrapper copyWrapper = new ValueWrapper(fromByteValueWrapper);
        assertEquals(result.left, copyWrapper.toString());
        assertEquals(result.right.compareTo(fromByteValueWrapper).intValue(), 0);
        assertEquals(fromByteValueWrapper.compareTo(copyWrapper).intValue(), 0);
        ValueWrapper[] aList = new ValueWrapper[2];
        aList[0] = new ValueWrapper(copyWrapper);
        aList[1] = result.right;
        ValueWrapper[] bList = new ValueWrapper[ThreadLocalRandom.current().nextInt(1, 1 + 2)];
        bList[0] = new ValueWrapper(copyWrapper.bytes, column.type, column.getLength(), false);
        if (bList.length > 1) {
          bList[1] = copyWrapper;
        }
        if (result.right.isNull) continue;

        for (int cmp = 0; cmp < 64; cmp++) {
          switch (column.type) {
            case STRING:
              String newValue = createStringRandomly(column.getStringLength() + 1);
              if (ThreadLocalRandom.current().nextDouble() < 0.7)
                copyWrapper.setWithNull("'" + newValue + "'");
              else {
                if (copyWrapper.isNull) continue;
                newValue = copyWrapper.toString();
              }
              assertEquals(
                  newValue.compareTo(result.left) > 0, copyWrapper.compareTo(result.right) > 0);
              assertEquals(
                  newValue.compareTo(result.left) < 0, copyWrapper.compareTo(result.right) < 0);

              if (bList.length > 1) {
                assertEquals(compareArray(aList, bList) > 0, result.left.compareTo(newValue) > 0);
              } else {
                assertTrue(compareArray(aList, bList) > 0);
              }
              break;
            case INT:
              int newIntValue = ThreadLocalRandom.current().nextInt();
              if (ThreadLocalRandom.current().nextDouble() < 0.7)
                copyWrapper.setWithNull(String.valueOf(newIntValue));
              else {
                if (copyWrapper.isNull) continue;
                newIntValue = Integer.parseInt(copyWrapper.toString());
              }
              assertEquals(
                  newIntValue > Integer.parseInt(result.left),
                  copyWrapper.compareTo(result.right) > 0);
              assertEquals(
                  newIntValue < Integer.parseInt(result.left),
                  copyWrapper.compareTo(result.right) < 0);

              if (bList.length > 1) {
                assertEquals(
                    compareArray(aList, bList) > 0, Integer.parseInt(result.left) > newIntValue);
              } else {
                assertTrue(compareArray(aList, bList) > 0);
              }
              break;
            case LONG:
              long newLongValue = ThreadLocalRandom.current().nextLong();
              if (ThreadLocalRandom.current().nextDouble() < 0.7)
                copyWrapper.setWithNull(String.valueOf(newLongValue));
              else {

                if (copyWrapper.isNull) continue;
                newLongValue = Long.parseLong(copyWrapper.toString());
              }
              assertEquals(
                  newLongValue > Long.parseLong(result.left),
                  copyWrapper.compareTo(result.right) > 0);
              assertEquals(
                  newLongValue < Long.parseLong(result.left),
                  copyWrapper.compareTo(result.right) < 0);

              if (bList.length > 1) {
                assertEquals(
                    compareArray(aList, bList) > 0, Long.parseLong(result.left) > newLongValue);
              } else {
                assertTrue(compareArray(aList, bList) > 0);
              }

              break;
            case FLOAT:
              float newFloatValue = ThreadLocalRandom.current().nextFloat();
              if (ThreadLocalRandom.current().nextDouble() < 0.7)
                copyWrapper.setWithNull(String.valueOf(newFloatValue));
              else {
                if (copyWrapper.isNull) continue;
                newFloatValue = Float.parseFloat(copyWrapper.toString());
              }
              assertEquals(
                  Float.compare(newFloatValue, Float.parseFloat(result.left)) > 0,
                  copyWrapper.compareTo(result.right) > 0);
              assertEquals(
                  Float.compare(newFloatValue, Float.parseFloat(result.left)) < 0,
                  copyWrapper.compareTo(result.right) < 0);
              if (bList.length > 1) {
                assertEquals(
                    compareArray(aList, bList) > 0, Float.parseFloat(result.left) > newFloatValue);
                assertEquals(
                    compareArray(aList, bList) < 0, Float.parseFloat(result.left) < newFloatValue);
              } else {
                assertTrue(compareArray(aList, bList) > 0);
              }
              break;
            case DOUBLE:
              double newDoubleValue = ThreadLocalRandom.current().nextDouble();
              if (ThreadLocalRandom.current().nextDouble() < 0.7)
                copyWrapper.setWithNull(String.valueOf(newDoubleValue));
              else {
                if (copyWrapper.isNull) continue;
                newDoubleValue = Double.parseDouble(copyWrapper.toString());
              }
              assertEquals(
                  Double.compare(newDoubleValue, Double.parseDouble(result.left)) > 0,
                  copyWrapper.compareTo(result.right) > 0);
              assertEquals(
                  Double.compare(newDoubleValue, Double.parseDouble(result.left)) < 0,
                  copyWrapper.compareTo(result.right) < 0);

              if (bList.length > 1) {
                assertEquals(
                    compareArray(aList, bList) > 0,
                    Double.parseDouble(result.left) > newDoubleValue);
                assertEquals(
                    compareArray(aList, bList) < 0,
                    Double.parseDouble(result.left) < newDoubleValue);
              } else {
                assertTrue(compareArray(aList, bList) > 0);
              }
              break;
          }
        }
      }
    }
  }
}
