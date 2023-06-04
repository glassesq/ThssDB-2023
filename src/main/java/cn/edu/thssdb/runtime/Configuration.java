package cn.edu.thssdb.runtime;

public class Configuration {

  public final int warningMemory = 4 * 1024 * 1024;
  public String testPath = "D:\\项目\\tmp";

  public String tablespacePath = testPath + "/" + "base";
  public String testPathRecover = testPath + "/" + "checkpoint";
  public String WALFilename = testPath + "/" + "WAL.log";
  public String DummyLogFilename = testPath + "/" + "DummyLog.log";
  public String DummyLogRecoverFilename = testPath + "/" + "DummyLogRecover.log";
  public String MetadataFilename = tablespacePath + "/" + "example.json";
  public int pageSize = 2 * 1024;
  public final int bufferSize = 30;

  public final int overallPageIndex = 0;
  public final int indexRootPageIndex = 1;
  public final int indexLeftmostLeafIndex = 2;

  /**
   * When {@code allow_implicit_transaction} is set to false, any statement without an explicit
   * 'begin transaction' will be rejected.
   */
  public boolean allow_implicit_transaction;

  /**
   * When {@code auto_commit} is set to true, after every single statement, a 'commit transaction'
   * statement will be automatically supplemented.
   */
  public boolean auto_commit;

  /**
   * When {@code serializable} is set to true, the database will use serializable isolation level.
   * Otherwise, the database will use read committed isolation level.
   */
  public boolean serializable;

  public final int maxCharsetLength = 4;

  public boolean useDummyLog;

  public boolean recoverFromDummyLog;

  public Configuration() {
    allow_implicit_transaction = true;
    auto_commit = true;
    serializable = false;
    useDummyLog = true;
    recoverFromDummyLog = false;
  }
}
