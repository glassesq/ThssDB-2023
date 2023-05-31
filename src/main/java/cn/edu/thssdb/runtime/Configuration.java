package cn.edu.thssdb.runtime;

public class Configuration {

  public String testPath = "/Users/rongyi/Desktop/metadata";

  public String MetadataFilename = testPath + "/" + "example.json";
  public String WALFilename = testPath + "/" + "WAL.log";
  public String DummyLogFilename = testPath + "/" + "DummyLog.log";

  public int pageSize = 2 * 1024;

  public final int overallPageIndex = 0;
  public final int indexRootPageIndex = 2;

  public final int indexLeftmostLeafIndex = 3;

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

  public boolean useDummyLog = false;

  public Configuration() {
    allow_implicit_transaction = true;
    auto_commit = true;
    serializable = false;
    useDummyLog = true;

    /* conflict test */
    // TODO: assert (!(allow_implicit_transaction && auto_commit));
    /* auto_commit cannot be used when implicit_transaction is allowed */
  }
}
