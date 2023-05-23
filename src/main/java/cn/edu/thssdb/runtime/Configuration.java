package cn.edu.thssdb.runtime;

public class Configuration {

  public String testPath = "D:\\项目\\tmp";

  public String MetadataFilename = testPath + "/" + "example.json";
  public String WALFilename = testPath + "/" + "WAL.log";

  public int pageSize = 16 * 1024;

  public final int overallPageIndex = 0;
  public final int indexRootPageIndex = 2;

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

  public final int maxCharsetLength = 4;

  public Configuration() {
    allow_implicit_transaction = true;
    auto_commit = true;

    /* conflict test */
    // TODO: assert (!(allow_implicit_transaction && auto_commit));
    /* auto_commit cannot be used when implicit_transaction is allowed */
  }
}
