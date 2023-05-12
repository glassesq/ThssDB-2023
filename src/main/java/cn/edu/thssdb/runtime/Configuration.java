package cn.edu.thssdb.runtime;

public class Configuration {

    public final int pageSize = 16 * 1024;

    /**
     * When {@code allow_implicit_transaction} is set to false,
     * any statement without an explicit 'begin transaction' will be rejected.
     */
    public boolean allow_implicit_transaction;

    /**
     * When {@code auto_commit} is set to true,
     * after every single statement, a 'commit transaction' statement will be automatically supplemented.
     */
    public boolean auto_commit;

    public Configuration() {
        allow_implicit_transaction = false;
        auto_commit = false;

        /* conflict test */
        // TODO: assert (!(allow_implicit_transaction && auto_commit));
        /* auto_commit cannot be used when implicit_transaction is allowed */
    }

}
