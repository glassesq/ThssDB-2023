package cn.edu.thssdb.runtime;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.utils.StatusUtil;

import java.util.Date;

public class SessionRuntime {
    /* The runtime of one session. */

    /**
     * current database under the session's use.
     */
    public String database = null;

    /* ********************* transaction ************************ */
    /**
     * current 8-byte transaction id under the session's use
     */
    public long transactionId = -1;


    /* ********************* transaction ************************ */

    /**
     * stop the session.
     */
    public void stop() {
        // TODO
    }

    public ExecuteStatementResp runPlan(LogicalPlan plan) {
        try {
            if (ServerRuntime.config.allow_implicit_transaction) {
                if (transactionId < 0) {
                    transactionId = ServerRuntime.newTransaction();
                }
            }
            switch (plan.getType()) {
                case CREATE_DB:
                    System.out.println("[DEBUG][" + new Date() + "] " + plan);
                    break;
                case COMMIT:
                    commit();
                    return new ExecuteStatementResp(StatusUtil.success("The transaction has been successfully committed."), false);
                default:
                /*
                ExecuteStatementResp resp =  new ExecuteStatementResp(StatusUtil.success(), true);
                resp.addToColumnsList("string1");
                resp.addToColumnsList("string2");
                resp.addToRowList(new ArrayList<>(Arrays.asList("a", "b")));
                return resp;
                 */
            }
            return new ExecuteStatementResp(StatusUtil.fail("Command not understood or implemented."), false);
        } catch (Exception e) {
            return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
    }

    public void beginTransaction() throws Exception {

    }

    public void commit() throws Exception {
        if (transactionId < 0) {
            throw new Exception("There is no active transaction now. Commit failed.");
        }
    }
}
