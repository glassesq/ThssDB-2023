package cn.edu.thssdb.runtime;

import cn.edu.thssdb.communication.IO;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.plan.impl.CreateDatabasePlan;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.utils.StatusUtil;

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

    /**
     * run plan inter the session.
     * create transaction if necessary.
     *
     * @param plan the plan to be executed.
     * @return executeStatementResponse
     */
    public ExecuteStatementResp runPlan(LogicalPlan plan) {
        try {
            if (transactionId < 0 && ServerRuntime.config.allow_implicit_transaction) {
                // automatically begin the transaction if allow_implicit_transaction is on.
                transactionId = ServerRuntime.newTransaction();
                IO.writeTransactionStart(transactionId);
            } else if (transactionId < 0) {
                return new ExecuteStatementResp(StatusUtil.fail("There is no active transaction now. Please begin a transaction first."), false);
            }
            ExecuteStatementResp response = null;
            System.out.println(plan); // For Test
            switch (plan.getType()) {
                case COMMIT:
                    IO.pushTransactionCommit(transactionId);
                    transactionId = -1;
                    // Commit statement shall be treated as the end of transaction. No matter it succeeds or not.
                    // TODO: If commit failed, the transaction shall enter its abort process.
                    return new ExecuteStatementResp(StatusUtil.success("The transaction has been successfully committed."), false);
                case CREATE_DB:
                    CreateDatabasePlan createDatabasePlan = (CreateDatabasePlan) plan;
                    if (Database.createDatabase(transactionId, createDatabasePlan.getDatabaseName()) == null)
                        response = new ExecuteStatementResp(StatusUtil.fail("Database " + createDatabasePlan.getDatabaseName() + " already existed."), false);
                    else
                        response = new ExecuteStatementResp(StatusUtil.success("Database " + createDatabasePlan.getDatabaseName() + " created."), false);
                    break;
                default:
            }
            if (ServerRuntime.config.auto_commit) {
                IO.pushTransactionCommit(transactionId);
                transactionId = -1;
                response.status.msg = response.status.msg + "\n\nEnd of the transaction.(auto commit on).";
            }
            if (response != null) return response;
            return new ExecuteStatementResp(StatusUtil.fail("Command not understood or implemented."), false);
        } catch (Exception e) {
            return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
    }

}
