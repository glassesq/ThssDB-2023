package cn.edu.thssdb.runtime;

import cn.edu.thssdb.storage.writeahead.WriteLog;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.utils.StatusUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The runtime of the database server.
 * Every member variable and function shall be static in this class.
 */
public class ServerRuntime {


    /**
     * A map from {@code sessionId} to {@code SessionRuntime}
     */
    public static HashMap<Long, SessionRuntime> sessions = new HashMap<>();

    private static final AtomicLong sessionCounter = new AtomicLong(0);

    private static final AtomicLong transactionCounter = new AtomicLong(0);

    private static final AtomicInteger tablespaceCounter = new AtomicInteger(0);

    /**
     * Configuration of the whole server.
     */
    public static final Configuration config = new Configuration();

    public static final WriteLog writeLog = new WriteLog();

    /**
     * increase transaction_counter by one
     *
     * @return 8-byte new transaction id (unused).
     * @throws IllegalStateException the transaction counter is exhausted.
     */
    public static long newTransaction() throws IllegalStateException {
        long tid = transactionCounter.incrementAndGet();
        if (tid == Long.MAX_VALUE) {
            throw new IllegalStateException("The transaction counter is exhausted. Please restart the server. ");
        }
        return tid;
    }


    public static int newTablespace() throws IllegalStateException {
        // TODO: tablespace id can be reused
        int tid = tablespaceCounter.incrementAndGet();
        if (tid == Integer.MAX_VALUE) {
            throw new IllegalStateException("The tablespace id is exhausted. Out of capability.");
        }
        return tid;
    }

    /**
     * create a session.
     */
    public static long newSession() {
        long sessionId = sessionCounter.incrementAndGet();
        SessionRuntime sessionRuntime = new SessionRuntime();
        sessions.put(sessionId, sessionRuntime);
        return sessionId;
    }

    /**
     * close a session.
     *
     * @param sessionId for the session to be closed.
     */
    public static void closeSession(long sessionId) {
        SessionRuntime sessionRuntime = sessions.get(sessionId);
        if (sessionRuntime != null) sessionRuntime.stop();

        sessions.remove(sessionId);
    }


    /**
     * check if the session exists.
     *
     * @param sessionId the session.
     * @return true or false.
     */
    public static boolean checkForSession(long sessionId) {
        SessionRuntime sessionRuntime = sessions.get(sessionId);
        return sessionRuntime != null;
    }

    /**
     * get the absolute path of the tablespace file
     *
     * @param spaceId tablespace
     * @return absolute path of the tablespace file
     */
    public static String getTablespaceFile(int spaceId) {
        // TODO: read from metadata file.
        // TODO: REPLACE FOR TEST
        return "/Users/rongyi/Desktop/tablespace1.tablespace";
    }

    /**
     * run plan in the session.
     *
     * @param sessionId the session.
     * @param plan      the logical plan.
     * @return executeStatementResp.
     */
    public static ExecuteStatementResp runPlan(long sessionId, LogicalPlan plan) {
        SessionRuntime sessionRuntime = sessions.get(sessionId);
        if (sessionRuntime == null) {
            return new ExecuteStatementResp(StatusUtil.fail("SessionRuntime does not exist for session" + sessionId + ". Uncommitted actions shall be automatically aborted. Please connect to the server again."), false);
        }
        return sessionRuntime.runPlan(plan);
    }


}
