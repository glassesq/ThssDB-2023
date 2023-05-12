package cn.edu.thssdb.service;

import cn.edu.thssdb.plan.LogicalGenerator;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.rpc.thrift.ConnectReq;
import cn.edu.thssdb.rpc.thrift.ConnectResp;
import cn.edu.thssdb.rpc.thrift.DisconnectReq;
import cn.edu.thssdb.rpc.thrift.DisconnectResp;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementReq;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.rpc.thrift.GetTimeReq;
import cn.edu.thssdb.rpc.thrift.GetTimeResp;
import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.rpc.thrift.Status;
import cn.edu.thssdb.runtime.ServerRuntime;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.StatusUtil;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class IServiceHandler implements IService.Iface {


    @Override
    public GetTimeResp getTime(GetTimeReq req) throws TException {
        GetTimeResp resp = new GetTimeResp();
        resp.setTime(new Date().toString());
        resp.setStatus(new Status(Global.SUCCESS_CODE));
        return resp;
    }

    @Override
    public ConnectResp connect(ConnectReq req) throws TException {
        long sid = ServerRuntime.newSession();
        return new ConnectResp(StatusUtil.success(), sid);
    }

    @Override
    public DisconnectResp disconnect(DisconnectReq req) throws TException {
        ServerRuntime.closeSession(req.sessionId);
        return new DisconnectResp(StatusUtil.success());
    }

    @Override
    public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {


        /* ONLY for Test */
        Table table = new Table("xxx", "yyy", new Column[0]);
        try {
            table.initTablespace();
        } catch (Exception e) {
            System.out.println(e);
        }
        System.out.println("stop.");
        return new ExecuteStatementResp(StatusUtil.fail("stop twice."), false);

/*

        if (ServerRuntime.checkForSession(req.getSessionId())) {
            return new ExecuteStatementResp(StatusUtil.fail("You are not connected. Please connect first."), false);
        }

        LogicalPlan plan = LogicalGenerator.generate(req.statement);
        return ServerRuntime.runPlan(req.getSessionId(), plan);

*/
    }
}
