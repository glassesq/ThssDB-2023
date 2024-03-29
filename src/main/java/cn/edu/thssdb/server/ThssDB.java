package cn.edu.thssdb.server;

import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.runtime.ServerRuntime;
import cn.edu.thssdb.service.IServiceHandler;
import cn.edu.thssdb.utils.Global;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThssDB {

  private static final Logger logger = LoggerFactory.getLogger(ThssDB.class);
  private static IServiceHandler handler;
  private static IService.Processor processor;
  private static TServerSocket transport;
  private static TServer server;

  public static ThssDB getInstance() {
    return ThssDBHolder.INSTANCE;
  }

  public static void main(String[] args) {
    ThssDB server = ThssDB.getInstance();
    server.start();
  }

  private void start() {
    handler = new IServiceHandler();
    processor = new IService.Processor(handler);
    /* setup server runtime */
    try {
      if (ServerRuntime.config.recoverFromDummyLog) ServerRuntime.recoverFromDummyLog();
      else ServerRuntime.setup();
    } catch (Exception e) {
      System.out.println("The server cannot start for some unexpected errors." + e.getMessage());
      return;
    }
    Runnable setup = () -> setUp(processor);
    new Thread(setup).start();
  }

  private static void setUp(IService.Processor processor) {
    try {
      transport = new TServerSocket(Global.DEFAULT_SERVER_PORT);
      server = new TThreadPoolServer(new TThreadPoolServer.Args(transport).processor(processor));
      logger.info("Starting ThssDB ...");
      server.serve();
    } catch (TTransportException e) {
      logger.error(e.getMessage());
    }
  }

  private static class ThssDBHolder {
    private static final ThssDB INSTANCE = new ThssDB();

    private ThssDBHolder() {}
  }
}
