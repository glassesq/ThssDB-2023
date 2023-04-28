package cn.edu.thssdb.client;

import cn.edu.thssdb.rpc.thrift.ConnectReq;
import cn.edu.thssdb.rpc.thrift.ConnectResp;
import cn.edu.thssdb.rpc.thrift.DisconnectReq;
import cn.edu.thssdb.rpc.thrift.DisconnectResp;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementReq;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.rpc.thrift.GetTimeReq;
import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.utils.Global;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.List;
import java.util.Scanner;

public class Client {

  private static final Logger logger = LoggerFactory.getLogger(Client.class);

  static final String HOST_ARGS = "h";
  static final String HOST_NAME = "host";

  static final String HELP_ARGS = "help";
  static final String HELP_NAME = "help";

  static final String PORT_ARGS = "p";
  static final String PORT_NAME = "port";

  private static long sessionID = -1;

  private static final PrintStream SCREEN_PRINTER = new PrintStream(System.out);
  private static final Scanner SCANNER = new Scanner(System.in);

  private static IService.Client client;

  public static void main(String[] args) {
    CommandLine commandLine = parseCmd(args);
    if (commandLine.hasOption(HELP_ARGS)) {
      showHelp();
      return;
    }
    try {
      echoStarting();
      String host = commandLine.getOptionValue(HOST_ARGS, Global.DEFAULT_SERVER_HOST);
      int port =
          Integer.parseInt(
              commandLine.getOptionValue(PORT_ARGS, String.valueOf(Global.DEFAULT_SERVER_PORT)));
      TTransport transport = new TSocket(host, port);
      transport.open();
      TProtocol protocol = new TBinaryProtocol(transport);
      client = new IService.Client(protocol);
      boolean open = true;
      while (true) {
        print(Global.CLI_PREFIX);
        String msg = SCANNER.nextLine().trim();
        long startTime = System.currentTimeMillis();
        if (msg.startsWith(Global.CONNECT)) {
          if (msg.endsWith(";")) {
            msg = msg.substring(0, msg.length() - 1);
          }
          String[] inputs = msg.split("\\s+");
          if (inputs.length < 3) {
            System.out.println("connect [username] [password];");
          } else {
            connect(inputs[1], inputs[2]);
          }
        } else if (msg.equals(Global.DISCONNECT)) {
          disconnect();
        } else if (msg.equals(Global.SHOW_TIME)) {
          getTime();
        } else if (msg.equals(Global.QUIT)) {
          disconnect();
          open = false;
        } else {
          execute(msg);
        }
        long endTime = System.currentTimeMillis();
        println("It costs " + (endTime - startTime) + " ms.");
        if (!open) {
          break;
        }
      }
      transport.close();
    } catch (TTransportException e) {
      logger.error(e.getMessage());
    }
  }

  private static void getTime() {
    GetTimeReq req = new GetTimeReq();
    try {
      println(client.getTime(req).getTime());
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  private static void execute(String statement) {
    ExecuteStatementReq req = new ExecuteStatementReq();
    req.setSessionId(sessionID);
    req.setStatement(statement);
    try {
      ExecuteStatementResp resp = client.executeStatement(req);
      if (resp.status.code == Global.SUCCESS_CODE) {
        if (resp.hasResult) {
          StringBuilder column_str = new StringBuilder();
          int column_size = resp.columnsList.size();
          for (int i = 0; i < column_size; ++i) {
            column_str.append(resp.columnsList.get(i));
            if (i != column_size - 1) column_str.append(", ");
          }
          println(column_str.toString());
          println("----------------------------------------------------------------");

          for (List<String> row : resp.rowList) {
            StringBuilder row_str = new StringBuilder();
            for (int i = 0; i < column_size; ++i) {
              row_str.append(row.get(i));
              if (i != column_size - 1) row_str.append(", ");
            }
            println(row_str.toString());
          }
        } else {
          println(resp.status.getMsg());
        }
      } else if (resp.status.code == Global.FAILURE_CODE) {
        println(resp.status.getMsg());
      }
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  private static void connect(String username, String password) {
    ConnectReq req = new ConnectReq();
    req.setUsername(username);
    req.setPassword(password);
    try {
      ConnectResp resp = client.connect(req);
      if (resp.status.code == Global.SUCCESS_CODE) {
        sessionID = resp.getSessionId();
        println(resp.status.getMsg());
      } else if (resp.status.code == Global.FAILURE_CODE) {
        sessionID = -1;
        println(resp.status.getMsg());
      }
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  private static void disconnect() {
    if (sessionID < 0) {
      println("you're not connected. plz connect first.");
      return;
    }
    DisconnectReq req = new DisconnectReq();
    req.setSessionId(sessionID);
    try {
      DisconnectResp resp = client.disconnect(req);
      if (resp.status.code == Global.SUCCESS_CODE) {
        sessionID = -1;
        println(resp.status.getMsg());
      } else if (resp.status.code == Global.FAILURE_CODE) {
        println(resp.status.getMsg());
      }
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  private static Options createOptions() {
    Options options = new Options();
    options.addOption(
        Option.builder(HELP_ARGS)
            .argName(HELP_NAME)
            .desc("Display help information(optional)")
            .hasArg(false)
            .required(false)
            .build());
    options.addOption(
        Option.builder(HOST_ARGS)
            .argName(HOST_NAME)
            .desc("Host (optional, default 127.0.0.1)")
            .hasArg(false)
            .required(false)
            .build());
    options.addOption(
        Option.builder(PORT_ARGS)
            .argName(PORT_NAME)
            .desc("Port (optional, default 6667)")
            .hasArg(false)
            .required(false)
            .build());
    return options;
  }

  private static CommandLine parseCmd(String[] args) {
    Options options = createOptions();
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      logger.error(e.getMessage());
      println("Invalid command line argument!");
      System.exit(-1);
    }
    return cmd;
  }

  private static void showHelp() {
    // TODO
    println("DO IT YOURSELF");
  }

  private static void echoStarting() {
    println("----------------------");
    println("Starting ThssDB Client");
    println("----------------------");
  }

  private static void print(String msg) {
    SCREEN_PRINTER.print(msg);
  }

  private static void println() {
    SCREEN_PRINTER.println();
  }

  private static void println(String msg) {
    SCREEN_PRINTER.println(msg);
  }
}
