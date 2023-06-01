package cn.edu.thssdb.benchmark.common;

import cn.edu.thssdb.benchmark.config.CommonConfig;

public class Constants {
  // 列数
  public static final int columnCount = 10;
  // 表数
  public static final int tableCount = 10;
  // 字符串长度
  public static final int stringLength = 5;
  public static final int SUCCESS_STATUS_CODE = CommonConfig.SUCCESS_CODE;
  // 列类型
  public static final DataType[] columnTypes = {
    DataType.INT, DataType.LONG, DataType.FLOAT, DataType.DOUBLE, DataType.STRING
  };
  // 行数
  public static final int rowCount = 10;
}
