package cn.edu.thssdb.benchmark.common;

import java.util.ArrayList;
import java.util.List;

public class PreparedStatement {

  private final String sql;
  private final List<Object> params;

  public PreparedStatement(String sql) {
    this.sql = sql;
    this.params = new ArrayList<>();
  }

  public PreparedStatement setString(int index, String value) {
    addParam(index, value);
    return this;
  }

  public PreparedStatement setInt(int index, int value) {
    addParam(index, value);
    return this;
  }

  private void addParam(int index, Object value) {
    int paramIndex = index;
    if (params.size() > paramIndex) {
      params.set(paramIndex, value);
    } else {
      params.add(paramIndex, value);
    }
  }

  public String getSQL() {
    String result = this.sql;
    for (int i = 0; i < params.size(); i++) {
      Object value = params.get(i);
      String paramStr = value.toString();
      result = result.replaceFirst("\\?", paramStr);
    }
    return result;
  }

  private static int countPlaceholders(String sql) {
    int count = 0;
    int pos = sql.indexOf("?");
    while (pos != -1) {
      count++;
      pos = sql.indexOf("?", pos + 1);
    }
    return count;
  }
}
