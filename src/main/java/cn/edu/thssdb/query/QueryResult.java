package cn.edu.thssdb.query;

import java.util.ArrayList;
import java.util.HashMap;

public class QueryResult {
  public ArrayList<String> columns;
  public ArrayList<ArrayList<String>> rows;

  public QueryResult() {
    rows = new ArrayList<>();
    columns = new ArrayList<>();
  }
}
