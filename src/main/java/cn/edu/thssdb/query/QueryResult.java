package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.ValueWrapper;
import cn.edu.thssdb.utils.Cell;

import javax.management.Query;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class QueryResult {
    HashMap<String, Integer> index;
    public ArrayList<String> columns;
    public ArrayList<ArrayList<ValueWrapper>> rows;
    public QueryResult() {
        rows = new ArrayList<>();
        columns = new ArrayList<>();
    }

    public void addToRows(ArrayList<ValueWrapper> row) {
        this.rows.add(row);
    }


}