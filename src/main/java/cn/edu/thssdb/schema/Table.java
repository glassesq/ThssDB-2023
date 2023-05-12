package cn.edu.thssdb.schema;

import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.runtime.ServerRuntime;
import cn.edu.thssdb.utils.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Table class.
 * The lifetime of this {@code Table} class object shall be only within the transaction that requested the Table.
 */
public class Table implements Iterable<Row> {
  ReentrantReadWriteLock lock;
  private String databaseName;
  public String tableName;
  public ArrayList<Column> columns;
  public BPlusTree<Entry, Row> index;
  private int primaryIndex;

  /**
   * tablespace Id.
   */
  public int spaceId;

  /**
   * if the table is temporary (e.g. may be created for join operation. )
   */
  public boolean temporary = false;

  /**
   * if the table is inited (The relevant tablespace file is already in disk buffer or disk.)
   */
  public boolean inited = false;

  /**
   * load metadata of the table from Server Runtime.
   * @param spaceId tablespace Id;
   */
  public void loadMetadata(int spaceId) {
    this.spaceId = spaceId;
    // TODO: read info from system
    inited = true;
  }

  /**
   * load tablespace information from tablespace file.
   * @param spaceId tablespace Id;
   */
  public void loadTableInfo(int spaceId) {
    this.spaceId = spaceId;
    // TODO: read information from tablespace file.
    inited = true;
  }

  /**
   * init this tablespace on disk (buffer). Set up file format and necessary information. Both Metadata and Tablespace Data.
   * A suitable tablespaceId will be allocated automatically.
   * @throws Exception init failed.
   */
  public void initTablespace() throws Exception {
    this.spaceId = ServerRuntime.newTablespace();
    /* Tablespace File Creation */

    /* Metadata File Modification */
    // TODO: a log system for shall also be introduced.
  }

  public Table(String databaseName, String tableName, Column[] columns) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.columns = new ArrayList<>(Arrays.asList(columns));
  }
  private void recover() {
    // TODO
  }

  public void insert() {
    // TODO
  }

  public void delete() {
    // TODO
  }

  public void update() {
    // TODO
  }

  private void serialize() {
    // TODO
  }

  private ArrayList<Row> deserialize() {
    // TODO
    return null;
  }

  private class TableIterator implements Iterator<Row> {
    private Iterator<Pair<Entry, Row>> iterator;

    TableIterator(Table table) {
      this.iterator = table.index.iterator();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Row next() {
      return iterator.next().right;
    }
  }

  @Override
  public Iterator<Row> iterator() {
    return new TableIterator(this);
  }

}
