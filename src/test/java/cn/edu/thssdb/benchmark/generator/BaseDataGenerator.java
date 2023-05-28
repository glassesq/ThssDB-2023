package cn.edu.thssdb.benchmark.generator;

import cn.edu.thssdb.benchmark.common.Constants;
import cn.edu.thssdb.benchmark.common.TableSchema;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public abstract class BaseDataGenerator {

  protected String stringFormat = "'%0" + Constants.stringLength + "d'";
  protected Map<String, TableSchema> schemaMap = new HashMap<>();

  public BaseDataGenerator() {
    initTableSchema();
  }

  protected abstract void initTableSchema();

  public abstract Object generateValue(String tableName, int rowId, int columnId);

  public TableSchema getTableSchema(String tableName) {
    return schemaMap.get(tableName);
  }

  public Map<String, TableSchema> getSchemaMap() {
    return schemaMap;
  }

  /**
   * Pair is a template class to represent a couple of values. It also override the Object basic
   * methods like hasnCode, equals and toString.
   *
   * @param <L> L type
   * @param <R> R type
   */
  public class Pair<L, R> implements Serializable {

    private static final long serialVersionUID = -1398609631703707002L;
    public L left;
    public R right;

    public Pair(L l, R r) {
      left = l;
      right = r;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((left == null) ? 0 : left.hashCode());
      result = prime * result + ((right == null) ? 0 : right.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      Pair<?, ?> other = (Pair<?, ?>) obj;
      if (left == null) {
        if (other.left != null) {
          return false;
        }
      } else if (!left.equals(other.left)) {
        return false;
      }
      if (right == null) {
        return other.right == null;
      } else return right.equals(other.right);
    }

    @Override
    public String toString() {
      return "<" + left + "," + right + ">";
    }

    public L getLeft() {
      return left;
    }
  }
}
