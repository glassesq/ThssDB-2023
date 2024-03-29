package cn.edu.thssdb.schema;

import cn.edu.thssdb.runtime.ServerRuntime;
import cn.edu.thssdb.type.DataType;
import org.json.JSONObject;

import java.util.regex.Pattern;

public class Column {

  private String name;

  public DataType type;
  public int primary = -1;
  private boolean notNull = false;
  private int length;
  public boolean offPage = false;

  JSONObject object;

  public String getName() {
    return name;
  }

  public String toString() {
    return "name: "
        + name
        + "type: "
        + type
        + " primaryKey: "
        + primary
        + " notNull: "
        + notNull
        + " length: "
        + length
        + " \n";
  }

  public Column() {}

  /**
   * prepare myself with name, type, length JSON object will be created in this method.
   *
   * @param name name of the column
   * @param type DataType of the column
   * @param length Length of the data. Only used when type is STRING
   */
  public void prepare(String name, DataType type, int length) {
    this.name = name;
    this.object = new JSONObject();
    this.object.put("columnName", name);
    this.type = type;
    this.object.put("type", dataType2Str(type));
    if (this.type == DataType.STRING) {
      this.length = length;
      this.object.put("length", length);
    }
  }

  public boolean isNotNull() {
    return primary >= 0 || notNull;
  }

  public static Column parse(JSONObject object) throws Exception {
    Column column = new Column();
    column.name = object.getString("columnName");
    column.object = object;
    column.type = str2DataType(object.getString("type"));
    if (column.type == DataType.STRING) column.length = object.getInt("length");
    column.primary = object.getInt("primaryKey");
    if (object.has("notNull") && object.getBoolean("notNull")) column.notNull = true;
    return column;
  }

  public static DataType str2DataType(String strType) {
    switch (strType.toLowerCase()) {
      case "string":
        return DataType.STRING;
      case "int":
        return DataType.INT;
      case "long":
        return DataType.LONG;
      case "float":
        return DataType.FLOAT;
      case "double":
        return DataType.DOUBLE;
    }
    if (Pattern.matches("string\\([0-9]+\\)", strType.toLowerCase())) {
      return DataType.STRING;
    }
    return DataType.INT;
  }

  public static String dataType2Str(DataType dataType) {
    switch (dataType) {
      case STRING:
        return "String";
      case INT:
        return "Int";
      case LONG:
        return "Long";
      case FLOAT:
        return "Float";
      case DOUBLE:
        return "Double";
    }
    return "NotImplemented";
  }

  public void setConstraint(String constraint) {
    switch (constraint.toLowerCase()) {
      case "notnull":
        this.notNull = true;
        this.object.put("notNull", true);
    }
  }

  public void setPrimaryKey(int order) {
    this.primary = order;
    this.object.put("primaryKey", order);
    if (order >= 0) setConstraint("notnull");
  }

  public int getLength() {
    switch (this.type) {
      case STRING:
        return length * ServerRuntime.config.maxCharsetLength;
      case INT:
      case FLOAT:
        return 4;
      case LONG:
      case DOUBLE:
        return 8;
    }
    return 0;
  }

  public int getStringLength() {
    return length;
  }
}
