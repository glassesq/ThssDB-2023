package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.DataType;
import org.json.JSONObject;

import javax.print.DocFlavor;
import javax.xml.crypto.Data;

public class Column {

    public String name;
    public DataType type;
    // TODO: multiple primary keys
    public int primary = 0;
    public boolean notNull = false;
    public int length;

    JSONObject object;

    public String toString() {
        return name + ',' + type + ',' + primary + ',' + notNull + ',' + length;
    }

    public static Column parse(JSONObject object) throws Exception {
        Column column = new Column();
        column.object = object;
        column.name = object.getString("columnName");

        String strType = object.getString("type");
        switch (strType) {
            case "String":
                column.type = DataType.STRING;
                break;
            case "Int":
                column.type = DataType.INT;
                break;
            case "Long":
                column.type = DataType.LONG;
                break;
            case "Float":
                column.type = DataType.FLOAT;
                break;
            case "Double":
                column.type = DataType.DOUBLE;
                break;
            default:
                throw new Exception("Unknown data type");
        }

        if (column.type == DataType.STRING) column.length = object.getInt("length");
        if (object.has("primaryKey") && object.getBoolean("primaryKey")) column.primary = 1;
        if (object.has("notNull") && object.getBoolean("notNull")) column.notNull = true;
        return column;
    }


}
