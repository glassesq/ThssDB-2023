package cn.edu.thssdb.schema;

import cn.edu.thssdb.runtime.ServerRuntime;
import cn.edu.thssdb.storage.page.IndexPage;
import cn.edu.thssdb.storage.page.OverallPage;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Table class.
 * The lifetime of this {@code Table} class object shall be only within the transaction that requested the Table.
 * Multiple transactions may run on same Table. They obtain different Table Object which share the same TableMetadata.
 * The shared TableMetadata Object is under the control of ServerRuntime.
 */
public class Table {
    //    ReentrantReadWriteLock lock;

    public static class TableMetadata {
        // TODO: Lock is allocated at the unit of transaction.

        /**
         * spaceId is a positive integer.
         */
        public int spaceId;
        public String name;
        public String tablespaceFilename;

        /**
         * if the table is temporary (e.g. may be created for join operation. )
         */
        public boolean temporary = false;

        /**
         * if the table is inited (The relevant tablespace file is already in disk buffer or disk.)
         */
        public boolean inited = false;
        public ArrayList<String> columns = new ArrayList<>();
        public HashMap<String, Column> columnDetails = new HashMap<>();

        public JSONObject object;
        public JSONArray columnObjectArray;

        public String columnInfo() {
            StringBuilder buffer = new StringBuilder();
            for (String column : columns) {
                buffer.append(column);
                buffer.append(',');
            }
            return buffer.toString();
        }

        public int getPrimaryKeyLength() {
            int count = 0;
            for (String column : columns) {
                if (columnDetails.get(column).primary >= 0) {
                    count += columnDetails.get(column).getLength();
                }
            }
            return count;
        }

        public int getNonPrimaryKeyLength() {
            int count = 0;
            for (String column : columns) {
                if (columnDetails.get(column).primary < 0) {
                    count += columnDetails.get(column).getLength();
                }
            }
            return count;
        }

        /**
         * get the length of nullBitmap for this table
         *
         * @return number of bytes
         */
        public int getNullBitmapLengthInByte() {
            int count = 0;
            for (String column : columns) {
                if (!columnDetails.get(column).notNull) {
                    count++;
                }
            }
            /* ceil */
            return ((count + 7) / 8);
        }

        /**
         * Prepare a table metadata object with name, spaceId, tablespaceFilename.
         * This table currently does not exist on disk. Therefore, {@code inited} is set to false.
         * Proper tablespace filename will be allocated.
         *
         * @param name    table name
         * @param spaceId tablespaceId
         */
        public void prepare(String name, int spaceId) {
            this.object = new JSONObject();
            this.columnObjectArray = new JSONArray();
            this.object.put("columns", this.columnObjectArray);
            this.name = name;
            this.object.put("tableName", name);
            this.spaceId = spaceId;
            this.object.put("tablespaceId", spaceId);
            this.tablespaceFilename = ServerRuntime.getTablespaceFile(spaceId);
            this.object.put("tablespaceFile", tablespaceFilename);
            temporary = false; // TODO: temporary table.
            inited = false;
        }

        public static TableMetadata parse(JSONObject object) throws Exception {
            TableMetadata metadata = new TableMetadata();
            metadata.object = object;
            metadata.name = object.getString("tableName");
            metadata.spaceId = object.getInt("tablespaceId");
            metadata.tablespaceFilename = object.getString("tablespaceFile");

            metadata.columnObjectArray = object.getJSONArray("columns");
            for (int i = 0; i < metadata.columnObjectArray.length(); i++) {
                Column column = Column.parse(metadata.columnObjectArray.getJSONObject(i));
                metadata.columnDetails.put(column.name, column);
                metadata.columns.add(column.name);
            }

            /* since the tableMetadata is formed according to an existed json object, it must be on disk. */
            metadata.temporary = false;
            metadata.inited = true;
            return metadata;

        }


        /**
         * init this tablespace on disk (buffer). Set up file format and necessary information. Both Metadata and Tablespace Data.
         * A suitable tablespaceId will be allocated automatically.
         *
         * @throws Exception init failed.
         */
        public void initTablespaceFile(long transactionId) throws Exception {
            /* Tablespace File Creation */
            String tablespaceFilename = ServerRuntime.getTablespaceFile(spaceId);
            System.out.println(tablespaceFilename);

            File tablespaceFile = new File(tablespaceFilename);
            tablespaceFile.createNewFile();
            if (!tablespaceFile.exists()) {
                throw new Exception("create tablespace file failed.");
            }

            OverallPage.createOverallPage(transactionId, spaceId, 0, false);
            System.out.println("overall page over.");
            IndexPage.createIndexPage(transactionId, spaceId, 2, false);
            System.out.println("index root page over.");
        }

        public void addColumn(Column column) {
            this.columns.add(column.name);
            this.columnDetails.put(column.name, column);
            columnObjectArray.put(column.object);
        }
    }

    public TableMetadata metadata;

    public Table(TableMetadata metadata) {
        this.metadata = metadata;
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


}
