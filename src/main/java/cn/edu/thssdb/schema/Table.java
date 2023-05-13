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

        public String columnInfo() {
            StringBuilder buffer = new StringBuilder();
            for (String column : columns) {
                buffer.append(column);
                buffer.append(',');
            }
            return buffer.toString();
        }

        public static TableMetadata parse(JSONObject object) throws Exception {
            TableMetadata metadata = new TableMetadata();
            metadata.object = object;
            metadata.name = object.getString("tableName");
            metadata.spaceId = object.getInt("tablespaceId");
            metadata.tablespaceFilename = object.getString("tablespaceFile");

            JSONArray columnArrays = object.getJSONArray("columns");
            for (int i = 0; i < columnArrays.length(); i++) {
                Column column = Column.parse(columnArrays.getJSONObject(i));
                metadata.columnDetails.put(column.name, column);
                metadata.columns.add(column.name);
            }

            /* since the tableMetadata is formed according to an existed json object, it must be on disk. */
            metadata.temporary = false;
            metadata.inited = true;
            return metadata;

        }
    }

    public TableMetadata metadata = new TableMetadata();

    /**
     * init this tablespace on disk (buffer). Set up file format and necessary information. Both Metadata and Tablespace Data.
     * A suitable tablespaceId will be allocated automatically.
     *
     * @throws Exception init failed.
     */
    public void initTablespace(long transactionId) throws Exception {
        this.metadata.spaceId = ServerRuntime.newTablespace();

        /* Tablespace File Creation */
        String tablespaceFilename = ServerRuntime.config.testPath + "/tablespace" + metadata.spaceId + ".tablespace";
        System.out.println(tablespaceFilename);

        File tablespaceFile = new File(tablespaceFilename);
        tablespaceFile.createNewFile();
        if (!tablespaceFile.exists()) {
            throw new Exception("create tablespace file failed.");
        }

        OverallPage overallPage = new OverallPage(transactionId, this.metadata.spaceId, 0, false);
        System.out.println("overall page over.");
        IndexPage indexRootPage = new IndexPage(transactionId, this.metadata.spaceId, 2, false);
        System.out.println("index root page over.");

        /* These two outputs are only for test. */
        // IO.pushWALAndPages();
        /* Metadata File Modification */
        // TODO: a log system for shall also be introduced.
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
