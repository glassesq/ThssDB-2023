package cn.edu.thssdb.schema;

import cn.edu.thssdb.communication.IO;
import cn.edu.thssdb.runtime.ServerRuntime;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.xml.crypto.Data;
import java.util.HashMap;

public class Database {
    // private String name;
    // ReentrantReadWriteLock lock;

    public static class DatabaseMetadata {
        public String name;
        public int databaseId;
        public HashMap<Integer, Table.TableMetadata> tables = new HashMap<>();
        JSONObject object;

        /**
         * create a new database metadata using existing JSONObject.
         * The lock is not required.
         *
         * @param object JSON object
         * @return new databaseMetadata
         * @throws Exception JSON error
         */
        public static DatabaseMetadata createDatabaseMetadata(JSONObject object) throws Exception {
            DatabaseMetadata metadata = new DatabaseMetadata();
            metadata.object = object;
            metadata.name = object.getString("databaseName");
            metadata.databaseId = object.getInt("databaseId");
            JSONArray tableArray = object.getJSONArray("tables");
            for (int i = 0; i < tableArray.length(); i++) {
                Table.TableMetadata tableMetadata = Table.TableMetadata.parse(tableArray.getJSONObject(i));
                metadata.tables.put(tableMetadata.spaceId, tableMetadata);
            }
            return metadata;
        }

        /**
         * This method create table, covering both data and metadata.
         * Proper changes shall be done to WAL buffer as well.
         * The lock is not required. Only the transaction requested this method can access tableMetadata now. It will not affect other tables.
         *
         * @param transactionId transactionId
         * @param tableMetadata tableMetadata
         * @throws Exception WAL error
         */
        public void createTable(long transactionId, Table.TableMetadata tableMetadata) throws Exception {
            tables.put(tableMetadata.spaceId, tableMetadata);
            object.getJSONArray("tables").put(tableMetadata.object);
            IO.writeCreateTable(transactionId, this.databaseId, tableMetadata);
            tableMetadata.initTablespaceFile(transactionId);
        }

    }

    public DatabaseMetadata metadata;

    public Database(DatabaseMetadata metadata) {
        this.metadata = metadata;
    }

    private void persist() {
        // TODO
    }

    /**
     * create database.
     * ServerRuntime{@code (databaseNameLookup, databaseMetadata, metadataArray)} will be automatically updated. Corresponding changes are recorded in WAL log buffer.
     * TODO: lock
     *
     * @param transactionId transactionId
     * @param name          name of the database
     * @return A Database Object
     * @throws Exception WAL error
     */
    public static Database createDatabase(long transactionId, String name) throws Exception {
        // TODO: lock for databaseMetadata
        if (ServerRuntime.databaseNameLookup.containsKey(name)) return null;
        Database database = new Database(new DatabaseMetadata());
        database.metadata.name = name;
        database.metadata.databaseId = ServerRuntime.newDatabase();
        database.metadata.tables = new HashMap<>();
        ServerRuntime.databaseMetadata.put(database.metadata.databaseId, database.metadata);
        database.metadata.object = new JSONObject();
        database.metadata.object.put("databaseName", name);
        database.metadata.object.put("databaseId", database.metadata.databaseId);
        database.metadata.object.put("tables", new JSONArray());
        ServerRuntime.metadataArray.put(database.metadata.object);
        ServerRuntime.databaseNameLookup.put(name, database.metadata.databaseId);
        IO.writeCreateDatabase(transactionId, name, database.metadata.databaseId);
        return database;
    }


    private void deleteDatabase() {
        // TODO
    }

    public void drop() {
        // TODO
    }

    public String select() {
        // TODO
        return null;
    }

    private void recover() {
        // TODO
    }

    public void quit() {
        // TODO
    }
}
