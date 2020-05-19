package beans;

import java.util.ArrayList;

public class SingleTable {

    public int insertSize;
    public String tableName;
    public String conn_str_name;
    public String loadTable;
    public ArrayList<ColumnInfo> columns;

    public SingleTable(){

    }

    public int getInsertSize() {
        return insertSize;
    }

    public String getTableName() {
        return tableName;
    }

    public String getConn_str_name() {
        return conn_str_name;
    }

    public String getLoadTable() {
        return loadTable;
    }

    public ArrayList<ColumnInfo> getColumns() {
        return columns;
    }
}
