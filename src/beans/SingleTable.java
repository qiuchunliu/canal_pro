package beans;

import java.util.ArrayList;

public class SingleTable {

    public int insertSize;
    public String tableName;
    public String connStrName;
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

    public String getConnStrName() {
        return connStrName;
    }

    public String getLoadTable() {
        return loadTable;
    }

    public ArrayList<ColumnInfo> getColumns() {
        return columns;
    }
}
