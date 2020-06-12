package beans;

import java.util.ArrayList;

public class SingleTable {

    private int insertSize;
    private String tableName;
    private String connStrName;
    private String loadTable;
    private ArrayList<ColumnInfo> columns;
    private String rowConditions;

    public void setInsertSize(int insertSize) {
        this.insertSize = insertSize;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setConnStrName(String connStrName) {
        this.connStrName = connStrName;
    }

    public void setLoadTable(String loadTable) {
        this.loadTable = loadTable;
    }

    public void setColumns(ArrayList<ColumnInfo> columns) {
        this.columns = columns;
    }

    public void setRowConditions(String rowConditions) {
        this.rowConditions = rowConditions;
    }

    public SingleTable(){

    }

    public String getRowConditions() {
        return rowConditions;
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
