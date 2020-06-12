package beans;

import java.util.ArrayList;

public class RowInfo {

    // list of columns
    private ArrayList<ColumnInfo> columnInfos;
    private int rowSize;
    // string of columns updated, separated by ','
    private String updatedCols;

    void setColumnInfos(ArrayList<ColumnInfo> columnInfos) {

        this.columnInfos = columnInfos;
    }

    public ArrayList<ColumnInfo> getColumnInfos() {
        return columnInfos;
    }

    void setRowSize(int rowSize) {
        this.rowSize = rowSize;
    }

    public int getRowSize() {
        return rowSize;
    }

    void setUpdatedCols(String updatedCols) {
        this.updatedCols = updatedCols;
    }

    public String getUpdatedCols() {
        return updatedCols;
    }

}
