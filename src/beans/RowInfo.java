package beans;

import java.util.ArrayList;

public class RowInfo {

    private ArrayList<ColumnInfo> columnInfos;
    private int rowSize;

    public ArrayList<ColumnInfo> getColumnInfos() {
        return columnInfos;
    }

    public int getRowSize() {
        return rowSize;
    }

    void setColumnInfos(ArrayList<ColumnInfo> columnInfos) {
        this.columnInfos = columnInfos;
    }

    void setRowSize(int rowSize) {
        this.rowSize = rowSize;
    }
}
