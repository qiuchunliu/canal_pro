package beans;

import java.util.ArrayList;

public class RowInfo {

    private ArrayList<ColumnInfo> columnInfos;
    private int rowSize;
    private String updatedCols;  // 记录哪些字段更新过

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

    public String getUpdatedCols() {
        return updatedCols;
    }

    void setUpdatedCols(String updatedCols) {
        this.updatedCols = updatedCols;
    }
}
