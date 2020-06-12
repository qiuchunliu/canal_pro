package beans;

public class ColumnInfo {
    private int  index;
    private int  sqlType;
    private String  name;
    private boolean  isKey;
    private boolean  updated;
    private boolean  isNull;
    private String  value;
    private String  mysqlType;
    private String colType;
    private String toCol;

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getSqlType() {
        return sqlType;
    }

    void setSqlType(int sqlType) {
        this.sqlType = sqlType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isKey() {
        return isKey;
    }

    public void setKey(boolean key) {
        isKey = key;
    }

    public boolean isUpdated() {
        return updated;
    }

    public void setUpdated(boolean updated) {
        this.updated = updated;
    }

    public boolean isNull() {
        return isNull;
    }

    public void setNull(boolean aNull) {
        isNull = aNull;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getMysqlType() {
        return mysqlType;
    }

    void setMysqlType(String mysqlType) {
        this.mysqlType = mysqlType;
    }

    public String getColType() {
        return colType;
    }

    public void setColType(String colType) {
        this.colType = colType;
    }

    public String getToCol() {
        return toCol;
    }

    public void setToCol(String toCol) {
        this.toCol = toCol;
    }
}
