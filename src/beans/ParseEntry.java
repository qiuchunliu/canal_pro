package beans;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;


public class ParseEntry {

    private String tableName;
    private long logfileOffset;
    private String databaseName;
    private String eventType;
    private long executeTime;
    private ArrayList<RowInfo> entryList;
    private static Logger log = Logger.getLogger(ParseEntry.class);


    /**
     * parse entry whose type is insert or update
     * @param entry entry to be parsed
     */
    public ParseEntry(CanalEntry.Entry entry){

        // entry 所在的表名
        this.tableName = entry.getHeader().getTableName();
        // binlog的position
        this.logfileOffset = entry.getHeader().getLogfileOffset();
        // entry所在的库名
        this.databaseName = entry.getHeader().getSchemaName();
        // 操作事件，insert、update、delete
        this.eventType = entry.getHeader().getEventType().toString();
        // 事务的执行时间
        this.executeTime = entry.getHeader().getExecuteTime();

        try {
            List<CanalEntry.RowData> rowDatas = CanalEntry.RowChange.parseFrom(entry.getStoreValue()).getRowDatasList();
            ArrayList<RowInfo> entryList = new ArrayList<>();
            for (CanalEntry.RowData rd : rowDatas){
                RowInfo rowInfo = new RowInfo();
                rowInfo.setRowSize(rd.getSerializedSize());
                // 每个rowData为一条记录
                ArrayList<ColumnInfo> columnList = new ArrayList<>();
                StringBuilder updatedCols = new StringBuilder();  // 更新过的字段
                List<CanalEntry.Column> afterColumnsList = rd.getAfterColumnsList();
                for (CanalEntry.Column col : afterColumnsList){
                    ColumnInfo tc = new ColumnInfo();
                    tc.setName(col.getName());
                    tc.setValue(col.getValue());
                    tc.setIndex(col.getIndex());
                    tc.setKey(col.getIsKey());
                    tc.setUpdated(col.getUpdated());
                    tc.setMysqlType(col.getMysqlType());
                    tc.setSqlType(col.getSqlType());
                    tc.setNull(col.getIsNull());
                    columnList.add(tc);
                    if (tc.isUpdated()){
                        updatedCols.append(tc.getName()).append(","); // 形如  ,col1,col2,col3,
                    }
                }
                rowInfo.setColumnInfos(columnList);
                if ("update".equalsIgnoreCase(this.eventType)){
                    rowInfo.setUpdatedCols(updatedCols.toString());
                }else {
                    rowInfo.setUpdatedCols("*");
                }
                entryList.add(rowInfo);
            }
            this.entryList = entryList;
        } catch (InvalidProtocolBufferException e) {
            log.warn("PARSE_ENTRY FAILED ->" + e.getMessage());
        }
    }

    /**
     * parse entry whose type is 'delete'
     * @param entry entry to be parsed
     * @param delete entry type
     */
    public ParseEntry(CanalEntry.Entry entry, String delete){

        System.out.println("the entry type is " + delete);
        this.tableName = entry.getHeader().getTableName();
        this.logfileOffset = entry.getHeader().getLogfileOffset();
        this.databaseName = entry.getHeader().getSchemaName();
        this.eventType = entry.getHeader().getEventType().toString();
        this.executeTime = entry.getHeader().getExecuteTime();

        try {
            List<CanalEntry.RowData> rowDatas = CanalEntry.RowChange.parseFrom(entry.getStoreValue()).getRowDatasList();
            ArrayList<RowInfo> entryList = new ArrayList<>();
            for (CanalEntry.RowData rd : rowDatas){
                RowInfo rowInfo = new RowInfo();
                rowInfo.setRowSize(rd.getSerializedSize());
                // 每个rowData为一条记录
                ArrayList<ColumnInfo> columnList = new ArrayList<>();
                List<CanalEntry.Column> beforeColumnsList = rd.getBeforeColumnsList();
                for (CanalEntry.Column col : beforeColumnsList){
                    ColumnInfo tc = new ColumnInfo();
                    tc.setName(col.getName());
                    tc.setValue(col.getValue());
                    tc.setIndex(col.getIndex());
                    tc.setKey(col.getIsKey());
                    tc.setUpdated(col.getUpdated());
                    tc.setMysqlType(col.getMysqlType());
                    tc.setSqlType(col.getSqlType());
                    tc.setNull(col.getIsNull());
                    columnList.add(tc);
                }
                rowInfo.setColumnInfos(columnList);
                rowInfo.setUpdatedCols("*");
                entryList.add(rowInfo);
            }
            this.entryList = entryList;
        } catch (InvalidProtocolBufferException e) {
            log.warn("PARSE_ENTRY FAILED ->" + e.getMessage());
        }


    }

    public String getTableName() {
        return tableName;
    }

    public long getLogfileOffset() {
        return logfileOffset;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getEventType() {
        return eventType;
    }

    public long getExecuteTime() {
        return executeTime;
    }

    public ArrayList<RowInfo> getEntryList() {
        return entryList;
    }
}
