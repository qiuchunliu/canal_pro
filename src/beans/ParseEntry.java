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
    private String logfileName;
    private String eventType;
    private long executeTime;
    private ArrayList<ArrayList<ColumnInfo>> entryList;
    private static Logger log = Logger.getLogger(ParseEntry.class);


    public ParseEntry(CanalEntry.Entry entry){

        this.tableName = entry.getHeader().getTableName();
        this.logfileOffset = entry.getHeader().getLogfileOffset();
        this.databaseName = entry.getHeader().getSchemaName();
        this.logfileName = entry.getHeader().getLogfileName();
        this.eventType = entry.getHeader().getEventType().toString();
        this.executeTime = entry.getHeader().getExecuteTime();

        try {
            List<CanalEntry.RowData> rowDatas = CanalEntry.RowChange.parseFrom(entry.getStoreValue()).getRowDatasList();
            ArrayList<ArrayList<ColumnInfo>> entryList = new ArrayList<>();
            for (CanalEntry.RowData rd : rowDatas){
                // 每个rowData为一条记录

                ArrayList<ColumnInfo> columnList = new ArrayList<>();
                List<CanalEntry.Column> afterColumnsList = rd.getAfterColumnsList();
                for (CanalEntry.Column col : afterColumnsList){
                    ColumnInfo tc = new ColumnInfo();
                    tc.name = col.getName();
                    tc.value = col.getValue();
                    tc.index = col.getIndex();
                    tc.isKey = col.getIsKey();
                    tc.updated = col.getUpdated();
                    tc.mysqlType = col.getMysqlType();
                    tc.sqlType = col.getSqlType();
                    tc.isNull = col.getIsNull();
                    columnList.add(tc);
                }
                entryList.add(columnList);
            }
            this.entryList = entryList;
        } catch (InvalidProtocolBufferException e) {
            log.error("PARSE_ENTRY FAILED", e);
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

    public String getLogfileName() {
        return logfileName;
    }

    public String getEventType() {
        return eventType;
    }

    public long getExecuteTime() {
        return executeTime;
    }

    public ArrayList<ArrayList<ColumnInfo>> getEntryList() {
        return entryList;
    }
}
