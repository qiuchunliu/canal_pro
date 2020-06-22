package tempTest;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import config.ConfigClass;
import beans.*;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class TestConnect {

    public static void main(String[] args) throws IOException {

        String canalUrl ;
        String baseConn ;
        int batchSize ;
        String xmlPath ;
        int sleepDuration;
        canalUrl = "111.231.66.20:11111/example1";
        baseConn = "mysql|base20=5v_user:dec44ad@192.168.0.159:30115/tosdfbase";
        batchSize = 1000;
//        xmlPath = "D:\\programs\\canal_pro\\src\\main\\resources\\schema1.xml";
        xmlPath = "D:\\programs\\canal_pro\\src\\main\\resources\\schema1.xml";
        sleepDuration = 2000;

        CanalConnector connector  = CanalConnectors.newSingleConnector(
                new InetSocketAddress(
                        "111.231.66.20", // example1 192.168.122.7   111.231.66.20
                        11111
                ),
                "example1",
                "",
                ""
        );
        ConfigClass config = new ConfigClass(canalUrl, batchSize, xmlPath, baseConn, sleepDuration);

        int emptyCount = 0;
        try {
            connector.connect();
            // 配置需要监控的表
            connector.subscribe(config.getSubscribeStr());
            // 回滚到未进行 {@link #ack} 的地方，下次fetch的时候，可以从最后一个没有 {@link #ack} 的地方开始拿
            connector.rollback();
            int totalEmtryCount = 1200;
            while (emptyCount < totalEmtryCount) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();

                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    emptyCount++;
                    System.out.println("empty count : " + emptyCount);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    emptyCount = 0;
                    System.out.printf("message[batchId=%s,size=%s] \n", batchId, size);
                    printEntry(message.getEntries());
                }

                connector.ack(batchId); // 提交确认
                connector.rollback(); // 处理失败, 回滚数据
            }

            System.out.println("empty too many times, exit");
        } finally {
            connector.disconnect();
        }
    }

    private static void printEntry(List<CanalEntry.Entry> entrys) throws InvalidProtocolBufferException {
        System.out.println("------------entries size is  "+entrys.size());
        for (CanalEntry.Entry entry : entrys) {



            // 用于过滤事务头事务尾
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                    || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            ParseEntry parseEntry = new ParseEntry(entry);
            String databaseName = parseEntry.getDatabaseName();

            String tableName = parseEntry.getTableName();

            ArrayList<RowInfo> entryList = parseEntry.getEntryList();

            for (RowInfo columns : entryList){
                StringBuilder sb = new StringBuilder("insert into ");
                sb.append(databaseName).append(".").append(tableName).append("(");
                for (ColumnInfo tc : columns.getColumnInfos()){
                    sb.append(",").append(tc.getName());
                }
                sb.append(") values(");
                for (ColumnInfo tc : columns.getColumnInfos()){
                    String v = tc.getValue().replace("\'", "");
                    System.out.println("_------------------------------------------------------" + v);
                    sb.append(",").append(v);
                }
                sb.append(");");
                String sql = sb.toString().replace("(,", "(");
                System.out.println(sql);
            }

            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            String eventtype = rowChange.getEventType().name();
            System.out.println("eventtype is " + eventtype);

            String entryType = entry.getEntryType().toString();
            System.out.println("entryType is  "+entryType);


            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }
            int rowDatasCount = rowChange.getRowDatasCount();
            System.out.println("rowdatacount is  " + rowDatasCount);

            CanalEntry.EventType eventType = rowChange.getEventType();



            System.out.println(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(), eventType));

            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {


                if (eventType == CanalEntry.EventType.DELETE) {
                    printColumn(rowData.getBeforeColumnsList());
                } else if (eventType == CanalEntry.EventType.INSERT) {
                    printColumn(rowData.getAfterColumnsList());
                } else {
                    System.out.println("-------> before");
                    printColumn(rowData.getBeforeColumnsList());
                    System.out.println("-------> after");
                    printColumn(rowData.getAfterColumnsList());
                }
            }
        }
    }

    private static void printColumn(List<CanalEntry.Column> columns) {
        for (CanalEntry.Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }

}
