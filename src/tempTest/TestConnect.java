package tempTest;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import config.ConfigClass;
import org.dom4j.DocumentException;
import beans.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class TestConnect {

    private static ConfigClass config;

//    private static ConfigClass config;

    public static void main(String[] args) throws IOException {



        String canalUrl ;
        String baseConn ;
        int batchSize ;
        String xmlPath ;
        int sleepDuration;
//        log.info("******************* THE JOB IS RUNNING *******************");
        canalUrl = "111.231.66.20:11111/example1";
        baseConn =
                "mysql#base20=mycanal:1111@111.231.66.20:3306/tobase3" +
                        ",mysql#base101=root:1111@192.168.24.101:3306/tobase1" +
//                ",mysql#base11=root:1111@192.168.69.178:3306/tobase2";
                        ",mysql#base11=root:1111@192.168.24.11:3306/tobase2";
        batchSize = 1000;
        xmlPath = "D:\\programs\\canal_pro\\src\\main\\resources\\schema.xml";
        sleepDuration = 2000;

        CanalConnector connector  = CanalConnectors.newSingleConnector(
                new InetSocketAddress(
                        "111.231.66.20", // example1 192.168.122.7
                        11111
                ),
                "example1",
                "",
                ""
        );
        config = new ConfigClass(canalUrl, batchSize, xmlPath, baseConn, sleepDuration);

//        int batchSize = 1000;
        int emptyCount = 0;
        try {
            connector.connect();
            // 配置需要监控的表
            connector.subscribe(config.getSubscribe_tb());
            // 回滚到未进行 {@link #ack} 的地方，下次fetch的时候，可以从最后一个没有 {@link #ack} 的地方开始拿
            connector.rollback();
            int totalEmtryCount = 1200;
            while (emptyCount < totalEmtryCount) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
//                System.out.println("batchid is  --------- " + batchId);

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
        System.out.println("entries size is  "+entrys.size());
        for (CanalEntry.Entry entry : entrys) {

            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                    || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            ParseEntry parseEntry = new ParseEntry(entry);
            String databaseName = parseEntry.getDatabaseName();

            String tableName = parseEntry.getTableName();
            System.out.println(databaseName + " --- " + tableName);
            for(Schema sc : config.getSchemas()){
                String from_database = sc.getFrom_database();
                for(SingleTable st : sc.getSingleTables()){
                    String tbn = st.getTableName();
                    // 从get的数据中匹配出xml中需要的表
                    if (databaseName.equalsIgnoreCase(from_database) && tableName.equalsIgnoreCase(tbn)){
                        // 根据 conn_name 匹配出数据库连接url
                        ConnArgs connArgs = config.getConnArgs().get(st.getConn_str_name());
                        System.out.println("\nload data to " + connArgs.getConUrl() + "\n");
                    }
                }
            }
            ArrayList<ArrayList<ColumnInfo>> entryList = parseEntry.getEntryList();

            for (ArrayList<ColumnInfo> columns : entryList){
                StringBuilder sb = new StringBuilder("insert into ");
                sb.append(databaseName).append(".").append(tableName).append("(");
                for (ColumnInfo tc : columns){
                    sb.append(",").append(tc.name);
                }
                sb.append(") values(");
                for (ColumnInfo tc : columns){
                    sb.append(",").append(tc.value);
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





            CanalEntry.RowChange rowChage;
            try {
                rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }
            int rowDatasCount = rowChage.getRowDatasCount();
            System.out.println("rowdatacount is  " + rowDatasCount);

            CanalEntry.EventType eventType = rowChage.getEventType();


            System.out.println(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(), eventType));

            for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {

                System.out.println(rowChage.getRowDatasList().size());


//                System.out.println("-----------\n"+entry+"\n------entry-----\n");
                System.out.println("-----------\n"+rowData+"\n------rowdata-----\n");
//                System.out.println("-----------\n"+rowChage+"\n------rowchange-----\n");

//                CanalEntry.Header header = entry.getHeader();

                System.out.println(entry.getHeader().getSerializedSize() + "-------=----------=-----entryheadersize");
                System.out.println(entry.getHeader().getLogfileOffset()+"-------=----------=-----offset");
                System.out.println(rowData.getSerializedSize()+"-------=----------=-----rowdatasize");
                System.out.println(rowChage.getSerializedSize() + "-------=----------=-----rowchangesize");
                System.out.println(entry.getSerializedSize()+"-------=----------=-----entrysize");




                // mysql中有个offset和size  可查
                // 尝试对序列化
                // 构造出标记每个rowData的值

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
