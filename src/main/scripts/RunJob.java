package main.scripts;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.protobuf.InvalidProtocolBufferException;
import config.ConfigClass;
import org.apache.commons.lang.StringUtils;
import beans.*;
import org.apache.log4j.Logger;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;

class RunJob {

    private static ConfigClass config;
    private static Logger log = Logger.getLogger(RunJob.class);


    /**
     * 配置文件的参数
     * @param schemaPath 配置文件路径
     */
    RunJob(String canalUrl, int batchSize, String schemaPath, String mysqlConnStr, int sleepDuration) {
        log.info(String.format("INITIAL_CONFIG DOING ->" + "schemaPath=%s," + "mysqlConnStr=%s," + "canalUrl=%s," +
                        "batchSize=%s", schemaPath, mysqlConnStr, canalUrl, batchSize));
        try {
            config = new ConfigClass(canalUrl, batchSize, schemaPath, mysqlConnStr, sleepDuration);
            log.info("INITIAL_CONFIG SUCCESS");
        }catch (Exception e){
            log.error("INITIAL_CONFIG FAILED ->" + e);
        }
    }

    /**
     * 主执行代码
     */
    void run() {
        log.info("RUN_CORE DOING ->******************* THE CORE IS RUNNING ******************* ");
        // canal连接的实例化对象
        log.info(String.format("INITIAL_CANALCONN DOING ->canalIp=%s;canalPort=%s;canalDesti=%s"
                ,config.getCanalIp(), config.getCanalPort(), config.getDestination()));
        CanalConnector connector;
        try {
            connector = CanalConnectors.newSingleConnector(
                    new InetSocketAddress(
                            config.getCanalIp(),  // canal的ip
                            config.getCanalPort()  // canal的端口
                    ),
                    config.getDestination(),  // canal的destination
                    "",  // mysql中配置的canal的 user
                    ""   // mysql中配置的canal的 password
            );
        }catch (Exception e){
            log.error("INITIAL_CANALCONN FAILED\n", e);
            return;
        }
        log.info("INITIAL_CANALCONN SUCCESS");

        try {
            // 连接对应的canal server
            connector.connect();
            /*
             * 设置需要监听的表
             * 此处调用 subscribe 方法会覆盖instance.properties文件的过滤配置
             */
            connector.subscribe(config.getSubscribeStr());

            // 回滚到未进行 {@link #ack} 的地方，下次fetch的时候，可以从最后一个没有 {@link #ack} 的地方开始拿
            connector.rollback();
        }catch (CanalClientException e){
            log.error("CONNECT_CANAL FAILED\n", e);
            return;
        }
        log.info("CONNECT_CANAL SUCCESS ->******************* CANAL CONNECTED *******************");

        int batchSize = config.getBatchSize();  // 每次获取的binlog条数
        log.info(String.format("RUN_LOOP DOING ->ready to get batchSize=%s", batchSize));
        try {
            while (true) {
                // message：事件集合
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                log.info(String.format("RUN_LOOP CYCLING ->entriesSize=%s, batchId=%s", size, batchId));
                if (batchId == -1 || size == 0) {
                    log.info("RUN_LOOP WAITING");
                    try {
                        Thread.sleep(config.getSleepDuration());  // 没有返回数据时等待
                    } catch (InterruptedException e) {
                        log.warn("RUN_LOOP WAITING ->Thread.sleep() error ", e);
                        return;
                    }
                } else {
                    List<CanalEntry.Entry> entries = message.getEntries();
                    printEntry(entries);// only to print, log file does'n contain it
                    ArrayList<Schema> schemas = config.getSchemas();
                    log.info("RUN_LOOP INSERT ->ready to insert .....");
                    insertEvent(entries, schemas);
                    log.info("RUN_LOOP SUCCESS ->insert success");
                }
                connector.ack(batchId); // 提交确认
            }
        }catch (CanalClientException e){
            log.error("RUN_LOOP FAILED \n"+e);
        } finally {
            connector.disconnect();
            log.info("RUN_CORE SUCCESS ->******************* DISCONNECT CANAL *******************");
        }
    }

    private static void printEntry(List<CanalEntry.Entry> entrys) {
        for (CanalEntry.Entry entry : entrys) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                    || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            CanalEntry.RowChange rowChage;
            try {
                rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }

            CanalEntry.EventType eventType = rowChage.getEventType();
            System.out.println(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(), eventType));


            for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
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

    /**
     * 将数据插入目标表
     * @param entries 相当于多条记录的集合
     * @param schemas 关注的schema
     */
    private static void insertEvent(List<CanalEntry.Entry> entries, ArrayList<Schema> schemas){
        log.info("INSERT PREPARE ->traversing schemas");
        for(Schema schema : schemas){
            String sourceDatabase = schema.getSourceDatabase();
            log.info("TRAVERSE DOING ->traverse schema="+sourceDatabase);
            for(SingleTable sourceTable : schema.getSingleTables()){
                String sourceTableName = sourceTable.getTableName();
                log.info("TRAVERSE DOING ->current table=" + sourceDatabase+"."+sourceTableName);
                String loadTable = sourceTable.getLoadTable();
                log.info(String.format("TRAVERSE DOING ->sourceTable=%s, destinationTable=%s",sourceDatabase+"."+sourceTableName, loadTable));

                int insertSize = sourceTable.getInsertSize();
                log.info("INSERT PREPARE ->insertSize=" + insertSize);
                ArrayList<ColumnInfo> loadColumns = sourceTable.getColumns();
                ConnArgs connArgs = config.getConnArgs().get(sourceTable.getConnStrName());// 获取数据库连接参数
                log.info("INSERT PREPARE ->mysql connection name=" + sourceTable.getConnStrName());

                // sql的col部分
                StringBuilder sqlColsStr = new StringBuilder();
                sqlColsStr.append("insert into ").append(loadTable).append("(");
                ArrayList<String> cols = new ArrayList<>();
                for(ColumnInfo ci : loadColumns){
                    cols.add(ci.toCol);
                }
                String colsStr = StringUtils.join(cols, ",");
                log.info("INSERT PREPARE ->destination columns = "+colsStr);
                String sqlHead = sqlColsStr.append(colsStr).append(") values") + "";

                // sql的values部分
                ArrayList<String> sqlValuesStr = new ArrayList<>();
                int procBatch = 1;
                for (CanalEntry.Entry entry : entries){
                    if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                            || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                        continue;
                    }

                    // 解析一个entry 此时解析的是ROWDATA的entry
                    String operate = "";
                    try {
                        operate = CanalEntry.RowChange.parseFrom(entry.getStoreValue()).getEventType().name();
                    } catch (InvalidProtocolBufferException e) {
                        log.error("PARSE_ENTRY FAILED -> get operate type failed");
                    }
                    ParseEntry parseEntry;
                    if (operate.equalsIgnoreCase("delete")){
                        parseEntry = new ParseEntry(entry, "delete");
                    }else {
                        parseEntry = new ParseEntry(entry);
                    }

                    // 将entry中所有的字段解析出来
                    ArrayList<RowInfo> entryList = parseEntry.getEntryList();
                    String sourceFullTableName = parseEntry.getDatabaseName() + "." + parseEntry.getTableName();

                    /*
                     * 校验获取的表
                     * 考虑用正则处理源库分表的问题
                     * filter设置为正则表达式，对表名进行匹配
                     */
                    if ((sourceDatabase+"."+sourceTableName).equalsIgnoreCase(sourceFullTableName)){
                        System.out.println("PARSE_ENTRY DOING -> entryInfo = \n" + entry.toString());
                        log.info(String.format("PARSE_ENTRY DOING ->" +
                                        "eventType=%s," + "logTime=%s," + "bin-log position=%s," + "databaseName=%s," +
                                        "tableName=%s",parseEntry.getEventType(),parseEntry.getExecuteTime()
                                ,parseEntry.getLogfileOffset(),parseEntry.getDatabaseName(),parseEntry.getTableName())
                        );
                        for (RowInfo columns : entryList){
                            // 将解析出的字段，创建成map
                            HashMap<String, String> colValue = makeKV(columns.getColumnInfos());
                            StringBuilder valuesStr = new StringBuilder("(");

                            /*
                             * 拼接insert的字段值
                             * 此处拼接的是根据需要输出字段匹配出的字段值
                             */
                            for (int i=0; i< loadColumns.size()-9; i++){
                                ColumnInfo tc = loadColumns.get(i);
                                valuesStr.append(",\"").append(colValue.get(tc.name)).append("\"");
                            }
                            // 补充控制字段
                            valuesStr.append(",\"").append(UUID.randomUUID().toString().replace("-","")).append("\""); // etl_id
                            try {
                                valuesStr.append(",\"").append(CanalEntry.TransactionEnd.parseFrom(entry.getStoreValue()).getTransactionId()).append("\""); // log_time
                            } catch (InvalidProtocolBufferException e) {
                                log.error("PARSE_ENTRY FAILED -> get transaction id failed ", e);
                            }
                            valuesStr.append(",\"").append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ssSSS").format(new Date())).append("\"");  // rec_time
                            valuesStr.append(",\"").append(System.currentTimeMillis()).append("\"");  // procBatch ,使用毫秒时间戳表示
                            valuesStr.append(",\"").append(0).append("\"");  // flag
                            valuesStr.append(",\"").append(columns.getRowSize()).append("\"");  // log_rec_size
                            valuesStr.append(",\"").append(entry.getHeader().getLogfileOffset()).append("\"");  // log_rec_pos
                            try {
                                String operateType = CanalEntry.RowChange.parseFrom(entry.getStoreValue()).getEventType().name();
                                int operateCode =
                                        operateType.equalsIgnoreCase("INSERT") ? 0 : operateType.equalsIgnoreCase("UPDATE") ? 1 : 2;
                                valuesStr.append(",\"").append(operateCode).append("\"");  // operate
                            } catch (InvalidProtocolBufferException e) {
                                log.error("PARSE_ENTRY FAILED -> failed to get eventType", e);
                            }

                            valuesStr.append(",\"").append(entry.getHeader().getLogfileName().hashCode()).append("\"").append(")");  // log_file_name hashcode()


                            String eachRowStr = valuesStr.toString().replace("(,", "(");
                            System.out.println("PARSE_ENTRY DONE ->colValues=" + eachRowStr);
                            sqlValuesStr.add(eachRowStr);
                            if (sqlValuesStr.size() == insertSize){
                                log.info(String.format("INSERT PREPARE ->valueRows match the insertSize, ready to insert %s values", procBatch++ ));
                                log.info(String.format("GET_MYSQLCONN DOING ->connect args=%s"
                                        ,connArgs.getUserId()+":"+connArgs.getPwd()+"@"+connArgs.getAddress()+":"+connArgs.getPort()+"/"+connArgs.getDatabase())
                                );
                                MysqlConn mysqlConn;
                                try {
                                    mysqlConn = new MysqlConn(connArgs.getAddress(), connArgs.getPort(), connArgs.getUserId(), connArgs.getPwd(), connArgs.getDatabase());
                                }catch (ClassNotFoundException e){
                                    log.error("GET_MYSQLCONN FAILED");
                                    return;
                                } catch (SQLException e) {
                                    return;
                                }

                                log.info("GET_MYSQLCONN SUCCESS");
                                Statement stmt = mysqlConn.getStmt();
                                StringBuilder fullSql = new StringBuilder(sqlHead);
                                String values = StringUtils.join(sqlValuesStr, ",");
                                fullSql.append(values).append(";");
                                // 执行sql进行insert
                                log.info("INSERT PREPARE ->sql=" + fullSql);
                                try {
                                    stmt.execute(String.valueOf(fullSql));
                                } catch (SQLException e) {
                                    log.error("INSERT FAILED", e);
                                    return;
                                }
                                log.info("INSERT SUCCESS");
                                sqlValuesStr.clear();  // 清空value列表
                                try {
                                    mysqlConn.close();
                                } catch (SQLException e) {
                                    return;
                                }
                            }
                        }
                    }
                }
                if (sqlValuesStr.size() != 0) {
                    log.info("INSERT PREPARE ->valueRows does't match the insertSize");
                    StringBuilder fullSql = new StringBuilder(sqlHead);
                    String values = StringUtils.join(sqlValuesStr, ",");
                    fullSql.append(values).append(";");
                    log.info(String.format("GET_MYSQLCONN DOING ->connect args=%s",
                            connArgs.getUserId()+":"+connArgs.getPwd()+"@"+connArgs.getAddress()+":"+connArgs.getPort()+"/"+connArgs.getDatabase()));
                    MysqlConn mysqlConn;
                    try {
                        mysqlConn = new MysqlConn(connArgs.getAddress(), connArgs.getPort(), connArgs.getUserId(), connArgs.getPwd(), connArgs.getDatabase());
                    } catch (ClassNotFoundException | SQLException e) {
                        log.error("GET_MYSQLCONN FAILED", e);
                        return;
                    }
                    log.info("GET_MYSQLCONN SUCCESS");
                    Statement stmt = mysqlConn.getStmt();
                    log.info("INSERT PREPARE ->sql=" + fullSql);
                    try {
                        stmt.execute(String.valueOf(fullSql));
                    } catch (SQLException e) {
                        log.error("INSERT FAILED", e);
                        return;
                    }
                    try {
                        mysqlConn.close();
                    } catch (SQLException e) {
                        return;
                    }
                }
            }
        }
    }

    /**
     * 将解析binlog得到的字段-字段值处理成键值对放在map中
     * @param columns binlog解析出的字段
     * @return 键值对的map，包含了解析出的所有字段
     */
    private static HashMap<String, String> makeKV(ArrayList<ColumnInfo> columns){
        HashMap<String, String> colValue = new HashMap<>();
        for (ColumnInfo ci : columns){
            colValue.put(ci.name, ci.value);
        }
        return colValue;
    }
    private static void createTable(ArrayList<ColumnInfo> columnsOut, String full_tbname, Statement stmt){

        StringBuilder sb = new StringBuilder("CREATE TABLE `");
        ArrayList<String> colType = new ArrayList<>();
        for (ColumnInfo ci: columnsOut) {
            String ct = "`" + ci.toCol + "` " + ci.colType + " ";
            colType.add(ct);
        }
        String temp = StringUtils.join(colType.toArray(), ",");
        StringBuilder sql = sb.append(full_tbname.split("\\.")[1]).append("` (").append(temp).append(") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;");
        try {
            stmt.execute(String.format("use %s;", full_tbname.split("\\.")[0]));
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
