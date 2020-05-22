package main.scripts;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
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
     * @param xmlUrl 配置文件路径
     */
    RunJob(String canalUrl, int batchSize, String xmlUrl, String conn_str, int sleepDuration) {
        log.info(String.format("INITIAL_CONFIG DOING ->" +
                        "xmlUrl=%s," +
                        "conn_str=%s," +
                        "canalUrl=%s," +
                        "batchSize=%s"
                ,xmlUrl, conn_str, canalUrl, batchSize)
        );
        try {
            config = new ConfigClass(canalUrl, batchSize, xmlUrl, conn_str, sleepDuration);
            log.info("INITIAL_CONFIG SUCCESS");
        }catch (Exception e){
            log.error("INITIAL_CONFIG FAILED ->" + e);
        }
    }

    /**
     * 主执行代码
     */
    void doit() {
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
            connector.subscribe(config.getSubscribe_tb());

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
        for(Schema sc : schemas){
            String database = sc.getFrom_database();
            log.info("TRAVERSE DOING ->traverse schema="+database);
            for(SingleTable st : sc.getSingleTables()){
                String tableName = st.getTableName();
                log.info("TRAVERSE DOING ->current table=" + database+"."+tableName);
                String loadTable = st.getLoadTable();
                log.info(String.format("TRAVERSE DOING ->sourceTable=%s, destinationTable=%s",database+"."+tableName, loadTable));

                int insertSize = st.getInsertSize();
                log.info("INSERT PREPARE ->insertSize=" + insertSize);
                ArrayList<ColumnInfo> loadColumns = st.getColumns();
                ConnArgs connArgs = config.getConnArgs().get(st.getConn_str_name());// 获取数据库连接参数
                log.info("INSERT PREPARE ->mysql connection name=" + st.getConn_str_name());

                // sql的col部分
                StringBuilder sb = new StringBuilder();
                sb.append("insert into ").append(loadTable).append("(");
                ArrayList<String> cols = new ArrayList<>();
                for(ColumnInfo ci : loadColumns){
                    cols.add(ci.toCol);
                }
                String col_str = StringUtils.join(cols, ",");
                log.info("INSERT PREPARE ->destination columns are="+col_str);
                String sqlHead = sb.append(col_str).append(") values") + "";

                // sql的values部分
                ArrayList<String> vas = new ArrayList<>();
                int proc_batch = 1;
                for (CanalEntry.Entry entry : entries){
                    if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                            || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
//                        log.info(String.format("entry type is %s, ignore this entry", entry.getEntryType()));
                        continue;
                    }

                    // 解析一个entry 此时解析的是ROWDATA的entry
                    log.info("PARSE_ENTRY DOING -> entryInfo = \n" + entry.toString());

                    ParseEntry parseEntry = new ParseEntry(entry);

                    // 将entry中所有的字段解析出来
                    ArrayList<ArrayList<ColumnInfo>> entryList = parseEntry.getEntryList();
                    String full_tbname = parseEntry.getDatabaseName() + "." + parseEntry.getTableName();

                    /*
                     * 校验获取的表
                     * 考虑用正则处理源库分表的问题
                     * filter设置为正则表达式，对表名进行匹配
                     */
                    if ((database+"."+tableName).equalsIgnoreCase(full_tbname)){
                        for (ArrayList<ColumnInfo> columns : entryList){
                            // 将解析出的字段，创建成map
                            HashMap<String, String> colValue = makeKV(columns);
                            StringBuilder vals = new StringBuilder("(");

                            /*
                             * 拼接insert的字段值
                             * 此处拼接的是根据需要输出字段匹配出的字段值
                             */
                            for (int i=0; i< loadColumns.size()-5; i++){
                                ColumnInfo tc = loadColumns.get(i);
                                vals.append(",\"").append(colValue.get(tc.name)).append("\"");
                            }
                            // 补充控制字段
                            vals.append(",\"").append(UUID.randomUUID().toString().replace("-","")).append("\""); // etl_id
                            vals.append(",\"").append(entry.getHeader().getExecuteTime()).append("\""); // log_time
                            vals.append(",\"").append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ssSSS").format(new Date())).append("\"");  // rec_time
                            vals.append(",\"").append(proc_batch).append("\"");  // proc_batch
                            vals.append(",\"").append(0).append("\"").append(")");  // flag

                            String vs = vals.toString().replace("(,", "(");
                            log.info("PARSE_ENTRY DONE ->colValues=%s" + vs);
                            vas.add(vs);
                            if (vas.size() == insertSize){
                                log.info(String.format("INSERT PREPARE ->valueRows match the insertSize, ready to insert %s values", proc_batch++ ));
                                log.info(String.format("GET_MYSQLCONN DOING ->connect args=%s"
                                        ,connArgs.getUser_id()+":"+connArgs.getPwd()+"@"+connArgs.getAddress()+":"+connArgs.getPort()+"/"+connArgs.getDatabase())
                                );
                                MysqlConn mysqlConn;
                                try {
                                    mysqlConn = new MysqlConn(connArgs.getAddress(), connArgs.getPort(), connArgs.getUser_id(), connArgs.getPwd(), connArgs.getDatabase());
                                }catch (ClassNotFoundException e){
                                    log.error("GET_MYSQLCONN FAILED");
                                    return;
                                } catch (SQLException e) {
                                    return;
                                }

                                log.info("GET_MYSQLCONN SUCCESS");
                                Statement stmt = mysqlConn.getStmt();
                                StringBuilder sql_temp = new StringBuilder(sqlHead);
                                String values = StringUtils.join(vas, ",");
                                sql_temp.append(values).append(";");
                                // 执行sql进行insert
                                log.info("INSERT PREPARE ->sql=" + sql_temp);
                                try {
                                    stmt.execute(String.valueOf(sql_temp));
                                } catch (SQLException e) {
                                    log.error("INSERT FAILED", e);
                                    return;
                                }
                                log.info("INSERT SUCCESS");
                                vas.clear();  // 清空value列表
                                try {
                                    mysqlConn.close();
                                } catch (SQLException e) {
                                    return;
                                }
                            }
                        }
                    }
                }
                if (vas.size() != 0) {
                    log.info("INSERT PREPARE ->valueRows does't match the insertSize");
                    String values = StringUtils.join(vas, ",");
                    sb.append(values).append(";");
                    log.info(String.format("GET_MYSQLCONN DOING ->connect args=%s",
                            connArgs.getUser_id()+":"+connArgs.getPwd()+"@"+connArgs.getAddress()+":"+connArgs.getPort()+"/"+connArgs.getDatabase()));
                    MysqlConn mysqlConn;
                    try {
                        mysqlConn = new MysqlConn(connArgs.getAddress(), connArgs.getPort(), connArgs.getUser_id(), connArgs.getPwd(), connArgs.getDatabase());
                    } catch (ClassNotFoundException | SQLException e) {
                        log.error("GET_MYSQLCONN FAILED", e);
                        return;
                    }
                    log.info("GET_MYSQLCONN SUCCESS");
                    Statement stmt = mysqlConn.getStmt();
                    log.info("INSERT PREPARE ->sql=" + sb);
                    try {
                        stmt.execute(String.valueOf(sb));
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
