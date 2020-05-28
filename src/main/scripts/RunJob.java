package main.scripts;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.protobuf.InvalidProtocolBufferException;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.exception.CompileExpressionErrorException;
import com.googlecode.aviator.exception.ExpressionRuntimeException;
import config.ConfigClass;
import org.apache.commons.lang.StringUtils;
import beans.*;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

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
        /*
         * 实例化canal连接对象
         */
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
            /*
             * 连接部署canal的server
             */
            connector.connect();
            /*
             * 设置需要监听的表
             * 此处如果调用 subscribe() 方法会覆盖instance.properties文件的过滤配置
             * 过滤字符串是正则表达式
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
        log.info("RUN_LOOP DOING ->ready to get batchSize=" + batchSize);
        try {
            int cycleCount = 0;
            while (true) {
                // message：事件集合
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    cycleCount++;
                    if (cycleCount >= 20){
                        log.info("RUN_LOOP WAITING");
                        cycleCount = 0;
                    }
                    try {
                        Thread.sleep(config.getSleepDuration());  // 没有返回数据时等待
                    } catch (InterruptedException e) {
                        log.warn("RUN_LOOP WAITING ->Thread.sleep() error ", e);
                        return;
                    }
                } else {
                    log.info(String.format("RUN_LOOP CYCLING ->entriesSize=%s, batchId=%s", size, batchId));
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
            log.error("RUN_LOOP FAILED ->" + e);
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
        /*
         * 先找出 TRANSACTIONEND 放在列表里备用
         */
        ArrayList<String> transactionIds = getTransactionId(entries);

        log.info("INSERT PREPARE ->traversing schemas");
        for(Schema schema : schemas){
            String sourceDatabase = schema.getSourceDatabase();
            log.info("TRAVERSE DOING ->traverse schema="+sourceDatabase);
            for(SingleTable sourceTable : schema.getSingleTables()){

                String transactionId = "-1";

                String sourceTableName = sourceTable.getTableName();
                log.info("TRAVERSE DOING ->current table=" + sourceDatabase+"."+sourceTableName);
                String loadTable = sourceTable.getLoadTable();
                log.info(String.format("TRAVERSE DOING ->sourceTable=%s, destinationTable=%s",sourceDatabase+"."+sourceTableName, loadTable));

                // 处理条件表达式引擎
                Expression compiledExp = parseCondition(sourceTable);
                HashMap<String, Object> env;

                int insertSize = sourceTable.getInsertSize();
                log.info(String.format("INSERT PREPARE ->insertSize=%s, mysql connection name=%s ", insertSize, sourceTable.getConnStrName()));
                ArrayList<ColumnInfo> loadColumns = sourceTable.getColumns();
                ConnArgs connArgs = config.getConnArgs().get(sourceTable.getConnStrName());// 获取数据库连接参数

                // sql的col部分
                StringBuilder sqlColsStr = new StringBuilder();
                sqlColsStr.append("replace into ").append(loadTable).append("(");
                ArrayList<String> cols = new ArrayList<>();
                for(ColumnInfo ci : loadColumns){
                    cols.add(ci.toCol);
                }
                String colsStr = StringUtils.join(cols, ",");
                log.info("INSERT PREPARE ->destination columns = "+colsStr);
                sqlColsStr.append(colsStr).append(") values");
                String sqlHead = sqlColsStr.toString();

                // sql的values部分
                ArrayList<String> sqlValuesStr = new ArrayList<>();
                int procBatch = 1;

                int transactionEndCnt = 0;
                /*
                 * 找到每个操作对应的事务id
                 */
                for (CanalEntry.Entry entry : entries){
                    if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                        if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN){
                            try {
                                transactionId = transactionIds.get(transactionEndCnt);
                            }catch (IndexOutOfBoundsException e){
                                log.error(String.format(
                                        "PARSE_ENTRY FAILED -> get transactionId failed, transactionIdsSize=%s, transactionEndCnt=%s"
                                        ,transactionIds.size()
                                        ,transactionEndCnt)
                                );
                            }
                            transactionEndCnt++;
                        }
                        continue;
                    }

                    // 解析一个entry 此时解析的是ROWDATA的entry
                    String operate = "";
                    try {
                        operate = CanalEntry.RowChange.parseFrom(entry.getStoreValue()).getEventType().name();
                    } catch (InvalidProtocolBufferException e) {
                        log.warn("PARSE_ENTRY FAILED -> get operate type failed");
                    }
                    ParseEntry parseEntry;
                    if (operate.equalsIgnoreCase("delete")){
                        parseEntry = new ParseEntry(entry, "delete");
                    }else {
                        parseEntry = new ParseEntry(entry);
                    }

                    // 将entry中所有的字段解析出来
                    ArrayList<RowInfo> entryList = parseEntry.getEntryList();
                    /*
                     * 校验获取的表
                     * filter可设置为正则表达式，对表名进行匹配
                     */
                    if (Pattern.compile(sourceDatabase).matcher(parseEntry.getDatabaseName()).matches()
                        && Pattern.compile(sourceTableName).matcher(parseEntry.getTableName()).matches()){
                        log.info(String.format("PARSE_ENTRY DOING -> eventType=%s, logTime=%s, bin-log position=%s, databaseName=%s, tableName=%s"
                                ,parseEntry.getEventType()
                                ,parseEntry.getExecuteTime()
                                ,parseEntry.getLogfileOffset()
                                ,parseEntry.getDatabaseName()
                                ,parseEntry.getTableName()
                                )
                        );
                        for (RowInfo columns : entryList){ // 每个 rowinfo 表示一条记录
                            // 将解析出的字段，创建成map
                            HashMap<String, String> colValue = makeKV(columns.getColumnInfos());

                            // 加载每一个字段到条件筛选
                            env = loadColsForCondition(colValue);

                            // 如果该条记录不符合条件，则过滤掉
                            try {
                                Boolean needed = (Boolean) compiledExp.execute(env);
                                if(!needed) continue;
                            } catch (ExpressionRuntimeException expressionRuntimeException){
                                log.error("PARSE_EXPRESSION FAILED -> data type maybe wrong ", expressionRuntimeException);
                                return;
                            }catch (NullPointerException e){
                                log.warn("PARSE_EXPRESSION FAILED -> caused by missing condition, ignore it");
                            } catch (Exception e){
                                log.info("PARSE_EXPRESSION FAILED", e);
                                return;
                            }

                            // 构建插入的值的str
                            StringBuilder valuesStr = new StringBuilder("(");
                            /*
                             * 拼接insert的字段值
                             * 此处拼接的是根据需要输出字段匹配出的字段值
                             */
                            for (int i=0; i< loadColumns.size()-9; i++){
                                ColumnInfo tc = loadColumns.get(i);
                                valuesStr.append(",\"").append(colValue.get(tc.name.toLowerCase())).append("\"");
                            }
                            /*
                             * 补充控制字段的字段值
                             * etl_id uuid函数生成
                             * log_time  取transactionId
                             * rec_time 取当前格式化时间
                             * procBatch 取事务的执行时间
                             * flag 记录操作状态
                             * log_rec_size 取rowData的size
                             * log_rec_pos 取entry的offset，即position
                             * operate 取eventType
                             * log_file_name 取binlog文件名的hashCode()
                             */
                            valuesStr.append(",\"").append(UUID.randomUUID().toString().replace("-","")).append("\""); // etl_id
                            valuesStr.append(",\"").append(transactionId).append("\""); // log_time
                            valuesStr.append(",\"").append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ssSSS").format(new Date())).append("\"");  // rec_time
                            valuesStr.append(",\"").append(entry.getHeader().getExecuteTime()).append("\"");  // procBatch ,使用毫秒时间戳表示
                            valuesStr.append(",\"").append(0).append("\"");  // flag
                            valuesStr.append(",\"").append(columns.getRowSize()).append("\"");  // log_rec_size
                            valuesStr.append(",\"").append(entry.getHeader().getLogfileOffset()).append("\"");  // log_rec_pos
                            try {
                                String operateType = CanalEntry.RowChange.parseFrom(entry.getStoreValue()).getEventType().name();
                                int operateCode = operateType.equalsIgnoreCase("INSERT") ? 0 : operateType.equalsIgnoreCase("UPDATE") ? 1 : 2;
                                valuesStr.append(",\"").append(operateCode).append("\"");  // operate
                            } catch (InvalidProtocolBufferException e) {
                                log.error("PARSE_ENTRY FAILED -> failed to get eventType", e);
                            }
                            valuesStr.append(",\"").append(entry.getHeader().getLogfileName().hashCode()).append("\"").append(")");  // log_file_name hashcode()

                            String eachRowStr = valuesStr.toString().replace("(,", "(");
                            System.out.println("PARSE_ENTRY DONE ->colValues=" + eachRowStr);  // 只输出显示，不记录到log
                            sqlValuesStr.add(eachRowStr);
                            if (sqlValuesStr.size() == insertSize){
                                log.info(String.format("INSERT PREPARE ->valueRows match the insertSize, ready to insert %s values", procBatch++ ));
                                executeInsert(connArgs, sqlValuesStr, sqlHead, 1);
                            }
                        }
                    }
                }
                if (sqlValuesStr.size() != 0) {
                    log.info("INSERT PREPARE ->valueRows does't match the insertSize");
                    executeInsert(connArgs,sqlValuesStr,sqlHead,0);
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
            colValue.put(ci.name.toLowerCase(), ci.value);
        }
        return colValue;
    }

    /**
     * 根据获取到的entries，找到每个事务尾所包含的事务id
     * @param entries get到的entries
     * @return 这些entries中包含的事务尾的 事务id
     */
    private static ArrayList<String> getTransactionId(List<CanalEntry.Entry> entries){
        ArrayList<String> transactionIds = new ArrayList<>();
        for (CanalEntry.Entry entry : entries){
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND){
                try {
                    String transacId = CanalEntry.TransactionEnd.parseFrom(entry.getStoreValue()).getTransactionId();
                    transactionIds.add(transacId);
                } catch (InvalidProtocolBufferException e) {
                    log.warn("PARSE_ENTRY FAILED ->get transactionId failed");
                }
            }
        }
        return transactionIds;
    }

    /**
     * 根据schema中的条件构造过滤表达式引擎
     * @param sourceTable schema中的一张表
     * @return 表达式引擎
     */
    private static Expression parseCondition(SingleTable sourceTable){
        String rowConditions = sourceTable.getRowConditions();
        Expression compiledExp = AviatorEvaluator.compile("hold");
        try {
            compiledExp = AviatorEvaluator.compile(rowConditions);
        }catch (NullPointerException | CompileExpressionErrorException e){
            log.warn("PARSE_CONDITION FAILED -> no condition here");
        }
        return compiledExp;
    }

    /**
     * 将所有字段加载到map中，用于后续与表达式匹配过滤
     * @param colValue 所有字段值
     * @return 加载所有字段的map
     */
    private static HashMap<String, Object> loadColsForCondition(HashMap<String, String> colValue){
        HashMap<String, Object> env = new HashMap<>();
        for(Map.Entry<String, String> s: colValue.entrySet()){
            try {  // 处理数据类型问题
                env.put(s.getKey(),Integer.parseInt(s.getValue()));
            }catch (Exception e){
                env.put(s.getKey(),s.getValue());
            }
        }
        return env;
    }

    /**
     * 当插入条数达到设定值时进行insert
     * @param connArgs 数据库连接参数
     * @param sqlValuesStr values的str
     * @param sqlHead insert的字段
     */
    private static void executeInsert(ConnArgs connArgs, ArrayList<String> sqlValuesStr, String sqlHead, int clearTag){
        log.info(String.format("GET_MYSQLCONN DOING ->connect args=%s"
                ,connArgs.getUserId()+":"+connArgs.getPwd()+"@"+connArgs.getAddress()+":"+connArgs.getPort()+"/"+connArgs.getDatabase())
        );
        MysqlConn mysqlConn;
        try {
            mysqlConn = new MysqlConn(connArgs.getAddress(), connArgs.getPort(), connArgs.getUserId(), connArgs.getPwd(), connArgs.getDatabase());
        }catch (ClassNotFoundException e){
            log.error("GET_MYSQLCONN FAILED", e);
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
        if (clearTag == 1) {sqlValuesStr.clear();}  // 清空value列表
        try {
            mysqlConn.close();
        } catch (SQLException e) {
            log.warn("CLOSE_MYSQLCONN FAILED");
        }
    }
}
