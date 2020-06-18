package main.scripts;

import com.alibaba.otter.canal.client.CanalConnector;
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
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.regex.Pattern;

class RunJob {

    private static ConfigClass config;
    private static Logger log = Logger.getLogger(RunJob.class);
    private static ArrayList<String> sqlList = new ArrayList<>();


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
            log.error("INITIAL_CONFIG FAILED ->" + e.getMessage());
            // 参数配置失败，程序终止
            System.exit(1);
        }
    }

    /**
     * 主执行代码
     */
    void run() {
        log.info("RUN_CORE DOING ->******************* THE CORE IS RUNNING ******************* ");
        // 实例化canal连接对象
        log.info(String.format("INITIAL_CANALCONN DOING ->canalIp=%s;canalPort=%s;canalDesti=%s"
                ,config.getCanalIp(), config.getCanalPort(), config.getDestination()));
        CanalConnector connector = config.getCanalConnector();
        log.info("INITIAL_CANALCONN SUCCESS");

        try {
            // 连接部署canal的server
            connector.connect();
            /*
             * 设置需要监听的表
             * 此处如果调用 subscribe() 方法会覆盖instance.properties文件的过滤配置
             * 过滤字符串是正则表达式
             */
            connector.subscribe(config.getSubscribeStr());

            // 回滚到未进行 ack 的地方，下次fetch的时候，可以从最后一个没有 ack 的地方开始拿
            connector.rollback();
        }catch (CanalClientException e){
            log.error("CONNECT_CANAL FAILED ->" + e.getMessage());
            System.exit(1);  // 连接canal服务失败，程序终止
        }
        log.info("CONNECT_CANAL SUCCESS ->******************* CANAL CONNECTED *******************");

        log.info("RUN_LOOP DOING ->ready to get batchSize=" + config.getBatchSize());
        try {
            // 控制没有binlog输入等待时的日志输出情况
            int cycleCount = 0;
            while (true) {
                // message：事件集合
                Message message = connector.getWithoutAck(config.getBatchSize()); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    cycleCount++;
                    if (cycleCount >= 30){
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
                    // 准备执行解析数据并写入
                    insertEvent(entries, schemas);
                    log.info("RUN_LOOP SUCCESS ->insert success");
                }
                connector.ack(batchId); // 提交确认
            }
        }catch (CanalClientException e){
            log.error("RUN_LOOP FAILED ->" + e.getMessage());
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
            System.out.println("TRAVERSE DOING ->traverse schema=" + sourceDatabase);
            for(SingleTable sourceTable : schema.getSingleTables()){

                String sourceTableName = sourceTable.getTableName();
                System.out.println("TRAVERSE DOING ->current table=" + sourceDatabase+"."+sourceTableName);
                String loadTable = sourceTable.getLoadTable();
                log.info(String.format("TRAVERSE DOING ->sourceTable=%s, destinationTable=%s",sourceDatabase+"."+sourceTableName, loadTable));

                // 处理条件表达式引擎
                Expression compiledExp = parseCondition(sourceTable);
                HashMap<String, Object> env;

                int insertSize = sourceTable.getInsertSize();
                log.info(String.format("INSERT PREPARE ->insertSize=%s, mysql connection name=%s ", insertSize, sourceTable.getConnStrName()));
                //
                ArrayList<ColumnInfo> loadColumns = sourceTable.getColumns();
                ConnArgs connArgs = config.getConnArgs().get(sourceTable.getConnStrName());// 获取数据库连接参数

                /*
                 * sql的col部分
                 * 遍历schema，构造插入sql的 column 部分
                 */
                String sqlHead = getSqlHead(loadTable, loadColumns);

                /*
                 * sql的values部分
                 * 构造sql的 values() 部分
                 */
                ArrayList<String> sqlValuesStr = new ArrayList<>();
                /*
                 * 遍历解析出的entry
                 * 放在内层循环是因为考虑到一个entry有可能包含多个表的dml语句
                 */
                long transTag = 0L; // Snowflake 生成的id，标记每个transaction
                for (CanalEntry.Entry entry : entries){
                    // 找到每个操作对应的id
                    if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN ||
                            entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                        if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN){
                            transTag = TransTag.getInstance().nextId();  // Snowflake 创建标记 id
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
                    // 对于delete操作，记录取getBeforeColumnsList的数据
                    if (operate.equalsIgnoreCase("delete")){
                        parseEntry = new ParseEntry(entry, "delete");
                    }else {
                        parseEntry = new ParseEntry(entry);
                    }

                    // 将entry中所有的字段解析出来
                    ArrayList<RowInfo> rowInfoList = parseEntry.getEntryList();
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
                        for (RowInfo rowInfo : rowInfoList){ // 每个 rowinfo 表示一条记录

                            ArrayList<ColumnInfo> ctlCols = parseCtlCol(String.valueOf(transTag), entry, rowInfo);
                            // 将解析出的字段，创建成map
                            HashMap<String, String> colValue = makeKV(rowInfo.getColumnInfos(), ctlCols);
                            // 加载每一个字段到条件筛选
                            env = loadColsForCondition(colValue);
                            // 如果该条记录不符合条件，则过滤掉
                            try {
                                Boolean needed = (Boolean) compiledExp.execute(env);
                                if(!needed) continue;
                            } catch (ExpressionRuntimeException expressionRuntimeException){
                                log.warn("PARSE_EXPRESSION FAILED -> data type maybe wrong ", expressionRuntimeException);
                            } catch (Exception ignored){
                            }
                            /*
                             * 拼接insert的字段值
                             * 此处拼接的是根据需要输出字段匹配出的字段值
                             */
                            String eachRowStr = getSingleValueStr(loadColumns, colValue);
                            sqlValuesStr.add(eachRowStr);
                            if (sqlValuesStr.size() == insertSize){
                                String finalSql = getFinalSql(sqlValuesStr, sqlHead, 1);
                                sqlList.add(finalSql);
                                if (sqlList.size() > 500){
                                    executeInsert(connArgs, sqlList);
                                    sqlList.clear();
                                }
                            }
                        }
                    }
                }
                if (sqlValuesStr.size() != 0) {
                    String finalSql = getFinalSql(sqlValuesStr, sqlHead, 0);
                    sqlList.add(finalSql);
                    if (sqlList.size() > 500){
                        executeInsert(connArgs, sqlList);
                        sqlList.clear();
                    }
                }
                // 最后收尾
                if (sqlList.size() != 0){
                    executeInsert(connArgs, sqlList);
                    sqlList.clear();
                }
            }
        }
    }

    /**
     * 构造每个values() 的()部分
     * @param loadColumns 待插入的字段
     * @param colValue 待插入的字段值
     * @return 一条values()的()字符串
     */
    private static String getSingleValueStr(ArrayList<ColumnInfo> loadColumns, HashMap<String, String> colValue){
        // 构建插入的值的str
        StringBuilder valuesStr = new StringBuilder("(");
        for (ColumnInfo tc : loadColumns) {
            // 对于 null 或者 NOW(6)，不能前后加引号转为字符串，要保留原状
            if (colValue.get(tc.getName().toLowerCase()) == null || colValue.get(tc.getName().toLowerCase()).equalsIgnoreCase("NOW(6)")){
                valuesStr.append(",").append(colValue.get(tc.getName().toLowerCase()));
            }else {
                valuesStr.append(",'").append(colValue.get(tc.getName().toLowerCase())).append("'");
            }
        }
        valuesStr.append(")");

        String eachRowStr = valuesStr.toString().replace("(,", "(");
        System.out.println("PARSE_ENTRY DONE ->colValues=" + eachRowStr);  // 只输出显示，不记录到log
        return eachRowStr;
    }

    /**
     * 构造出sql的前半部分
     * @param loadTable 目标表
     * @param loadColumns 目标表的目标字段
     * @return sqlHead
     */
    private static String getSqlHead(String loadTable, ArrayList<ColumnInfo> loadColumns){
        StringBuilder sqlColsStr = new StringBuilder("REPLACE INTO " + loadTable + "(");
        ArrayList<String> cols = new ArrayList<>();
        for(ColumnInfo ci : loadColumns){
            cols.add(ci.getToCol());
        }
        String colsStr = StringUtils.join(cols, ",");
        System.out.println("INSERT PREPARE ->destination columns = "+colsStr);
        sqlColsStr.append(colsStr).append(") VALUES");

        return sqlColsStr.toString();
    }


    /**
      * 补充控制字段的字段值
      * trans_tag 事务id
      * rec_time 取当前格式化时间
      * procBatch 取事务的执行时间
      * flag 记录操作状态
      * log_rec_size 取rowData的size
      * log_rec_pos 取entry的offset，即position
      * operate 取eventType
      * log_file_name 取binlog文件名的hashCode()
      */
    private static ArrayList<ColumnInfo> parseCtlCol(String transTag, CanalEntry.Entry entry, RowInfo columns){
        ArrayList<ColumnInfo> ctlCol = new ArrayList<>();
        ColumnInfo ci1 = new ColumnInfo();
        ci1.setToCol("trans_tag"); // 记录所在transaction的唯一标记值
        ci1.setValue(transTag);
        ctlCol.add(ci1);
        ColumnInfo ci2 = new ColumnInfo();
        ci2.setToCol("rec_time"); // 记录时间
        ci2.setValue("NOW(6)");
        ctlCol.add(ci2);
        ColumnInfo ci3 = new ColumnInfo();
        ci3.setToCol("log_file_name");  // binlog文件名的hashcode
        ci3.setValue(String.valueOf(entry.getHeader().getLogfileName().hashCode()));
        ctlCol.add(ci3);
        ColumnInfo ci4 = new ColumnInfo();
        ci4.setToCol("log_rec_pos");  // entry的position，后续可用于修复数据
        ci4.setValue(String.valueOf(entry.getHeader().getLogfileOffset()));
        ctlCol.add(ci4);
        ColumnInfo ci5 = new ColumnInfo();
        ci5.setToCol("log_rec_size");  // 每个rowdata的size
        ci5.setValue(String.valueOf(columns.getRowSize()));
        ctlCol.add(ci5);
        ColumnInfo ci6 = new ColumnInfo();
        ci6.setToCol("operate");  // 操作类型：insert、update、delete
        int operateCode = 0;
        try {
            String operateType = CanalEntry.RowChange.parseFrom(entry.getStoreValue()).getEventType().name();
            operateCode = operateType.equalsIgnoreCase("INSERT") ? 0 : operateType.equalsIgnoreCase("UPDATE") ? 1 : 2;
        } catch (InvalidProtocolBufferException e) {
            log.error("PARSE_ENTRY FAILED -> failed to get eventType" + e.getMessage());
        }
        ci6.setValue(String.valueOf(operateCode));
        ctlCol.add(ci6);
        ColumnInfo ci7 = new ColumnInfo();
        ci7.setToCol("proc_batch");  // 事务执行的时间戳
        ci7.setValue(String.valueOf(entry.getHeader().getExecuteTime()));
        ctlCol.add(ci7);
        ColumnInfo ci8 = new ColumnInfo();
        ci8.setToCol("flag");  // 记录的处理状态，默认为 0：待处理
        ci8.setValue("0");
        ctlCol.add(ci8);
        ColumnInfo ci9 = new ColumnInfo();
        ci9.setToCol("modified");  // 记录更新过的字段，形如：,col1,col2,col3,
        ci9.setValue(columns.getUpdatedCols());
        ctlCol.add(ci9);
        return ctlCol;
    }

    /**
     * 将解析binlog得到的字段-字段值处理成键值对放在map中
     * @param columns binlog解析出的字段
     * @return 键值对的map，包含了解析出的所有字段
     */
    private static HashMap<String, String> makeKV(ArrayList<ColumnInfo> columns, ArrayList<ColumnInfo> ctlCols){
        HashMap<String, String> colValue = new HashMap<>();
        for (ColumnInfo ci : columns){

            if (ci.isNull()){
                colValue.put(ci.getName().toLowerCase(), null);
            }else {
                colValue.put(ci.getName().toLowerCase(), ci.getValue());
            }
        }
        for (ColumnInfo ci : ctlCols){
            colValue.put(ci.getToCol().toLowerCase(), ci.getValue());
        }
        return colValue;
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
     */
    private static void executeInsert(ConnArgs connArgs, ArrayList<String> sqlList){

        try {
                System.out.println((String.format("GET_MYSQLCONN DOING ->connect args=%s"
                        ,connArgs.getUserId()+":"+connArgs.getPwd()+"@"+connArgs.getAddress()+":"+connArgs.getPort()+"/"+connArgs.getDatabase())));
            }catch (NullPointerException e){
                // 输入参数中的数据库连接名不对时会报该错
                log.error("GET_MYSQLCONN FAILED -> mysqlConnStrName in the args maybe wrong, check start args");
                System.exit(1);
            }

            MysqlConn mysqlConn;
            try {
                mysqlConn = new MysqlConn(connArgs.getAddress(), connArgs.getPort(), connArgs.getUserId(), connArgs.getPwd(), connArgs.getDatabase());
            }catch (Exception e){
                log.error("GET_MYSQLCONN FAILED ->" + e.getMessage());
                System.exit(1);
                return;
            }
            log.info("GET_MYSQLCONN SUCCESS");
            Statement stmt = mysqlConn.getStmt();
        Connection conn = mysqlConn.getConn();

        for(String sql : sqlList){
            try {
                stmt.addBatch(sql);
            }catch (SQLException e) {
                log.error("INSERT FAILED -> addBatch failed");
                System.exit(1);
            }
        }
        try {
            conn.setAutoCommit(false);
            stmt.executeBatch();
            conn.commit();
            log.info("INSERT SUCCESS");
        }catch (SQLException e) {
            if (e.getErrorCode() == 1146){
                log.error("INSERT FAILED -> MySQLSyntaxError: Table doesn't exist ");
                System.exit(1);
            }else {
                log.error("INSERT FAILED -> MySQLSyntaxError " + e.getErrorCode() + " " + e.getMessage());
                System.exit(1);
            }
        }
        try {
            mysqlConn.close();
        } catch (SQLException e) {
            log.warn("CLOSE_MYSQLCONN FAILED");
        }
    }

    /**
     * 根据sql的cols部分和values部分构造最终的sql
     * @param sqlValuesStr sql的values部分string
     * @param sqlHead sql的columns部分string
     * @param clearTag 是否清除valuesList
     * @return 最终的sql
     */
    private static String getFinalSql(ArrayList<String> sqlValuesStr, String sqlHead, int clearTag){

        StringBuilder fullSql = new StringBuilder(sqlHead);
        String values = StringUtils.join(sqlValuesStr, ",");
        fullSql.append(values).append(";");
        String finalSql = fullSql.toString();
        log.info("INSERT PREPARE ->sql=" + finalSql);
        if (clearTag == 1) {sqlValuesStr.clear();}  // 清空value列表
        return finalSql;
    }
}
