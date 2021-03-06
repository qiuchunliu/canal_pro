package config;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import org.apache.commons.lang.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import beans.ColumnInfo;
import beans.ConnArgs;
import beans.Schema;
import beans.SingleTable;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.log4j.Logger;

/**
 * 全局配置类
 */
public class ConfigClass {

    private Element rootElement;
    private HashMap<String, ConnArgs> mysqlConns;
    private Logger log = Logger.getLogger(ConfigClass.class);
    private int batchSize;
    private String canalIp;
    private int canalPort;
    private String destination;
    private int sleepDuration;

    public ConfigClass(String canalUrl, int batchSize, String schemaPath, String mysqlConnStr, int sleepDuration) {
        // 配置schema文件
        this.rootElement = parseSchemaFile(schemaPath);
        // canal配置参数
        try {
            log.info("PARSE_CANALURL DOING");
            setBatchSize(batchSize);
            setCanalIp(canalUrl.split(":")[0]);
            setCanalPort(Integer.parseInt(canalUrl.split(":")[1].split("/")[0]));
            setDestination(canalUrl.split("/")[1]);
            log.info("PARSE_CANALURL SUCCESS");
        }catch (Exception e){
            log.error("PARSE_CANALURL FAILED ->canalUrl=" + canalUrl + " ," + e.getMessage());
            System.exit(1);
        }

        // 循环等待时的duration
        setSleepDuration(sleepDuration);

        // 配置数据库连接
        this.mysqlConns = setMysqlConnStr(mysqlConnStr);
    }

    private void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    private void setCanalIp(String canalIp) {
        this.canalIp = canalIp;
    }

    private void setCanalPort(int canalPort) {
        this.canalPort = canalPort;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    private void setSleepDuration(int sleepDuration) {
        this.sleepDuration = sleepDuration;
    }

    /**
     * parse schemaFile
     * @param schemaPath absolute file path of schema
     * @return the rootElement of schema
     */
    private Element parseSchemaFile(String schemaPath){
        SAXReader reader = new SAXReader();
        Element rootElement = null;
        try {
            log.info(String.format("READ_FILE DOING ->reading schema file path=%s", schemaPath));
            FileInputStream fileInputStream = new FileInputStream(schemaPath);
            Document read;
            try {
                log.info("PROC_FILE DOING");
                read = reader.read(fileInputStream);
                if (read != null) {
                    rootElement = read.getRootElement();
                }else {
                    throw new DocumentException();
                }
            } catch (DocumentException e) {
                log.error("PROC_FILE FAILED ->processing of a DOM4J document failed. end the job, " + e.getMessage());
                System.exit(1);
            }
        } catch (FileNotFoundException e) {
            log.error("READ_FILE FAILED ->schema file not found. end the job. maybe the path is wrong: " + schemaPath);
            System.exit(1);
        }
        return rootElement;
    }

    /**
     * parse mysql connection string from input args
     * @param mysqlConnStr mysqlConnStr given when input
     * @return map of each mysqlConnectionStr
     */
    private HashMap<String, ConnArgs> setMysqlConnStr(String mysqlConnStr){
        HashMap<String, ConnArgs> mysqlConns = new HashMap<>();
        try {
            log.info("PARSE_MYSQLCONN DOING ->mysqlConnStr=" + mysqlConnStr);
            for(String eachMysqlConnStr : mysqlConnStr.split(",")){
                ConnArgs connArgs = new ConnArgs();
                String mysqlConnStrName = eachMysqlConnStr.split("=")[0].split("\\|")[1].trim();
                connArgs.setUserId(eachMysqlConnStr.split("=")[1].split("@")[0].split(":")[0]);
                connArgs.setPwd(eachMysqlConnStr.split("=")[1].split("@")[0].split(":")[1]);
                connArgs.setAddress(eachMysqlConnStr.split("=")[1].split("@")[1].split(":")[0]);
                connArgs.setPort(eachMysqlConnStr.split("=")[1].split("@")[1].split(":")[1].split("/")[0]);
                try {
                    connArgs.setDatabase(eachMysqlConnStr.split("=")[1].split("@")[1].split(":")[1].split("/")[1]);
                }catch (IndexOutOfBoundsException e){
                    connArgs.setDatabase("");
                }
                mysqlConns.put(mysqlConnStrName, connArgs);
                log.info(String.format("- - ->one connection prepared, url=%s", mysqlConnStrName + ":" + connArgs.getConUrl()));
            }
            log.info("PARSE_MYSQLCONN SUCCESS");
        }catch (Exception e){
            log.warn("PARSE_MYSQLCONN FAILED ->parse connection string failed, please check, url=%s" + mysqlConnStr + " " + e.getMessage());
        }
        return mysqlConns;
    }


    /**
     * 解析xml中的库
     * @return xml中包含的库的列表
     */
    public ArrayList<Schema> getSchemas(){
        List schemaElements = rootElement.elements();  // schema列表
        ArrayList<Schema> schemaList = new ArrayList<>();
        try {
            log.info("PARSE_SCHEMAS DOING");
            for (Object so: schemaElements) {
                Schema schema = new Schema();
                Element schemaElement = (Element)so;  // 每个schema
                schema.sourceDatabase = schemaElement.attributeValue("sourceDatabase");
                schema.singleTables = getTables(schemaElement);
                schemaList.add(schema);
            }
        }catch (Exception e){
            log.warn("PARSE_SCHEMAS FAILED ->" + e.getMessage());
            return schemaList;
        }
        log.info("PARSE_SCHEMAS SUCCESS ->"+"schemasNum="+schemaList.size());
        return schemaList;
    }

    /**
     * 解析schema中每个表的字段
     * @param tb 待处理的表
     * @return tb下的字段列表
     */
    private ArrayList<ColumnInfo> getColumns(Element tb){
        ArrayList<ColumnInfo> columnInfoList = new ArrayList<>();
        try {
            List cols = tb.elements();
            for (Object col: cols) {
                ColumnInfo columnInfo = new ColumnInfo();
                Element colElement = (Element) col;
                columnInfo.setName(colElement.attributeValue("fromCol") == null ? colElement.attributeValue("toCol") : colElement.attributeValue("fromCol"));
                columnInfo.setToCol(colElement.attributeValue("toCol"));
                columnInfoList.add(columnInfo);
            }
        }catch (Exception e){
            log.warn("PARSE_COLUMNS FAILED ->parse columns in schemas failed, " + e.getMessage());
            return columnInfoList;
        }
        return columnInfoList;
    }

    /**
     * 解析xml文件中每个库下的表
     * @param schemaElement 待处理的库
     * @return 该库下的表的列表
     */
    private ArrayList<SingleTable> getTables(Element schemaElement){

        List elements = schemaElement.elements();
        ArrayList<SingleTable> singleTables = new ArrayList<>();
        try {
            for(Object to: elements){
                SingleTable singleTable = new SingleTable();
                Element tb = (Element)to;
                singleTable.setInsertSize(Integer.parseInt(tb.attributeValue("insertSize")));
                singleTable.setTableName(tb.attributeValue("sourceTableName"));
                singleTable.setConnStrName(tb.attributeValue("mysqlConnStrName"));
                singleTable.setLoadTable(tb.attributeValue("destinationTable"));
                singleTable.setColumns(getColumns(tb));
                singleTable.setRowConditions(parseRowConditions(tb.attributeValue("condition")));
                singleTables.add(singleTable);
            }
        }catch (Exception e){
            log.warn("PARSE_TABLES FAILED ->parse tables in schemas, " + e.getMessage());
            return singleTables;
        }
        log.info("PARSE_TABLES SUCCESS ->parse tables in schemas");
        return singleTables;
    }

    /**
     * 根据schema中的where条件构造出表达式引擎
     * @param initialCondition schema中的条件属性值
     * @return 用于表达式引擎的条件字符串
     */
    private String parseRowConditions(String initialCondition){
        String regConditionStr;
        String replace1 = initialCondition.replace(">=", "biggerThan").replace("<=", "smallerThan").replace("!=", "unEqual");
        String replace2 = replace1.replace(" and ", " && ").replace(" AND ", " && ").replace(" or ", " || ").replace(" OR ", " || ").replace("=", " == ");
        String replace3 = replace2.replace("biggerThan", ">=").replace("smallerThan", "<=").replace("unEqual", "!=");
        regConditionStr = replace3.replace(" like ", " =~ ").replace(" LIKE ", " =~ ").replace("\"%", "/.*").replace("%\"", ".*/").replace("\'%", "/.*").replace("%\'", ".*/");
        return regConditionStr;
    }

    /**
     * 获取过滤条件 subscribe_str
     * @return 过滤 subscribe_str
     */
    public String getSubscribeStr() {
        ArrayList<String> subscribeStr = new ArrayList<>();
        List bases = rootElement.elements();
        try {
            for(Object o : bases){
                Element e = (Element)o;
                String from_database = e.attributeValue("sourceDatabase");
                List tbs = e.elements();
                for (Object o1: tbs) {
                    Element tb = (Element) o1;
                    String tbname = from_database + "." + tb.attributeValue("sourceTableName");
                    subscribeStr.add(tbname);
                }
            }
        }catch (Exception e){
            log.warn("PARSE_FILTER FAILED ->" + e.getMessage());
            return StringUtils.join(subscribeStr,",");
        }
        log.info("PARSE_FILTER SUCCESS ->filter=" + StringUtils.join(subscribeStr,","));
        return StringUtils.join(subscribeStr,",");
    }

    /**
     * 返回canal的serverIp
     * @return Ip
     */
    public String getCanalIp(){
        return canalIp;
    }

    /**
     * 返回canal的port
     * @return port
     */
    public int getCanalPort(){
        return canalPort;
    }

    /**
     * 返回每次读取binlog的条数
     * @return batchSize
     */
    public int getBatchSize(){
        return batchSize;
    }

    /**
     * 设定当没有新增binlog时的等待时长
     * @return sleepDuration
     */
    public int getSleepDuration() {
        return sleepDuration;
    }

    /**
     * 返回数据库连接
     * @return mysqlConns
     */
    public HashMap<String, ConnArgs> getConnArgs(){
        return mysqlConns;
    }

    public String getDestination(){
        return destination;
    }

    public CanalConnector getCanalConnector(){
        CanalConnector connector = null;
        try {
            connector = CanalConnectors.newSingleConnector(
                    new InetSocketAddress(
                            getCanalIp(),  // canal的ip
                            getCanalPort()  // canal的端口
                    ),
                    getDestination(),  // canal的destination
                    "",  // mysql中配置的canal的 user
                    ""   // mysql中配置的canal的 password
            );
        }catch (Exception e){
            log.error("INITIAL_CANALCONN FAILED ->" + e.getMessage());
            System.exit(1);
        }
        return connector;

    }

}
