package config;

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.log4j.Logger;

public class ConfigClass {

    private Element rootElement;
    private HashMap<String, ConnArgs> map = new HashMap<>();
    private static Logger log = Logger.getLogger(ConfigClass.class);

    private static int batchSize;
    private static String canalIp;
    private static int canalPort;
    private static String destination;
    private static int sleepDuration;



    public ConfigClass(String canalUrl, int batchSize, String schemaPath, String mysqlConnStr, int sleepDuration) {
        // 配置schema文件
        SAXReader reader = new SAXReader();
        try {
            log.info(String.format("READ_FILE DOING ->reading schema file path=%s", schemaPath));
            FileInputStream fileInputStream = new FileInputStream(schemaPath);
            Document read;
            try {
                log.info("PROC_FILE DOING");
                read = reader.read(fileInputStream);
                if (read != null) {
                    this.rootElement = read.getRootElement();
                }else {
                    throw new DocumentException();
                }
            } catch (DocumentException e) {
                log.error("PROC_FILE FAILED ->processing of a DOM4J document failed. end the job");
                System.exit(1);
            }
        } catch (FileNotFoundException e) {
            log.error("READ_FILE FAILED ->schema file not found. end the job", e);
            System.exit(1);
        }

        // 配置canal
        try {
            log.info("PARSE_CANALURL DOING");
            ConfigClass.batchSize = batchSize;
            ConfigClass.canalIp = canalUrl.split(":")[0];
            ConfigClass.canalPort = Integer.parseInt(canalUrl.split(":")[1].split("/")[0]);
            ConfigClass.destination = canalUrl.split("/")[1];
            log.info("PARSE_CANALURL SUCCESS");
        }catch (Exception e){
            log.error("PARSE_CANALURL FAILED ->canalUrl=" + canalUrl, e);
            System.exit(1);
        }

        // 循环等待时的duration
        ConfigClass.sleepDuration = sleepDuration;


        // 配置数据库连接
        try {
            log.info("PARSE_MYSQLCONN DOING ->mysqlConnStr=" + mysqlConnStr);
            for(String eachMysqlConnStr : mysqlConnStr.split(",")){
                ConnArgs connArgs = new ConnArgs();
                String mysqlConnStrName = eachMysqlConnStr.split("=")[0].split("#")[1].trim();
                connArgs.userId = eachMysqlConnStr.split("=")[1].split("@")[0].split(":")[0];
                connArgs.pwd = eachMysqlConnStr.split("=")[1].split("@")[0].split(":")[1];
                connArgs.address = eachMysqlConnStr.split("=")[1].split("@")[1].split(":")[0];
                connArgs.port = eachMysqlConnStr.split("=")[1].split("@")[1].split(":")[1].split("/")[0];
                try {
                    connArgs.database = eachMysqlConnStr.split("=")[1].split("@")[1].split(":")[1].split("/")[1];
                }catch (IndexOutOfBoundsException e){
                    connArgs.database = "";
                }
                map.put(mysqlConnStrName, connArgs);
                log.info(String.format("- - ->one connection prepared, url=%s", mysqlConnStrName + ":" + connArgs.getConUrl()));
            }
            log.info("PARSE_MYSQLCONN SUCCESS");
        }catch (Exception e){
            log.error(String.format("PARSE_MYSQLCONN FAILED ->parse connection string failed, please check, url=%s\n", mysqlConnStr), e);
        }

    }


    public int getSleepDuration() {
        return sleepDuration;
    }

    public HashMap<String, ConnArgs> getConnArgs(){
        return map;
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
            log.error("PARSE_SCHEMAS FAILED\n", e);
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
                columnInfo.name = colElement.attributeValue("fromCol") == null ? colElement.attributeValue("sourceTableName") : colElement.attributeValue("fromCol");
                columnInfo.toCol = colElement.attributeValue("toCol");
                columnInfoList.add(columnInfo);
            }
        }catch (Exception e){
            log.error("PARSE_COLUMNS FAILED ->parse columns in schemas\n", e);
            return columnInfoList;
        }
        log.info("PARSE_COLUMNS SUCCESS ->parse columns in schemas");
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
                singleTable.insertSize = Integer.parseInt(tb.attributeValue("insertSize"));
                singleTable.tableName = tb.attributeValue("sourceTableName");
                singleTable.connStrName = tb.attributeValue("mysqlConnStrName");
                singleTable.loadTable = tb.attributeValue("destinationTable");
                singleTable.columns = getColumns(tb);
                singleTable.rowConditions = parseRowConditions(tb.attributeValue("condition"));
                singleTables.add(singleTable);
            }
        }catch (Exception e){
            log.error("PARSE_TABLES FAILED ->parse tables in schemas\n", e);
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
            log.error("PARSE_FILTER FAILED\n", e);
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

    public int getBatchSize(){
        return batchSize;
    }
    public String getDestination(){
        return destination;
    }

}
