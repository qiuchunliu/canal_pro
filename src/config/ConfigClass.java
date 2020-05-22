package config;

import org.apache.commons.lang.StringUtils;
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


    public ConfigClass(String canalUrl, int batchSize, String xml_url, String conn_str, int sleepDuration) {
        // 配置schema文件
        SAXReader reader = new SAXReader();
        try {
            log.info(String.format("READ_FILE DOING ->reading schema file path=%s", xml_url));
            FileInputStream fileInputStream = new FileInputStream(xml_url);
            org.dom4j.Document read;
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
                return;
            }
        } catch (FileNotFoundException e) {
            log.error("READ_FILE FAILED ->file not found. end the job", e);
            return;
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
            log.error(String.format("PARSE_CANALURL FAILED ->canalUrl=%s\n", canalUrl), e);
            return;
        }

        ConfigClass.sleepDuration = sleepDuration;


        // 配置数据库连接
        try {
            log.info(String.format("PARSE_MYSQLCONN DOING ->conn_str=%s", conn_str));
            for(String s : conn_str.split(",")){
                ConnArgs connArgs = new ConnArgs();
                String conn_name = s.split("=")[0].split("#")[1].trim();
                connArgs.user_id = s.split("=")[1].split("@")[0].split(":")[0];
                connArgs.pwd = s.split("=")[1].split("@")[0].split(":")[1];
                connArgs.address = s.split("=")[1].split("@")[1].split(":")[0];
                connArgs.port = s.split("=")[1].split("@")[1].split(":")[1].split("/")[0];
                try {
                    connArgs.database = s.split("=")[1].split("@")[1].split(":")[1].split("/")[1];
                }catch (IndexOutOfBoundsException e){
                    connArgs.database = "";
                }
                map.put(conn_name, connArgs);
                log.info(String.format("- - ->one connection prepared, url=%s", conn_name + ":" + connArgs.getConUrl()));
            }
            log.info("PARSE_MYSQLCONN SUCCESS");
        }catch (Exception e){
            log.error(String.format("PARSE_MYSQLCONN FAILED ->parse connection string failed, please check, url=%s\n", conn_str), e);
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
        ArrayList<Schema> schemas = new ArrayList<>();
        try {
            log.info("PARSE_SCHEMAS DOING");
            for (Object so: schemaElements) {
                Schema schema = new Schema();
                Element schemaElement = (Element)so;  // 每个schema
                schema.from_database = schemaElement.attributeValue("from_database");
                schema.singleTables = getTables(schemaElement);
                schemas.add(schema);
            }
        }catch (Exception e){
            log.error("PARSE_SCHEMAS FAILED\n", e);
            return schemas;
        }
        log.info("PARSE_SCHEMAS SUCCESS ->"+"schemasNum="+schemas.size());
        return schemas;
    }

    /**
     * 解析xml中每个表的字段
     * @param tb 待处理的表
     * @return tb下的字段列表
     */
    private ArrayList<ColumnInfo> getColumns(Element tb){
        ArrayList<ColumnInfo> columnInfos = new ArrayList<>();
        try {
            List cols = tb.elements();
            for (Object co: cols) {
                ColumnInfo columnInfo = new ColumnInfo();
                Element eco = (Element) co;
                columnInfo.name = eco.attributeValue("fromCol") == null ? eco.attributeValue("name") : eco.attributeValue("fromCol");
                columnInfo.toCol = eco.attributeValue("toCol");
                columnInfos.add(columnInfo);
            }
        }catch (Exception e){
            log.error("PARSE_COLUMNS FAILED ->parse columns in schemas\n", e);
            return columnInfos;
        }
        log.info("PARSE_COLUMNS SUCCESS ->parse columns in schemas");
        return columnInfos;
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
                singleTable.insertSize = Integer.parseInt(tb.attributeValue("insert_size"));
                singleTable.tableName = tb.attributeValue("name");
                singleTable.connStrName = tb.attributeValue("conn_str");
                singleTable.loadTable = tb.attributeValue("to_table");
                singleTable.columns = getColumns(tb);
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
     * 获取过滤条件 subscribe_str
     * @return 过滤 subscribe_str
     */
    public String getSubscribe_tb() {
        ArrayList<String> subscribe_tb = new ArrayList<>();
        List bases = rootElement.elements();
        try {
            for(Object o : bases){
                Element e = (Element)o;
                String from_database = e.attributeValue("from_database");
                List tbs = e.elements();
                for (Object o1: tbs) {
                    Element tb = (Element) o1;
                    String tbname = from_database + "." + tb.attributeValue("name");
                    subscribe_tb.add(tbname);
                }
            }
        }catch (Exception e){
            log.error("PARSE_FILTER FAILED\n", e);
            return StringUtils.join(subscribe_tb,",");
        }
        log.info("PARSE_FILTER SUCCESS ->filter=" + StringUtils.join(subscribe_tb,","));
        return StringUtils.join(subscribe_tb,",");
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
