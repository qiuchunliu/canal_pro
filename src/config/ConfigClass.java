package config;

import org.apache.commons.lang.StringUtils;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import beans.ColumnInfo;
import beans.ConnArgs;
import beans.Schema;
import beans.SingleTable;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import org.apache.log4j.Logger;

public class ConfigClass {

    private Element rootElement;
    private Properties properties;
    private HashMap<String, ConnArgs> map = new HashMap<>();
    private static Logger log = Logger.getLogger(ConfigClass.class);

    private static int batchSize;
    private static String canalIp;
    private static int canalPort;
    private static String destination;


    public ConfigClass(String canalUrl, int batchSize, String xml_url, String conn_str) {
        // 配置xml
        SAXReader reader = new SAXReader();
        InputStream xml_in = ConfigClass.class.getClassLoader().getResourceAsStream(xml_url);
        org.dom4j.Document read;
        try {
            log.info(String.format("loading xml @ %s", xml_url));
            read = reader.read(xml_in);
            if (read != null) {
                this.rootElement = read.getRootElement();
            }else {
                throw new DocumentException();
            }
        } catch (DocumentException e) {
            log.info(".xml missed", e);
            e.printStackTrace();
        }

        // 配置properties
        ConfigClass.batchSize = batchSize;
        ConfigClass.canalIp = canalUrl.split(":")[0];
        ConfigClass.canalPort = Integer.parseInt(canalUrl.split(":")[1].split("/")[0]);
        ConfigClass.destination = canalUrl.split("/")[1];


//        Properties properties = new Properties();
//        InputStream in = ConfigClass.class.getClassLoader().getResourceAsStream(propUrl);
//        try {
//            log.info(String.format("loading properties @ %s",propUrl));
//            properties.load(in);
//        } catch (IOException e) {
//            log.error(".properties is wrong", e);
//            e.printStackTrace();
//        }
//        this.properties = properties;

        // 配置数据库连接
        for(String s : conn_str.split(",")){
            ConnArgs connArgs = new ConnArgs();
            String conn_name = s.split("=")[0].split("\\|")[1].trim();
            connArgs.user_id = s.split("=")[1].split("@")[0].split(":")[0];
            connArgs.pwd = s.split("=")[1].split("@")[0].split(":")[1];
            connArgs.address = s.split("=")[1].split("@")[1].split(":")[0];
            connArgs.port = s.split("=")[1].split("@")[1].split(":")[1].split("/")[0];
            connArgs.database = s.split("=")[1].split("@")[1].split(":")[1].split("/")[1];
            map.put(conn_name, connArgs);
            log.info(String.format("one connection prepared : %s", conn_name + ":" + connArgs.getConUrl()));
        }
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

        for (Object so: schemaElements) {
            Schema schema = new Schema();
            Element schemaElement = (Element)so;  // 每个schema
            schema.from_database = schemaElement.attributeValue("from_database");
            schema.singleTables = getTables(schemaElement);
            schemas.add(schema);
        }
        log.info(schemas.size() + " schemas parsed out");
        return schemas;
    }

    /**
     * 解析xml中每个表的字段
     * @param tb 待处理的表
     * @return tb下的字段列表
     */
    private ArrayList<ColumnInfo> getColumns(Element tb){
        ArrayList<ColumnInfo> columnInfos = new ArrayList<>();
        List cols = tb.elements();
        for (Object co: cols) {
            ColumnInfo columnInfo = new ColumnInfo();
            Element eco = (Element) co;
            columnInfo.name = eco.attributeValue("fromCol") == null ? eco.attributeValue("name") : eco.attributeValue("fromCol");
            columnInfo.toCol = eco.attributeValue("toCol");
            columnInfos.add(columnInfo);
        }
        log.info("parsed columns of table in xml done");
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
        for(Object to: elements){
            SingleTable singleTable = new SingleTable();
            Element tb = (Element)to;
            singleTable.insertSize = Integer.parseInt(tb.attributeValue("insert_size"));
            singleTable.tableName = tb.attributeValue("name");
            singleTable.conn_str_name = tb.attributeValue("conn_str");
            singleTable.loadTable = tb.attributeValue("to_table");
            singleTable.columns = getColumns(tb);
            singleTables.add(singleTable);
        }
        log.info("parsed tables in xml");
        return singleTables;
    }

    /**
     * 获取过滤条件 subscribe_str
     * @return 过滤 subscribe_str
     */
    public String getSubscribe_tb() {
        ArrayList<String> subscribe_tb = new ArrayList<>();
        List bases = rootElement.elements();
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
        log.info(String.format("subscribes = %s", StringUtils.join(subscribe_tb,",")));
        return StringUtils.join(subscribe_tb,",");
    }

    /**
     * 返回canal的serverIp
     * @return Ip
     */
    public String getCanalIp(){
        return canalIp;
//        return properties.getProperty("canal_server_ip");
    }

    /**
     * 返回canal的port
     * @return port
     */
    public int getCanalPort(){
        return canalPort;
//        return Integer.parseInt(properties.getProperty("canal_server_port"));
    }

    /**
     * jdbc driver
     * @return driver
     */
    public String getJDBCDriver(){
        return "com.mysql.jdbc.Driver";
    }

    public int getBatchSize(){
        return batchSize;
//        return Integer.parseInt(properties.getProperty("get_batch_size"));
    }
    public String getDestination(){
        return destination;
//        return properties.getProperty("canal_destination");
    }

}
