package main.scripts;


import org.apache.log4j.Logger;

public class MainStart {
    private static Logger log = Logger.getLogger(MainStart.class);

    /**
     * 启动任务时传参
     * -DcanalUrl=IP:PORT/DESTINATION
     * -DmysqlConnStr=mysql#klingon=mycanal:1111@111.231.66.20:3306/canaltobase,another one
     * -DbatchSize=2000
     * -DxmlPath=配置文件的绝对路径
     * -DsleepDur=等待时间
     */
    public static void main(String[] args) {
        log.info("BEGIN_JOB DOING ->******************* THE JOB IS RUNNING *******************");

        // 打jar包时解注
//        String canalUrl = System.getProperty("canalUrl");
//        String mysqlConnStr = System.getProperty("mysqlConnStr");
//        String batchSize = System.getProperty("batchSize");
//        String schemaPath = System.getProperty("schemaPath");
//        String sleepDuration = System.getProperty("sleepDur");
//        if (canalUrl ==null || mysqlConnStr == null || batchSize == null || schemaPath == null || sleepDuration == null) {
//            log.error("some arg is wrong or missed, please check ");
//            System.out.println("args example\n");
//            System.out.println("-DcanalUrl=IP:PORT/DESTINATION");
//            System.out.println("-DmysqlConnStr=数据库类型|连接名=用户名:密码@数据库IP:端口/目标库(如有多个，','分隔)");
//            System.out.println("-DbatchSize=每次get的binlog条数");
//            System.out.println("-DschemaPath=配置文件的绝对路径\n");
//            System.out.println("-DsleepDuration=等待时间\n");
//            System.exit(1);
//        }

//        打jar包时加注
        String canalUrl;
        String mysqlConnStr;
        String batchSize;
        String schemaPath ;
        String sleepDuration;
        canalUrl = "111.231.66.20:11111/example1";
        mysqlConnStr = "mysql|base20=mycanal:1111@111.231.66.20:3306/tosdfbase";
        batchSize = "5000";
        schemaPath = "D:\\programs\\canal_pro\\src\\main\\resources\\schema1.xml";
        sleepDuration = "1000";


        log.info("INITIAL_JOB DOING");
        RunJob runJob = new RunJob(canalUrl, Integer.valueOf(batchSize),schemaPath, mysqlConnStr, Integer.parseInt(sleepDuration));
        log.info("INITIAL_JOB SUCCESS");
        runJob.run();
        log.info("RUN_CORE SUCCESS");
        log.info("END_JOB DONE ->******************* THE JOB IS DONE *******************");
    }
}
