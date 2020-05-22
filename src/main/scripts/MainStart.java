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

        //// 打jar包时解注
//        String canalUrl = System.getProperty("canalUrl");
//        String mysqlConnStr = System.getProperty("mysqlConnStr");
//        String batchSize = System.getProperty("batchSize");
//        String schemaPath = System.getProperty("schemaPath");
//        int sleepDuration = System.getProperty("sleepDur");
//        if (canalUrl ==null || mysqlConnStr == null || batchSize == null || schemaPath == null) {
//            log.error("some arg is wrong or missed, please check ");
//            System.out.println("args example\n");
//            System.out.println("-DcanalUrl=IP:PORT/DESTINATION");
//            System.out.println("-DmysqlConnStr=mysql#klingon=mycanal:1111@111.231.66.20:3306/canaltobase");
//            System.out.println("-DbatchSize=2000");
//            System.out.println("-DxmlPath=配置文件的绝对路径\n");
//            System.out.println("-DsleepDur=等待时间\n");
//            return;
//        }


////         打jar包时加注
        String canalUrl ;
        String mysqlConnStr ;
        String batchSize ;
        String schemaPath ;
        int sleepDuration;
        canalUrl = "111.231.66.20:11111/example1";
        mysqlConnStr =
                "mysql#base20=mycanal:1111@111.231.66.20:3306/tobase3" +
                ",mysql#base101=root:1111@192.168.24.101:3306/tobase1" +
//                ",mysql#base11=root:1111@192.168.69.178:3306/tobase2";
                ",mysql#base11=root:1111@192.168.24.11:3306/tobase2";
        batchSize = "1000";
        schemaPath = "D:\\programs\\canal_pro\\src\\main\\resources\\schema.xml";
        sleepDuration = 1000;


        log.info("INITIAL_JOB DOING");
        RunJob runJob = new RunJob(canalUrl, Integer.valueOf(batchSize),schemaPath, mysqlConnStr, sleepDuration);
        log.info("INITIAL_JOB SUCCESS");
        runJob.run();
        log.info("RUN_CORE SUCCESS");
        log.info("END_JOB DONE ->******************* THE JOB IS DONE *******************");
    }
}
