package main.scripts;


import org.apache.log4j.Logger;

public class MainStart {
    private static Logger log = Logger.getLogger(MainStart.class);

    /**
     * 启动任务时传参
     * -DcanalUrl=IP:PORT/DESTINATION
     * -DbaseConn=mysql|klingon=mycanal:1111@111.231.66.20:3306/canaltobase,mysql|yunxin=canal:1111@192.168.24.11:3306/canaltobase,mysql|zhenxin=mycanal:1111@192.168.24.101:3306/canaltobase
     * -DbatchSize=2000
     * -DxmlPath=配置文件的路径，与jar包同一个目录
     */
    public static void main(String[] args) {

        String canalUrl = System.getProperty("canalUrl");
        String baseConn = System.getProperty("baseConn");
        String batchSize = System.getProperty("batchSize");
        String xmlPath = System.getProperty("xmlPath");
        if (canalUrl ==null || baseConn == null || batchSize == null || xmlPath == null) {
            log.error("some arg is wrong, please check ");
            System.out.println("args example\n");
            System.out.println("-DcanalUrl=IP:PORT/DESTINATION");
            System.out.println("-DbaseConn=mysql|klingon=mycanal:1111@111.231.66.20:3306/canaltobase");
            System.out.println("-DbatchSize=2000");
            System.out.println("-DxmlPath=配置文件的路径，与jar包同一个目录");
            return;
        }

        log.info("THE JOB IS RUNNING");
        String conn_str = "mysql|klingon=mycanal:1111@111.231.66.20:3306/canaltobase" +
                ",mysql|yunxin=canal:1111@192.168.24.11:3306/canaltobase," +
                "mysql|zhenxin=mycanal:1111@192.168.24.101:3306/canaltobase";
        RunJob runJob = new RunJob(canalUrl, Integer.valueOf(batchSize),xmlPath, conn_str);
        runJob.doit();
        log.info("THE JOB IS DONE");
    }
}
