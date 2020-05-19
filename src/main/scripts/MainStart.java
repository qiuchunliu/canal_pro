package main.scripts;


import org.apache.log4j.Logger;

public class MainStart {
    private static Logger log = Logger.getLogger(MainStart.class);
    public static void main(String[] args) {
        log.info("START THE JOB");
        String conn_str = "mysql|klingon=mycanal:1111@111.231.66.20:3306/canaltobase" +
                ",mysql|yunxin=canal:1111@192.168.24.11:3306/canaltobase," +
                "mysql|zhenxin=mycanal:1111@192.168.24.101:3306/canaltobase";
        RunJob runJob = new RunJob("main/resources/schema.xml",
                "main/resources/config.properties", conn_str);
        runJob.doit();
        log.info("JOB IS DONE");
    }
}
