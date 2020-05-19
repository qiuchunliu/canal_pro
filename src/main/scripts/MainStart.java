package main.scripts;


import org.dom4j.DocumentException;
import org.apache.log4j.Logger;
import java.io.IOException;

public class MainStart {
    private static Logger log = Logger.getLogger(MainStart.class);
    public static void main(String[] args) throws DocumentException, IOException {
        log.info("START THE JOB");
//        RunJob runJob = new RunJob(args[0]);
        String conn_str = "mysql|klingon=mycanal:1111@111.231.66.20:3306/canaltobase" +
                ",mysql|yunxin=canal:1111@192.168.24.11:3306/canaltobase," +
                "mysql|zhenxin=mycanal:1111@192.168.24.101:3306/canaltobase";
        RunJob runJob = new RunJob("main/resources/schema.xml",
                "main/resources/config.properties", conn_str);
        runJob.doit();
        log.info("THE JOB IS DONE");
    }
}
