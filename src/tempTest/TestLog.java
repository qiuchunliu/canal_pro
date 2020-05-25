package tempTest;


import org.apache.log4j.Logger;

import java.util.Date;

public class TestLog {

    public static void main(String[] args) {
//        Logger log = Logger.getLogger(TestLog.class);
//        log.info("info message");
//        log.error("error message");

//        System.out.println("mysql-bin.000002".hashCode());
        System.out.println(System.currentTimeMillis());
        System.out.println(new Date().getTime());
    }
}
