package tempTest;


import java.sql.*;

public class LinkMySQL {

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
//    private static final String DB_URL = "jdbc:mysql://192.168.24.101:3306/tobase1?useSSL=false&serverTimezone=UTC";
    private static final String DB_URL = "jdbc:mysql://192.168.69.178:3306/test?useSSL=false&serverTimezone=UTC";
//    private static final String DB_URL = "jdbc:mysql://111.231.66.20:3306/frombase1?useSSL=false&serverTimezone=UTC";
    private static final String USER = "root";
    private static final String PW = "1111";


    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        Connection conn;
        Statement stmt;

        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(DB_URL, USER, PW);
        stmt = conn.createStatement();


        ResultSet res = stmt.executeQuery("select * from moduleinfo limit 100;");
        while (res.next()){
            String name = res.getString("dau");
            System.out.println(name);
        }
        res.close();


//        for(int i = 0; i < 20 ; i++){
//            stmt.execute("INSERT INTO frombase1.fromtable1(fiducia_id,contract_id,account ,contract_no ,report_date ,report_date_code ,account_flag ,account_type ,name ,\n" +
//                    "cert_type ,cert_no ,mngmt_org_code ,general_type ,busi_detail ,date_opened ,currency ,loan_amount ,flag ,date_closed ,repay_mode,repay_freq ,repay_periods ,\n" +
//                    "guarantee_way ,othe_repay_guar_way ,asset_trand_flag ,fund_sou ,loan_from ,`month` ,sett_date ,account_status \n" +
//                    ") VALUES( 144219031412774782,144219031412774856,\"10B10111110111HN04P4HNFHCS1900041\",\"HN04P4_HNFHCS190004_1\",\"2019-12-31\",31,1,\"D1\",\"田玲\",10,\"43122719851204302X\"\n" +
//                    ",\"B10111000H0001\",1,91,\"2019-09-24\",\"CNY\",840000.0000,0 ,\"2020-09-23\"  ,11 ,03,12 ,4 ,0 ,0 ,04,1 ,\"201912\" ,\"2020-01-24\" ,1);");
////            stmt.execute(String.format("INSERT INTO qcl_test.testcanal(name,age,country,province,city) values(\"dr3i\",%d,\"chna\",1,2);",i));
//            System.out.println("insert " + i + " rows");
//            try {
//                Thread.sleep(5);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }

        stmt.close();
        conn.close();

    }
}