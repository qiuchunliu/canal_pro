package tempTest;


import java.sql.*;

public class LinkMySQL {

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
//    private static final String DB_URL = "jdbc:mysql://192.168.24.101:3306/tobase1?useSSL=false&serverTimezone=UTC";
//    private static final String DB_URL = "jdbc:mysql://192.168.69.178:3306/?useSSL=false&serverTimezone=UTC";
//    private static final String DB_URL = "jdbc:mysql://111.231.66.20:3306/frombase1?useSSL=false&serverTimezone=UTC";
    private static final String DB_URL = "jdbc:mysql://192.168.0.159:30115/frombase?useSSL=false&serverTimezone=UTC";
    private static final String USER = "5v_user";
    private static final String PW = "dec44ad";


    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        Connection conn;
        Statement stmt;

        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(DB_URL, USER, PW);
        stmt = conn.createStatement();


//        ResultSet res = stmt.executeQuery("select * from con_fin_prt_inc limit 100;");
//        while (res.next()){
//            String name = res.getString("contract_uuid");
//            System.out.println(name);
//        }
//        res.close();


        for(int i = 41; i < 70 ; i++){
            stmt.execute(String.format("INSERT INTO frombase.con_fin_prt_inc (\n" +
                    "  `loan_id`,`contract_uuid`,`version_no`,`core_finger_print`,`installments`,`create_time`,\n" +
                    "  `repeated`,`time_flag`,`version_order`) VALUES (\n" +
                    "144217791992433235,\n" +
                    "uuid(),\n" +
                    "41,\n" +
                    "\"2f07ec4167896f264f11e3a7a7eec19d\",\n" +
                    "%s,\n" +
                    "\"2019-04-09 17:58:34.0\",\n" +
                    "1,\n" +
                    "1566553320,\n" +
                    "1\n" +
                    ");",i));
//            stmt.execute(String.format("INSERT INTO qcl_test.testcanal(name,age,country,province,city) values(\"dr3i\",%d,\"chna\",1,2);",i));
            System.out.println("insert " + i + " rows");
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        stmt.close();
        conn.close();

    }
}