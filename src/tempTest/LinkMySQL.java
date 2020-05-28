package tempTest;


import java.sql.*;

public class LinkMySQL {

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
//    private static final String DB_URL = "jdbc:mysql://192.168.24.101:3306/tobase1?useSSL=false&serverTimezone=UTC";
//    private static final String DB_URL = "jdbc:mysql://192.168.69.178:3306/?useSSL=false&serverTimezone=UTC";
    private static final String DB_URL = "jdbc:mysql://111.231.66.20:3306/fromsdfbase?useSSL=false&serverTimezone=UTC&characterEncoding=utf-8";
//    private static final String DB_URL = "jdbc:mysql://192.168.0.159:30115/frombase?useSSL=false&serverTimezone=UTC";
    private static final String USER = "mycanal";
    private static final String PW = "1111";


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


        for(int i = 0; i < 2 ; i++){
            stmt.execute("INSERT INTO fromsdfbase.borrower (\n" +
                    "  `borrower_uuid`,\n" +
                    "  `borrower_name`,\n" +
                    "  `certificate_type`,\n" +
                    "  `certificate_no`,\n" +
                    "  `gmt_create`\n" +
                    "  ) VALUES(uuid(),\n" +
                    "  \"中文\",\n" +
                    "  0,\n" +
                    "  \"50022719951023043X\",\n" +
                    "  \"2018-12-04 15:14:59.0\"\n" +
                    "  );");
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