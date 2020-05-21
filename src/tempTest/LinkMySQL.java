package tempTest;


import java.sql.*;

public class LinkMySQL {

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_URL = "jdbc:mysql://192.168.24.101:3306/tobase1?useSSL=false&serverTimezone=UTC";
//    private static final String DB_URL = "jdbc:mysql://111.231.66.20:3306/canalfrombase?useSSL=false&serverTimezone=UTC";
    private static final String USER = "root";
    private static final String PW = "1111";


    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        Connection conn;
        Statement stmt;

        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(DB_URL, USER, PW);
        stmt = conn.createStatement();


        ResultSet res = stmt.executeQuery("select * from totable1 limit 100;");
        while (res.next()){
            String name = res.getString("id");
            System.out.println(name);
        }
        res.close();


//        for(int i = 0; i < 20 ; i++){
//            stmt.execute(String.format("INSERT INTO qcl_test.testcanal(name,age,country,province,city) values(\"dr3i\",%d,\"chna\",1,2);",i));
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