package tempTest;


import java.sql.*;

public class LinkMySQL {

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_URL = "jdbc:mysql://192.168.0.159:30115/qcl_test?useSSL=false&serverTimezone=UTC";
//    private static final String DB_URL = "jdbc:mysql://111.231.66.20:3306/canalfrombase?useSSL=false&serverTimezone=UTC";
    private static final String USER = "5v_user";
    private static final String PW = "dec44ad";


    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        Connection conn;
        Statement stmt;

        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(DB_URL, USER, PW);
        stmt = conn.createStatement();


        ResultSet res = stmt.executeQuery("select * from testcanal limit 100;");
        while (res.next()){
            String name = res.getString("id");
            System.out.println(name);
        }
        res.close();


//        for(int i = 0; i < 2000 ; i++){
//            stmt.execute(String.format("INSERT INTO canalfrombase.canalfromtable(name,age,country,province,city) values(\"dr3i\",%d,\"chna\",1,2);",i));
//            System.out.println("insert " + i + " rows");
//            Thread.sleep(5);
//        }

        stmt.close();
        conn.close();

    }
}