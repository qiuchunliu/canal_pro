package tempTest;


import java.sql.*;

public class LinkMySQL {

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_URL = "jdbc:mysql://111.231.66.20:3306/canalfrombase?useSSL=false&serverTimezone=UTC";
    private static final String USER = "mycanal";
    private static final String PW = "1111";


    public static void main(String[] args) throws ClassNotFoundException, SQLException, InterruptedException {

        Connection conn;
        Statement stmt;

        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(DB_URL, USER, PW);
        stmt = conn.createStatement();


//        ResultSet res = stmt.executeQuery("select * from canalfromtable limit 100;");
//        while (res.next()){
//            String name = res.getString("id");
//            System.out.println(name);
//        }
//        res.close();


        for(int i = 0; i < 50 ; i++){
            stmt.execute(String.format("INSERT INTO canalfrombase.canalfromtable(name,age,country,province,city) values(\"dr3i\",%d,\"chna\",1,2);",i));
            System.out.println("insert " + i + " rows");
            Thread.sleep(1);
        }

        stmt.close();
        conn.close();

    }
}