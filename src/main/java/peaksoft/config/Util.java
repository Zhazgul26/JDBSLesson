package peaksoft.config;


import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Util {
    private final static String url= "jdbc:postgresql://localhost:5432/JDBSLesson";
    private final static String username ="postgres";
    private final static String password = "1234";


      public static Connection getConnection(){
          Connection connection = null;
        try {
             connection = DriverManager.getConnection(url,username,password);
            System.out.println("fnsdjfihs");
        }catch (SQLException e){
            System.out.println(e.getMessage());
        }
        return connection;
    }

}
