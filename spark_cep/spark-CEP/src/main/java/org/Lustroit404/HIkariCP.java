package org.Lustroit404;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.Statement;

public class HIkariCP {
    public static void main(String[] args){
        HikariDataSource dataSource=new HikariDataSource();
        dataSource.setDataSourceProperties(new java.util.Properties());

        try(Connection conn= dataSource.getConnection();
        Statement stmt=conn.createStatement()){
            stmt.executeUpdate("insert into table(columns) values ('')");
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            dataSource.close();
        }
    }
}
