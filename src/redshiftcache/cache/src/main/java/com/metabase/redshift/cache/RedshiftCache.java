package com.metabase.redshift.cache;

import java.sql.*;
import java.util.Properties;
import com.amazon.redshift.jdbc42.*;
import com.Tester.*;

public class RedshiftCache {
    
	public static void main(String args[])
	{
		Connection conn = null;
        Statement stmt = null;
        try{
        	
        	String fuu = System.getenv("YOLO");
        	System.out.println(fuu);
        	//Class.forName("com.amazon.redshift.jdbc42.Driver");
           //Open a connection and define properties.
           System.out.println("Connecting to database...");
           Properties props = new Properties();

           //Uncomment the following line if using a keystore.
           props.setProperty("ssl", "true");  
           props.setProperty("sslmode", "verify-full");
           //props.setProperty("sslfactory", "org.postgresql.ssl.NonValidatingFactory");
           props.setProperty("user", "metabase_ro");
           props.setProperty("password", "WPauw2B&vwpcef2ewRtYbjjodXWG*E");
           //props.setProperty("sslcert", "/Users/akreuziger/Downloads/redshift-ca-bundle.pem");
          // props.setProperty("sslrootcert", "/Users/akreuziger/Downloads/redshift-ca-bundle.pem");
           conn = DriverManager.getConnection("jdbc:redshift://convoy-dw.cemwvbpl0zlp.us-west-2.redshift.amazonaws.com:5439/convoy", props);
        
           // Pull the last time each table was written to
           
           
           //Try a simple query.
           System.out.println("Listing system tables...");
           stmt = conn.createStatement();
           String sql;
           /*
           sql = "WITH lastupdate AS ( SELECT tbl, MAX(endtime) AS last_updated FROM stl_insert GROUP BY tbl)\n" + 
           		"SELECT svv_table_info.schema, svv_table_info.\"table\", lastupdate.last_updated FROM lastupdate JOIN svv_table_info ON lastupdate.tbl = svv_table_info.table_id ORDER BY schema, \"table\"";
           ResultSet rs = stmt.executeQuery(sql);
           
           //Get the data from the result set.
           while(rs.next()){
              //Retrieve two columns.
              String table_name = rs.getString("table_name");
              String table_id = rs.getString("tbl");
              String last_update = rs.getString("last_updated");
              
              //Display values.
              System.out.print(String.format("%s (%s):\t\t%s", table_name, table_id.toString(), last_update.toString()));
              //System.out.println(", Name: " + name);
           }*/
           ResultSet rs;
           	if(false)
           	{
	           sql = "SELECT schema AS schema, \"table\" AS tbl FROM svv_table_info  WHERE schema = 'src' ORDER BY tbl";
	            rs = stmt.executeQuery(sql);
	           
	           //Get the data from the result set.
	           while(rs.next()){
	              //Retrieve two columns.
	              String schema = rs.getString("schema");
	              String table = rs.getString("tbl");
	              
	              //Display values.
	              System.out.println(String.format("%s %s", schema, table));
	              //System.out.println(", Name: " + name);
	           }
           	}
           	else
           	{
           		sql = "WITH lastupdate AS ( SELECT tbl, MAX(endtime) AS last_updated FROM stl_insert GROUP BY tbl)\n" + 
                   		"SELECT svv_table_info.schema, svv_table_info.\"table\", lastupdate.last_updated FROM lastupdate JOIN svv_table_info ON lastupdate.tbl = svv_table_info.table_id ORDER BY schema, \"table\"";
           		 rs = stmt.executeQuery(sql);
           		ResultSetMetaData rsmd = rs.getMetaData();
           		int columnsNumber = rsmd.getColumnCount();
           		while (rs.next()) {
           		    for (int i = 1; i <= columnsNumber; i++) {
           		        if (i > 1) System.out.print(",  ");
           		        String columnValue = rs.getString(i);
           		        System.out.print(columnValue + ":" + rsmd.getColumnName(i) + ", ");
           		    }
           		    System.out.println("");
           		}
           	}
           rs.close();
           stmt.close();
           conn.close();
        }catch(Exception ex){
           //For convenience, handle all errors here.
           ex.printStackTrace();
        }finally{
           //Finally block to close resources.
           try{
              if(stmt!=null)
                 stmt.close();
           }catch(Exception ex){
           }// nothing we can do
           try{
              if(conn!=null)
                 conn.close();
           }catch(Exception ex){
              ex.printStackTrace();
           }
        }
        System.out.println("Finished connectivity test.");
	}
}
