package com.iglomo;
import java.sql.*;
import java.util.*;
import java.io.*;

public class DBConnection {
		public String msg;
    private static DBConnection instance = null;
    private DBConnection(){
    		msg="";
        try{
            Class.forName("org.postgresql.Driver");
        }catch (Exception e){
            msg="ERROR: failed to load postgresql JDBC driver:"+e.getMessage();
						System.err.println(msg);
						e.printStackTrace();
        }
    }
    synchronized static public DBConnection getInstance() {
        if (instance == null) {
            instance = new DBConnection();
        }
        return instance;
    }
    Connection getConnection(){
      Connection con = null;
      try{
      	DriverManager.setLoginTimeout(10);
        con= DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/smppdb?charSet=UTF-8","smpper","SmIpp3r");
      }catch (Exception e){
          msg="ERROR DB: failed to connect!"+e.getMessage();
					System.err.println(msg);
					e.printStackTrace();
      }      
      return con;
    }
}