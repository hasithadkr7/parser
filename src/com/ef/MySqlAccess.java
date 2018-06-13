package com.ef;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class MySqlAccess {
    private Connection con;
    private Properties properties;
    private ConfigProperties configProperties;

    MySqlAccess() {
        configProperties = new ConfigProperties();
        this.properties = configProperties.getMySqlDbConfig();
        con = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            this.con = DriverManager.getConnection("jdbc:mysql://"
                            +properties.getProperty("dbhost")+":"
                            +properties.getProperty("port")+"/"
                            +properties.getProperty("database")+"?verifyServerCertificate=false&useSSL=true",//To avoid ssl warning.
                    properties.getProperty("dbuser"),
                    properties.getProperty("dbpassword"));
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    Connection getConnection(){
        try{
            if(con == null){
                if (configProperties == null){
                    properties = new Properties();
                    configProperties = new ConfigProperties();
                    properties = configProperties.getMySqlDbConfig();
                }
                Class.forName("com.mysql.cj.jdbc.Driver");
                con = DriverManager.getConnection("jdbc:mysql://"
                                +properties.getProperty("dbhost")+":"
                                +properties.getProperty("port")+"/"
                                +properties.getProperty("database"),
                        properties.getProperty("dbuser"),
                        properties.getProperty("dbpassword"));
                return con;
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return con;
    }

    void closeDbConnection(Connection connection){
        if(connection!=null){
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    void insertToDb(String valuesStr, Connection connection){
        String sql = "INSERT INTO `tbl_access_log` (`ip`,`date_time`,`request`) VALUES "+valuesStr+";";
        try {
            if (connection !=null){
                Statement statement = connection.createStatement();
                statement.executeUpdate(sql);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    void flushLogTable(Connection connection){
        String sql = "truncate tbl_access_log";

        try {
            if (connection !=null){
                Statement statement = connection.createStatement();
                statement.executeUpdate(sql);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*public void addToDb(Boolean hourly,String valuesStr){
        String sql;
        if (hourly){
            sql = "INSERT INTO `tbl_hourly` (`ip`,`count`) VALUES "+valuesStr+" ON DUPLICATE KEY UPDATE `count`= `count`+ VALUES(`count`);";
        }else {
            sql = "INSERT INTO `tbl_daily` (`ip`,`count`) VALUES "+valuesStr+" ON DUPLICATE KEY UPDATE `count`= `count`+ VALUES(`count`);";
        }
        try {
            Connection connection = getConnection();
            if (connection !=null){
                Statement statement = connection.createStatement();
                statement.executeUpdate(sql);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void flushTable(Boolean hourly){
        String sql;
        if (hourly){
            sql = "truncate tbl_hourly";
        }else {
            sql = "truncate tbl_daily";
        }
        try {
            Connection connection = getConnection();
            if (connection !=null){
                Statement statement = connection.createStatement();
                statement.executeUpdate(sql);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }*/


    void getIps(java.util.Date startDate, java.util.Date endDate, int threshold, String duration, Connection connection) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String startDateStr = formatter.format(startDate);
        String endDateStr = formatter.format(endDate);
        String sql = "select tbl_last.ip_address as req_ips,tbl_last.request_count as req_count from " +
                "(select tbl.ip as ip_address,count(tbl.ip) as request_count from " +
                "(SELECT ip,date_time FROM log_db.tbl_access_log where " +
                "date_time>'"+startDateStr+"' and date_time <= '"+endDateStr+"') " +
                "as tbl group by tbl.ip) as tbl_last where tbl_last.request_count > "+threshold+";";
        try {
            if (connection !=null){
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql);
                insertToBlocked(startDateStr,duration,threshold,resultSet,connection);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void insertToBlocked(String startDate,String duration,int threshold,ResultSet resultSet, Connection connection) {
        List<String> list = new ArrayList<>();
        try{
            while (resultSet.next()) {
                String ip = resultSet.getString("req_ips");
                System.out.println(ip);
                String comment = "current request count is "+resultSet.getInt("req_count");
                String valueStr = "('"+ip+"','"+startDate+"','"+duration+"',"+threshold+",'"+comment+"')";
                list.add(valueStr);
            }
            if (list.size()>0){
                String joined = String.join(",", list);
                String sql = "INSERT INTO `tbl_blocked` (`ip`,`start_date`,`duration`,`threshold`,`comment`) VALUES "+joined+";";
                try {
                    if (connection !=null){
                        Statement statement = connection.createStatement();
                        statement.executeUpdate(sql);
                    }
                } catch (SQLIntegrityConstraintViolationException e) {
                    System.out.println("Same data already exists.");
                }
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
    }
}


