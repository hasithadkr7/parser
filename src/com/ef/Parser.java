package com.ef;

import java.io.*;
import java.sql.Connection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;


public class Parser {
    private static MySqlAccess  mySqlAccess = new MySqlAccess();

    public static void main(String[] args) {
        String[] inputArray;
        HashMap<String, String> inputMap = new HashMap<>();//Read command line inputs to hashmap.
        for (String arg : args) {
            inputArray = arg.split("=");
            if (inputArray[0].split("--")[1] != null) {
                inputMap.put(inputArray[0].split("--")[1], inputArray[1]);
            }
        }
        if (inputMap.size()>0) try {
            Connection connection = mySqlAccess.getConnection();
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd.HH:mm:ss");
            try {
                Date startDate = formatter.parse(inputMap.get("startDate"));
                LocalDateTime localDateTime = startDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
                if (inputMap.get("duration").equals("hourly")) {
                    localDateTime = localDateTime.plusMinutes(59).plusSeconds(59);
                } else {
                    localDateTime = localDateTime.plusHours(23).plusMinutes(59).plusSeconds(59);
                }
                Date endDate = Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
                readFile(inputMap.get("accesslog"), connection);
                mySqlAccess.getIps(startDate, endDate, Integer.parseInt(inputMap.get("threshold")), inputMap.get("duration"), connection);
                mySqlAccess.closeDbConnection(connection);
            } catch (ParseException e) {
                mySqlAccess.closeDbConnection(connection);
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        else {
            System.out.println("Input params are required.");
        }
    }

    // Read log file and insert ip,date,request columns to database.
    private static void readFile(String fileName,Connection connection){
        mySqlAccess.flushLogTable(connection);
        BufferedReader reader = null;
        int chunkSize = 100;//For batch insertions.
        List<String> list = new ArrayList<>();
        Date date;
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), "UTF-8"));
            for (String line; (line = reader.readLine()) != null;) {
                String[] parts = line.split("\\|");
                try {
                    date = formatter.parse(parts[0]);
                    String ip = parts[1];
                    String request = parts[2];
                    String insertStr = "(\""+ip+"\",'"+formatter.format(date)+"','"+request+"')";
                    list.add(insertStr);
                    if (list.size()>=chunkSize){
                        String joined = String.join(",", list);
                        mySqlAccess.insertToDb(joined,connection);
                        list.clear();
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
            if (list.size()>0){
                String joined = String.join(",", list);
                mySqlAccess.insertToDb(joined,connection);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(reader != null){
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
