package com.yovole.s3.admin;


import java.io.*;
import java.util.*;
import java.net.*;
import java.text.*;
//import java.util.Map.Entry;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.json.simple.JSONArray;

import com.yovole.s3.common.CephAdminAPI;
public class Usage {

    CephAdminAPI adminApi;

    public Usage(CephAdminAPI api){
        this.adminApi = api;
    }

    //GET /{admin}/usage?format=json
    public int getAllUsage(){
        System.out.println("GET /{admin}/usage ");
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("format", "json");
        response = adminApi.execute("GET", "/admin/usage", null, requestArgs, null);
        int status = response.getStatusLine().getStatusCode();
        System.out.println(status);
        entity = response.getEntity();
        try {
            System.out.println("\nResponse Content is: "
                        + EntityUtils.toString(entity, "UTF-8") + "\n");
            response.close();
            return status;
        } catch (IOException e){
            System.err.println ("Encountered an I/O exception.");
            e.printStackTrace();
            return -1;
        }
    }

    public int getUsageWithUid(String uid){
        System.out.println("get usage with uid Input: "+uid);
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("format", "json");
        requestArgs.put("uid", uid);
        response = adminApi.execute("GET", "/admin/usage", null, requestArgs, null);
        int status = response.getStatusLine().getStatusCode();
        System.out.println(status);
        entity = response.getEntity();
        try {
            System.out.println("\nResponse Content is: "
                        + EntityUtils.toString(entity, "UTF-8") + "\n");
            response.close();
            return status;
        } catch (IOException e){
            System.err.println ("Encountered an I/O exception.");
            e.printStackTrace();
            return -1;
        }

    }

    public int getUsageWithDate(String startDate, String endDate){
        System.out.println("get usage with date Input: "+startDate+" "+endDate);
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("format", "json");
        //SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss");
        //Date end_date = new Date();
        requestArgs.put("start", startDate);
        requestArgs.put("end", endDate);
        response = adminApi.execute("GET", "/admin/usage", null, requestArgs, null);
        int status = response.getStatusLine().getStatusCode();
        System.out.println(status);
        entity = response.getEntity();
        try {
            System.out.println("\nResponse Content is: "
                        + EntityUtils.toString(entity, "UTF-8") + "\n");
            response.close();
            return status;
        } catch (IOException e){
            System.err.println ("Encountered an I/O exception.");
            e.printStackTrace();
            return -1;
        }
    }


    // DELETE /{admin}/usage?format=json
    public int removeUsageWithDate(String startDate, String endDate){
        System.out.println("DELETE /{admin}/usage Input: "+startDate+" "+endDate);
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("format", "json");
        requestArgs.put("start", startDate);
        //SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss");
        //Date end_date = new Date();
        requestArgs.put("end",   endDate);
        response = adminApi.execute("DELETE", "/admin/usage", null, requestArgs, null);
        int status = response.getStatusLine().getStatusCode();
        System.out.println(status);
        entity = response.getEntity();
        try {
            System.out.println("\nResponse Content is: "
                        + EntityUtils.toString(entity, "UTF-8") + "\n");
            response.close();
            return status;
        } catch (IOException e){
            System.err.println ("Encountered an I/O exception.");
            e.printStackTrace();
            return -1;
        }
    }

    public int removeUsageWithUid(String uid){
        System.out.println("DELETE /{admin}/usage Input: "+uid);
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("format", "json");
        requestArgs.put("uid", uid);
        response = adminApi.execute("DELETE", "/admin/usage", null, requestArgs, null);
        int status = response.getStatusLine().getStatusCode();
        System.out.println(status);
        entity = response.getEntity();
        try {
            System.out.println("\nResponse Content is: "
                        + EntityUtils.toString(entity, "UTF-8") + "\n");
            response.close();
            return status;
        } catch (IOException e){
            System.err.println ("Encountered an I/O exception.");
            e.printStackTrace();
            return -1;
        }

    }

    public int removeAllUsage(){
        System.out.println("DELETE /{admin}/usage");
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("format", "json");
        requestArgs.put("remove-all", true);
        response = adminApi.execute("DELETE", "/admin/usage", null, requestArgs, null);
        int status = response.getStatusLine().getStatusCode();
        System.out.println(status);
        entity = response.getEntity();
        try {
            System.out.println("\nResponse Content is: "
                        + EntityUtils.toString(entity, "UTF-8") + "\n");
            response.close();
            return status;
        } catch (IOException e){
            System.err.println ("Encountered an I/O exception.");
            e.printStackTrace();
            return -1;
        }
   }
}
