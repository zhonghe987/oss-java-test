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

public class S3Objects {
    CephAdminAPI adminApi;

    public S3Objects(CephAdminAPI api){
        this.adminApi = api;
    }

    //DELETE /{admin}/bucket?object&format=json
    public int removeObject(String bucket, String object){
        System.out.println("DELETE /{admin}/bucket?object Input: "+bucket+" "+object);
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("format", "json");
        requestArgs.put("bucket", bucket);
        requestArgs.put("object", object);
        response = adminApi.execute("DELETE", "/admin/bucket", "object", requestArgs, null);
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
