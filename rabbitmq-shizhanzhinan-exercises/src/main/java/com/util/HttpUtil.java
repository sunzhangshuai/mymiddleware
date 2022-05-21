package com.util;

import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import sun.misc.BASE64Encoder;

import java.io.IOException;

/**
 * @author sunchen
 */
public class HttpUtil {

    /**
     * get请求
     *
     * @param url      请求地址
     * @param userName 用户名
     * @param password 密码
     * @return 请求结果
     */
    public static String doGet(String url, String userName, String password) {
        HttpGet httpGet = new HttpGet(url);
        return doRequest(httpGet, userName, password);
    }

    /**
     * post请求
     *
     * @param url      请求地址
     * @param jsonStr  请求参数
     * @param userName 用户名
     * @param password 密码
     * @return 请求结果
     */
    public static String doPost(String url, String jsonStr, String userName, String password) {
        HttpPost httpPost = new HttpPost(url);
        return doEntityRequest(httpPost, jsonStr, userName, password);
    }

    /**
     * put请求
     *
     * @param url      请求地址
     * @param jsonStr  请求参数
     * @param userName 用户名
     * @param password 密码
     * @return 请求结果
     */
    public static String doPut(String url, String jsonStr, String userName, String password) {
        HttpPut httpPut = new HttpPut(url);
        return doEntityRequest(httpPut, jsonStr, userName, password);
    }

    /**
     * delete请求
     *
     * @param url      请求地址
     * @param userName 用户名
     * @param password 密码
     * @return 请求结果
     */
    public static String doDelete(String url, String userName, String password) {
        HttpDelete httpDelete = new HttpDelete(url);
        return doRequest(httpDelete, userName, password);
    }

    /**
     * 请求的最后处理
     *
     * @param httpClient   http客户端
     * @param httpResponse http响应
     */
    private static void finallyRequest(CloseableHttpClient httpClient, CloseableHttpResponse httpResponse) {
        if (httpResponse != null) {
            try {
                httpResponse.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (null != httpClient) {
            try {
                httpClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 带参数的请求
     *
     * @param method   请求方法
     * @param jsonStr  请求参数
     * @param userName 用户名
     * @param password 密码
     * @return 请求结果
     */
    private static <T extends HttpEntityEnclosingRequestBase> String doEntityRequest(T method, String jsonStr, String userName, String password) {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        setRequestHeaders(method, userName, password);
        CloseableHttpResponse httpResponse = null;
        try {
            method.setEntity(new StringEntity(jsonStr));
            httpResponse = httpClient.execute(method);
            HttpEntity entity = httpResponse.getEntity();
            return EntityUtils.toString(entity);
        } catch (IOException | IllegalArgumentException e) {
            e.printStackTrace();
        } finally {
            finallyRequest(httpClient, httpResponse);
        }
        return null;
    }

    /**
     * 不带参数的请求
     *
     * @param method   请求方法
     * @param userName 用户名
     * @param password 密码
     * @return 请求结果
     */
    private static <T extends HttpRequestBase> String doRequest(T method, String userName, String password) {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        setRequestHeaders(method, userName, password);
        CloseableHttpResponse httpResponse = null;
        try {
            httpResponse = httpClient.execute(method);
            HttpEntity entity = httpResponse.getEntity();
            return EntityUtils.toString(entity);
        } catch (IOException | IllegalArgumentException e) {
            e.printStackTrace();
        } finally {
            finallyRequest(httpClient, httpResponse);
        }
        return null;
    }

    /**
     * 设置请求头
     *
     * @param httpRequestBase 请求
     * @param userName        用户名
     * @param password        密码
     */
    private static <T extends HttpRequestBase> void setRequestHeaders(T httpRequestBase, String userName, String password) {
        RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(35000).setConnectionRequestTimeout(35000).setSocketTimeout(60000).build();
        httpRequestBase.setConfig(requestConfig);
        httpRequestBase.setHeader("Content-type", "application/json");
        httpRequestBase.setHeader("DataEncoding", "UTF-8");
        String auth = userName + ":" + password;
        BASE64Encoder enc = new BASE64Encoder();
        String encoding = enc.encode(auth.getBytes());
        httpRequestBase.setHeader("Authorization", "Basic " + encoding);
    }
}