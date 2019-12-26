package com.wugui.datax.admin.tool.datax.writer;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import com.wugui.datax.admin.entity.JobJdbcDatasource;
import com.wugui.datax.admin.tool.pojo.DataxPluginPojo;
import org.json.JSONObject;

import java.util.*;

/**
 * hdfs writer构建类
 *
 * @author zhouhongfa@gz-yibo.com
 * @ClassName MysqlWriter
 * @Version 1.0
 * @since 2019/7/30 23:08
 */
public class HdfsWriter extends BaseWriterPlugin implements DataxWriterInterface {
    @Override
    public String getName() {
        return "hdfswriter";
    }

    @Override
    public Map<String, Object> build(DataxPluginPojo dataxPluginPojo) {
        Map<String, Object> writerObj = Maps.newLinkedHashMap();
        writerObj.put("name", getName());

        Map<String, Object> parameterObj = Maps.newLinkedHashMap();

        JobJdbcDatasource jobJdbcDatasource = dataxPluginPojo.getJdbcDatasource();

        JSONObject extraComments = new JSONObject(jobJdbcDatasource.getComments());

        parameterObj.put("defaultFS", extraComments.get("defaultFS"));
        parameterObj.put("fileType", extraComments.get("fileType"));
        parameterObj.put("path", extraComments.get("basePath") + dataxPluginPojo.getTables().get(0));
        parameterObj.put("fileName", dataxPluginPojo.getTables().get(0));

        List<Object> jsonColumns = new ArrayList<Object>();
        for (String temp : dataxPluginPojo.getColumns()) {
            String name = temp.split("\\|")[0];
            String type = temp.split("\\|")[1];
            jsonColumns.add(JSON.parse("{\"name\": \"" + name + "\", \"type\": \"" + type + "\"}"));
        }
        parameterObj.put("column", jsonColumns);

        parameterObj.put("writeMode", extraComments.get("writeMode"));
        parameterObj.put("fieldDelimiter", extraComments.get("fieldDelimiter"));
        parameterObj.put("compress", extraComments.get("compress"));

        writerObj.put("parameter", parameterObj);
        return writerObj;
    }

    @Override
    public Map<String, Object> sample() {
        return null;
    }
}
