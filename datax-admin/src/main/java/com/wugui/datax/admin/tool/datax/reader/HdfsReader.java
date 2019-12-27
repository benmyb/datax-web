package com.wugui.datax.admin.tool.datax.reader;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import com.wugui.datax.admin.entity.JobJdbcDatasource;
import com.wugui.datax.admin.tool.pojo.DataxPluginPojo;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * hdfs reader 构建类
 *
 * @author zhouhongfa@gz-yibo.com
 * @ClassName MysqlReader
 * @Version 1.0
 * @since 2019/7/30 23:07
 */
public class HdfsReader extends BaseReaderPlugin implements DataxReaderInterface {
    @Override
    public String getName() {
        return "hdfsreader";
    }

    public Map<String, Object> build(DataxPluginPojo dataxPluginPojo) {
        //构建
        Map<String, Object> readerObj = Maps.newLinkedHashMap();

        readerObj.put("name", getName());
//
        Map<String, Object> parameterObj = Maps.newLinkedHashMap();

        JobJdbcDatasource jobJdbcDatasource = dataxPluginPojo.getJdbcDatasource();

        JSONObject extraComments = new JSONObject(jobJdbcDatasource.getComments());

        parameterObj.put("defaultFS", extraComments.get("defaultFS"));
        parameterObj.put("fileType", extraComments.get("fileType"));
        parameterObj.put("path", extraComments.get("basePath") + dataxPluginPojo.getTables().get(0));
        parameterObj.put("encoding", "UTF-8");

        List<Object> jsonColumns = new ArrayList<Object>();
        List<String> rawColumns = dataxPluginPojo.getColumns();
        for (int i = 0; i < rawColumns.size(); ++i) {
            String type = rawColumns.get(i).split("\\|")[1];
            jsonColumns.add(JSON.parse("{\"index\": \"" + String.valueOf(i) + "\", \"type\": \"" + type + "\"}"));
        }
        parameterObj.put("column", jsonColumns);

        parameterObj.put("fieldDelimiter", extraComments.get("fieldDelimiter"));

        readerObj.put("parameter", parameterObj);

        return readerObj;
    }


    @Override
    public Map<String, Object> sample() {
        return null;
    }
}
