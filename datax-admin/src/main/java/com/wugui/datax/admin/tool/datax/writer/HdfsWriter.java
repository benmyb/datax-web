package com.wugui.datax.admin.tool.datax.writer;

import java.util.Map;

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
    public Map<String, Object> sample() {
        return null;
    }
}
