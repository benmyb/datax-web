package com.wugui.datax.admin.tool.datax.reader;

import java.util.Map;

/**
 * hive reader 构建类
 *
 * @author zhouhongfa@gz-yibo.com
 * @ClassName MysqlReader
 * @Version 1.0
 * @since 2019/7/30 23:07
 */
public class HiveReader extends BaseReaderPlugin implements DataxReaderInterface {
    @Override
    public String getName() {
        return "hivereader";
    }


    @Override
    public Map<String, Object> sample() {
        return null;
    }
}
