package com.wugui.datax.admin.service.impl;

import cn.hutool.core.util.StrUtil;
import com.alibaba.druid.util.JdbcConstants;
import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.wugui.datax.admin.dto.DataxJsonDto;
import com.wugui.datax.admin.entity.JobJdbcDatasource;
import com.wugui.datax.admin.service.DataxJsonService;
import com.wugui.datax.admin.service.IJobJdbcDatasourceService;
import com.wugui.datax.admin.tool.datax.DataxJsonHelper;
import com.wugui.datax.admin.tool.datax.writer.StreamWriter;
import com.wugui.datax.admin.tool.query.BaseQueryTool;
import com.wugui.datax.admin.tool.query.QueryToolFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * com.wugui.datax json构建实现类
 *
 * @author zhouhongfa@gz-yibo.com
 * @ClassName DataxJsonServiceImpl
 * @Version 1.0
 * @since 2019/8/1 16:40
 */
@Service
public class DataxJsonServiceImpl implements DataxJsonService {

    @Autowired
    private IJobJdbcDatasourceService jobJdbcDatasourceService;

    @Override
    public String buildJobJson(DataxJsonDto dataxJsonDto) {

        // com.wugui.datax json helper
        DataxJsonHelper dataxJsonHelper = new DataxJsonHelper();

        // reader
        JobJdbcDatasource readerDatasource = jobJdbcDatasourceService.getById(dataxJsonDto.getReaderDatasourceId());

        //querySql
        dataxJsonHelper.setQuerySql(dataxJsonDto.getQuerySql());
        //where
        if (StrUtil.isNotBlank(dataxJsonDto.getWhereParams())) {
            dataxJsonHelper.addWhereParams(dataxJsonDto.getWhereParams());
        }

        // reader plugin init
        String readerDbType = JdbcUtils.getDbType(readerDatasource.getJdbcUrl(), readerDatasource.getJdbcDriverClass());
        if (JdbcConstants.HIVE.equals(readerDbType)) {
            List<String> rawColumns = dataxJsonDto.getReaderColumns();
            List<String> needColumns = Lists.newArrayList();
            BaseQueryTool hiveQueryTool = QueryToolFactory.getByDbType(readerDatasource);
            Map<String, String> tableColumnTypeAndComment = hiveQueryTool.getColumnTypeAndComment(dataxJsonDto.getReaderTables().get(0));
            for (String temp : rawColumns) {
                String colName = temp.split("\\.")[1];
                needColumns.add(colName + "|" + tableColumnTypeAndComment.get(colName).toUpperCase());
            }
            dataxJsonHelper.initReader(readerDatasource, dataxJsonDto.getReaderTables(), needColumns);
        } else {
            dataxJsonHelper.initReader(readerDatasource, dataxJsonDto.getReaderTables(), dataxJsonDto.getReaderColumns());
        }
        //
        //如果是streamwriter
        if (dataxJsonDto.getIfStreamWriter()) {
            dataxJsonHelper.setWriterPlugin(new StreamWriter());
        } else {
            JobJdbcDatasource writerDatasource = jobJdbcDatasourceService.getById(dataxJsonDto.getWriterDatasourceId());
            String writerDbType = JdbcUtils.getDbType(writerDatasource.getJdbcUrl(), writerDatasource.getJdbcDriverClass());
            if (JdbcConstants.HIVE.equals(writerDbType)) {
                List<String> rawColumns = dataxJsonDto.getWriterColumns();
                List<String> needColumns = Lists.newArrayList();
                BaseQueryTool hiveQueryTool = QueryToolFactory.getByDbType(writerDatasource);
                Map<String, String> tableColumnTypeAndComment = hiveQueryTool.getColumnTypeAndComment(dataxJsonDto.getWriterTables().get(0));
                for (String temp : rawColumns) {
                    String colName = temp.split("\\.")[1];
                    needColumns.add(colName + "|" + tableColumnTypeAndComment.get(colName).toUpperCase());
                }
                dataxJsonHelper.initWriter(writerDatasource, dataxJsonDto.getWriterTables(), needColumns);
            } else {
                dataxJsonHelper.initWriter(writerDatasource, dataxJsonDto.getWriterTables(), dataxJsonDto.getWriterColumns());
            }
            dataxJsonHelper.setPreSql(dataxJsonDto.getPreSql());
        }

        return JSON.toJSONString(dataxJsonHelper.buildJob());
    }
}
