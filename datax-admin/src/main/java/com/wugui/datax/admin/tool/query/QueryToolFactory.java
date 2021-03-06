package com.wugui.datax.admin.tool.query;

import com.alibaba.druid.util.JdbcConstants;
import com.alibaba.druid.util.JdbcUtils;
import com.wugui.datax.admin.entity.JobJdbcDatasource;
import com.wugui.datax.admin.util.RdbmsException;

import java.sql.SQLException;

/**
 * 工具类，获取单例实体
 *
 * @author zhouhongfa@gz-yibo.com
 * @ClassName QueryToolFactory
 * @Version 1.0
 * @since 2019/7/18 9:36
 */
public class QueryToolFactory {
    public static final BaseQueryTool getByDbType(JobJdbcDatasource jobJdbcDatasource) {
        //获取dbType
        String dbType = JdbcUtils.getDbType(jobJdbcDatasource.getJdbcUrl(), jobJdbcDatasource.getJdbcDriverClass());
        if (JdbcConstants.MYSQL.equals(dbType)) {
            return getMySQLQueryToolInstance(jobJdbcDatasource);
        } else if (JdbcConstants.ORACLE.equals(dbType)) {
            return getOracleQueryToolInstance(jobJdbcDatasource);
        } else if (JdbcConstants.POSTGRESQL.equals(dbType)) {
            return getPostgresqlQueryToolInstance(jobJdbcDatasource);
        } else if (JdbcConstants.SQL_SERVER.equals(dbType)) {
            return getSqlserverQueryToolInstance(jobJdbcDatasource);
        } else if (JdbcConstants.HIVE.equals(dbType)) {
            return getHdfsQueryToolInstance(jobJdbcDatasource);
        }
        throw new UnsupportedOperationException("找不到该类型: ".concat(dbType));
    }

    private static BaseQueryTool getMySQLQueryToolInstance(JobJdbcDatasource jdbcDatasource) {
        try {
            return new MySQLQueryTool(jdbcDatasource);
        } catch (Exception e) {
            throw RdbmsException.asConnException(JdbcConstants.MYSQL,
                    e,jdbcDatasource.getJdbcUsername(),jdbcDatasource.getDatasourceName());
        }
    }

    private static BaseQueryTool getOracleQueryToolInstance(JobJdbcDatasource jdbcDatasource) {
        try {
            return new OracleQueryTool(jdbcDatasource);
        } catch (SQLException e) {
            throw RdbmsException.asConnException(JdbcConstants.ORACLE,
                    e,jdbcDatasource.getJdbcUsername(),jdbcDatasource.getDatasourceName());
        }
    }

    private static BaseQueryTool getPostgresqlQueryToolInstance(JobJdbcDatasource jdbcDatasource) {
        try {
            return new PostgresqlQueryTool(jdbcDatasource);
        } catch (SQLException e) {
            throw RdbmsException.asConnException(JdbcConstants.POSTGRESQL,
                    e,jdbcDatasource.getJdbcUsername(),jdbcDatasource.getDatasourceName());
        }
    }

    private static BaseQueryTool getSqlserverQueryToolInstance(JobJdbcDatasource jdbcDatasource) {
        try {
            return new SqlServerQueryTool(jdbcDatasource);
        } catch (SQLException e) {
            throw RdbmsException.asConnException(JdbcConstants.SQL_SERVER,
                    e,jdbcDatasource.getJdbcUsername(),jdbcDatasource.getDatasourceName());
        }
    }

    private static BaseQueryTool getHdfsQueryToolInstance(JobJdbcDatasource jdbcDatasource) {
        try {
            return new HdfsQueryTool(jdbcDatasource);
        } catch (SQLException e) {
            throw RdbmsException.asConnException(JdbcConstants.HIVE,
                    e,jdbcDatasource.getJdbcUsername(),jdbcDatasource.getDatasourceName());
        }
    }
}
