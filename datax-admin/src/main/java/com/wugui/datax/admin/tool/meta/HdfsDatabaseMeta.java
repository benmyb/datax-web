package com.wugui.datax.admin.tool.meta;

/**
 * TODO
 *
 * @author benzalus@163.com
 * @ClassName HdfsDatabaseMeta
 * @Version 1.0
 * @since 2019/12/25 11:05
 */
public class HdfsDatabaseMeta extends BaseDatabaseMeta implements DatabaseInterface {
    private volatile static HdfsDatabaseMeta single;

    public static HdfsDatabaseMeta getInstance() {
        if (single == null) {
            synchronized (HdfsDatabaseMeta.class) {
                if (single == null) {
                    single = new HdfsDatabaseMeta();
                }
            }
        }
        return single;
    }

    @Override
    public String getSQLQueryTables(String... args) {
        return "show tables";
    }

    @Override
    public String getSQLQueryColumns(String... args) {
        return "DESCRIBE " + args[0];
    }
}
