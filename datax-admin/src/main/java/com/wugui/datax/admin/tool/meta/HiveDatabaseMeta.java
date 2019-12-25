package com.wugui.datax.admin.tool.meta;

/**
 * TODO
 *
 * @author benzalus@163.com
 * @ClassName HiveDatabaseMeta
 * @Version 1.0
 * @since 2019/12/25 11:05
 */
public class HiveDatabaseMeta extends BaseDatabaseMeta implements DatabaseInterface {
    private volatile static HiveDatabaseMeta single;

    public static HiveDatabaseMeta getInstance() {
        if (single == null) {
            synchronized (HiveDatabaseMeta.class) {
                if (single == null) {
                    single = new HiveDatabaseMeta();
                }
            }
        }
        return single;
    }

    @Override
    public String getSQLQueryTables(String... args) {
        return "show tables";
    }
}
