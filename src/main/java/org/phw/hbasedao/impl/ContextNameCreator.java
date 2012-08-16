package org.phw.hbasedao.impl;

/**
 * 基于现场上下文的表名创建器。
 * 需要在使用之前调用setSuffix()方法。
 * @author Bingoo
 *
 */
public class ContextNameCreator {
    private static ThreadLocal<String> suffix = new ThreadLocal<String>();

    public static void setSuffix(String suffixStr) {
        suffix.set(suffixStr);
    }

    public String tableName(String name) {
        if (suffix.get() == null) return name;

        return name + '_' + suffix.get();
    }
}
