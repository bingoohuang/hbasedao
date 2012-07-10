package org.phw.hbasedao.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 定义HBase表。
 * @author BingooHuang
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(value = { ElementType.TYPE })
public @interface HBaseTable {
    /**
     * HTable表名。
     * @return
     */
    String name();

    /**
     * 是否在表不存在的时候自动创建表。
     * @return
     */
    boolean autoCreate() default false;

    /**
     * 需要自动创建表时，列族的定义。
     * @return
     */
    String[] families() default { "f" };
}
