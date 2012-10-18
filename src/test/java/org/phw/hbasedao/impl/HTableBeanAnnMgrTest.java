package org.phw.hbasedao.impl;

import java.lang.reflect.Method;

import org.junit.Test;
import org.phw.hbasedao.annotations.HBaseTable;
import org.phw.hbasedao.ex.HTableDefException;

import static org.junit.Assert.*;

public class HTableBeanAnnMgrTest {

    @Test
    public void testGetTableName() throws HTableDefException {
        @HBaseTable(name = "hjb", nameCreator = ContextNameCreator.class, autoCreate = true)
        final class c {}

        ContextNameCreator.setSuffix(null);
        String tableName = HTableBeanAnnMgr.getTableName("default", c.class.getAnnotation(HBaseTable.class), c.class);
        assertEquals("hjb", tableName);

        ContextNameCreator.setSuffix("001");
        tableName = HTableBeanAnnMgr.getTableName("default", c.class.getAnnotation(HBaseTable.class), c.class);
        assertEquals("hjb_001", tableName);
    }

    @Test
    public void testFindProperMethod() {
        Method method = HTableBeanAnnMgr.findProperMethod(ContextNameCreator.class);
        assertEquals("tableName", method.getName());
    }

    @Test
    public void testInvokeMethod() throws HTableDefException {
        Method method = HTableBeanAnnMgr.findProperMethod(ContextNameCreator.class);

        ContextNameCreator.setSuffix(null);
        String tableName = HTableBeanAnnMgr.invokeMethod(method, new ContextNameCreator(), "hjb");
        assertEquals("hjb", tableName);

        ContextNameCreator.setSuffix("001");
        tableName = HTableBeanAnnMgr.invokeMethod(method, new ContextNameCreator(), "hjb");
        assertEquals("hjb_001", tableName);
    }

}
