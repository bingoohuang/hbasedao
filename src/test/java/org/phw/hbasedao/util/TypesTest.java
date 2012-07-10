package org.phw.hbasedao.util;

import java.util.Date;

import junit.framework.Assert;

import org.junit.Test;

public class TypesTest {

    @Test
    public void testBase() {
        Assert.assertNull(Types.toBytes(null));
        Assert.assertNull(Types.fromBytes(null, null));
        byte[] bs = {};
        Assert.assertEquals(bs, Types.toBytes(bs));
        Assert.assertEquals(bs, Types.fromBytes(bs, byte[].class));
        boolean b = true;
        Assert.assertEquals(b, (boolean) Types.fromBytes(Types.toBytes(b), Boolean.class));
        byte bt = 'a';
        Assert.assertEquals(bt, (byte) Types.fromBytes(Types.toBytes(bt), Byte.class));
        char ch = 'a';
        Assert.assertEquals(ch, (char) Types.fromBytes(Types.toBytes(ch), Character.class));
        short sh = 1;
        Assert.assertEquals(sh, (short) Types.fromBytes(Types.toBytes(sh), Short.class));
        int in = 1;
        Assert.assertEquals(in, (int) Types.fromBytes(Types.toBytes(in), Integer.class));
        long lg = 1;
        Assert.assertEquals(lg, (long) Types.fromBytes(Types.toBytes(lg), Long.class));
        float ft = 1;
        Assert.assertEquals(ft, (float) Types.fromBytes(Types.toBytes(ft), Float.class));
        double db = 1;
        Assert.assertEquals(db, (double) Types.fromBytes(Types.toBytes(db), Double.class));
        String str = "abc";
        Assert.assertEquals(str, Types.fromBytes(Types.toBytes(str), String.class));
        Date dt = new Date();
        Assert.assertEquals(dt, Types.fromBytes(Types.toBytes(dt), Date.class));
    }

    @Test
    public void testBean() {
        // 未实现BytesConvertable的Bean
        SampleBean sb1 = new SampleBean();
        sb1.setAge(10);
        sb1.setName("sam");
        SampleBean sb2 = Types.fromBytes(Types.toBytes(sb1), SampleBean.class);
        Assert.assertTrue(sb1.equals(sb2));

        // 实现了BytesConvertable的Bean
        SampleBean2 obj1 = new SampleBean2();
        obj1.setAge(10);
        obj1.setName("sam");
        SampleBean2 obj2 = Types.fromBytes(Types.toBytes(obj1), SampleBean2.class);
        Assert.assertTrue(obj1.equals(obj2));
    }
}
