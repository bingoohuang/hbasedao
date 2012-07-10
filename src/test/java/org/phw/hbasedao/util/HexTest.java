package org.phw.hbasedao.util;

import junit.framework.Assert;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class HexTest {

    @Test
    public void testBase() {
        Assert.assertEquals("something", Bytes.toString(Hex.fromHex(Hex.toHex("something".getBytes()))));
    }
}
