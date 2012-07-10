package org.phw.hbasedao.util;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

public class MapsTest {

    @Test
    public void testBase() {
        Map<String, String> m = new HashMap<String, String>();
        m.put("aaa", "AAA");
        m.put("bbb", "BBB");
        Assert.assertTrue(Maps.valueOf("aaa", "AAA", "bbb", "BBB").equals(m));
    }

}
