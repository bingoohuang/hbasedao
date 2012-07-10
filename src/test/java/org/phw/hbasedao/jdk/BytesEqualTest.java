package org.phw.hbasedao.jdk;

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.phw.hbasedao.util.Types;

public class BytesEqualTest {
    @Test
    public void test() {
        Set<byte[]> set = new HashSet<byte[]>();
        byte[] bytes = Types.toBytes(100);
        set.add(bytes);
        Assert.assertFalse(set.contains(Types.toBytes(100)));
    }
}
