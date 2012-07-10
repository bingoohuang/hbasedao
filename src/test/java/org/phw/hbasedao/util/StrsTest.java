package org.phw.hbasedao.util;

import junit.framework.Assert;

import org.junit.Test;

public class StrsTest {

    @Test
    public void testBase() {
        String s1 = null;
        String s2 = "";
        String s3 = "song";
        Assert.assertTrue(Strs.isEmpty(s1));
        Assert.assertTrue(Strs.isEmpty(s2));
        Assert.assertFalse(Strs.isEmpty(s3));

        Assert.assertFalse(Strs.isNotEmpty(s1));
        Assert.assertFalse(Strs.isNotEmpty(s2));
        Assert.assertTrue(Strs.isNotEmpty(s3));

        Assert.assertEquals(null, Strs.capitalize(s1));
        Assert.assertEquals("", Strs.capitalize(s2));
        Assert.assertEquals("Song", Strs.capitalize(s3));
        Assert.assertEquals("Song", Strs.capitalize("Song"));

        Assert.assertFalse(Strs.equals(s1, s2));
        Assert.assertFalse(Strs.equals(s2, s3));
        Assert.assertFalse(Strs.equals(s1, s3));
        Assert.assertTrue(Strs.equals(s1, null));
        Assert.assertTrue(Strs.equals(s2, ""));
        Assert.assertTrue(Strs.equals(s3, "song"));

        Assert.assertEquals("Song", Strs.defaultString(s1, "Song"));
        Assert.assertEquals("Song", Strs.defaultString(s2, "Song"));
        Assert.assertEquals("song", Strs.defaultString(s3, "Song"));
    }

}
