package org.phw.hbasedao.complex1;

import junit.framework.Assert;

import org.junit.Test;
import org.phw.hbasedao.DefaultHDao;
import org.phw.hbasedao.HDao;
import org.phw.hbasedao.ex.HDaoException;

public class Complex1Test {
    @Test
    public void test() throws HDaoException {
        Complex1 complex1 = new Complex1();
        complex1.setRowkey("60476");
        complex1.setAdult(true);
        complex1.setAge(33);
        complex1.setName("BingooHuang");
        MyDay myday = new MyDay();
        myday.setYear(2011);
        myday.setMonth(11);
        myday.setDay(11);
        complex1.setMyday(myday);
        OtherDay otherday = new OtherDay();
        otherday.setYear(2011);
        otherday.setMonth(11);
        otherday.setDay(11);
        complex1.setOtherday(otherday);

        HDao dao = new DefaultHDao();
        dao.put(complex1);

        Complex1 complex12 = dao.get(Complex1.class, "60476");
        Assert.assertEquals(complex1, complex12);

        dao.delete(Complex1.class, "60476");

    }
}
