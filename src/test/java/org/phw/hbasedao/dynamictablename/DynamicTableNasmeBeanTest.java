package org.phw.hbasedao.dynamictablename;

import org.junit.Test;
import org.phw.hbasedao.DefaultHDao;
import org.phw.hbasedao.HDao;
import org.phw.hbasedao.ex.HDaoException;
import org.phw.hbasedao.impl.ContextNameCreator;

public class DynamicTableNasmeBeanTest {
    @Test
    public void test1() throws HDaoException {
        DynamicTableNameBean bean = new DynamicTableNameBean();
        bean.setRowkey(1L);
        bean.setName("hjb");

        HDao dao = new DefaultHDao();

        ContextNameCreator.setSuffix("01");
        dao.insert(bean);

        ContextNameCreator.setSuffix("02");
        dao.insert(bean);
    }
}
