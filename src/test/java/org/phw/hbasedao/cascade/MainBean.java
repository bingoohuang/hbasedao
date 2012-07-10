package org.phw.hbasedao.cascade;

import java.util.List;

import org.phw.hbasedao.annotations.HBaseTable;
import org.phw.hbasedao.annotations.HCascade;
import org.phw.hbasedao.annotations.HColumn;
import org.phw.hbasedao.annotations.HRowkey;

@HBaseTable(name = "maintable", autoCreate = true, families = { "f" })
public class MainBean {
    @HRowkey
    private String id;
    @HColumn
    private String name;

    @HCascade
    private List<SubItemBean> subItems;
    @HCascade
    private List<SubItem2Bean> subItems2;
    @HCascade
    private OtherBean otherBean;

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (id == null ? 0 : id.hashCode());
        result = prime * result + (name == null ? 0 : name.hashCode());
        result = prime * result + (otherBean == null ? 0 : otherBean.hashCode());
        result = prime * result + (subItems == null ? 0 : subItems.hashCode());
        result = prime * result + (subItems2 == null ? 0 : subItems2.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        MainBean other = (MainBean) obj;
        if (id == null) {
            if (other.id != null) {
                return false;
            }
        }
        else if (!id.equals(other.id)) {
            return false;
        }
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        }
        else if (!name.equals(other.name)) {
            return false;
        }
        if (otherBean == null) {
            if (other.otherBean != null) {
                return false;
            }
        }
        else if (!otherBean.equals(other.otherBean)) {
            return false;
        }
        if (subItems == null) {
            if (other.subItems != null) {
                return false;
            }
        }
        else if (!subItems.equals(other.subItems)) {
            return false;
        }
        if (subItems2 == null) {
            if (other.subItems2 != null) {
                return false;
            }
        }
        else if (!subItems2.equals(other.subItems2)) {
            return false;
        }
        return true;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<SubItemBean> getSubItems() {
        return subItems;
    }

    public void setSubItems(List<SubItemBean> subItems) {
        this.subItems = subItems;
    }

    public List<SubItem2Bean> getSubItems2() {
        return subItems2;
    }

    public void setSubItems2(List<SubItem2Bean> subItems2) {
        this.subItems2 = subItems2;
    }

    public OtherBean getOtherBean() {
        return otherBean;
    }

    public void setOtherBean(OtherBean otherBean) {
        this.otherBean = otherBean;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
