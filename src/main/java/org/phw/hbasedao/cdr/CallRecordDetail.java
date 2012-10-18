package org.phw.hbasedao.cdr;

import org.phw.hbasedao.annotations.HBaseTable;
import org.phw.hbasedao.annotations.HColumn;
import org.phw.hbasedao.annotations.HRowkeyPart;
import org.phw.hbasedao.impl.ContextNameCreator;

@HBaseTable(name = "cdr", nameCreator = ContextNameCreator.class, autoCreate = true)
public class CallRecordDetail {
    @HRowkeyPart
    private long timestamp; // 主叫号码
    @HRowkeyPart
    private long desc; // 通话起始时间
    @HColumn(key = "a")
    private int seconds; //  通话时长
    @HColumn(key = "b")
    private boolean calling; // 呼叫类型  主叫/被叫
    @HColumn(key = "c")
    private long calleeNumber; // 对方号码
    @HColumn(key = "d")
    private String location; // 通话地点
    @HColumn(key = "e")
    private String callType; // 通话类型（国内通话）
    @HColumn(key = "f")
    private int callFee; // 通话费
    @HColumn(key = "g")
    private int otherFee; // 其他费
    @HColumn(key = "h")
    private int subtotal; // 小计

    public int getSeconds() {
        return seconds;
    }

    public void setSeconds(int seconds) {
        this.seconds = seconds;
    }

    public boolean isCalling() {
        return calling;
    }

    public void setCalling(boolean calling) {
        this.calling = calling;
    }

    public long getCalleeNumber() {
        return calleeNumber;
    }

    public void setCalleeNumber(long calleeNumber) {
        this.calleeNumber = calleeNumber;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getCallType() {
        return callType;
    }

    public void setCallType(String callType) {
        this.callType = callType;
    }

    public int getCallFee() {
        return callFee;
    }

    public void setCallFee(int callFee) {
        this.callFee = callFee;
    }

    public int getOtherFee() {
        return otherFee;
    }

    public void setOtherFee(int otherFee) {
        this.otherFee = otherFee;
    }

    public int getSubtotal() {
        return subtotal;
    }

    public void setSubtotal(int subtotal) {
        this.subtotal = subtotal;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getDesc() {
        return desc;
    }

    public void setDesc(long desc) {
        this.desc = desc;
    }

}
