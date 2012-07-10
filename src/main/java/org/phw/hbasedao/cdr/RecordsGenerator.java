package org.phw.hbasedao.cdr;

import java.util.Random;

public class RecordsGenerator {
    private Random random = new Random();

    // 业务类型 通话起始时间                   通话时长  呼叫类型     对方号码             通话地点 通话类型 通话费   其他费   小计
    // 语音通话 2011-11-01 07:05:40 25秒        主叫/被叫   134 6631 4298 北京        国内通话 0.00   0.00   0.00
    public CallRecordDetail randomRecord(CallRecordDetail lastRecord) {
        CallRecordDetail callRecordDetail = new CallRecordDetail();

        callRecordDetail.setTimestamp(lastRecord == null ? System.currentTimeMillis() : lastRecord.getTimestamp());
        callRecordDetail.setDesc(lastRecord == null ? Long.MAX_VALUE : lastRecord.getDesc() - 1);

        callRecordDetail.setSeconds(random.nextInt(1000));
        callRecordDetail.setCalling(random.nextBoolean());
        callRecordDetail.setCalleeNumber(13466314298L + random.nextInt(10000));
        callRecordDetail.setLocation("北京");
        callRecordDetail.setCallType("国内通话");
        callRecordDetail.setCallFee(random.nextInt(10000));
        callRecordDetail.setOtherFee(0);
        callRecordDetail.setSubtotal(callRecordDetail.getCallFee() + callRecordDetail.getOtherFee());

        return callRecordDetail;
    }
}
