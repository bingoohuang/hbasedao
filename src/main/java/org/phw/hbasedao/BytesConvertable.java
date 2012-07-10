package org.phw.hbasedao;

public interface BytesConvertable<T> {
    /**
     * 从对象转换为字节数组
     * @param object 被转换的对象
     * @return 字节数组
     */
    byte[] toBytes(T object);

    /**
     * 从字节数组到对象。
     * @param bytes 字节数组
     * @return 转换后的对象，返回null表示转换失败
     */
    T fromBytes(byte[] bytes);
}
