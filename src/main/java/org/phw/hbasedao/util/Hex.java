package org.phw.hbasedao.util;


public class Hex {
    private static final char[] HEX_CHARS = "0123456789ABCDEF".toCharArray();

    /**
     * 十六进制编码。
     * @param str 字符串。
     * @return 十六进制编码的字符串。
     */
    public static String toHex(byte[] buf) {
        int l = buf.length;
        char[] chars = new char[l << 1];
        for (int i = 0, j = 0; i < l; ++i) {
            chars[j++] = HEX_CHARS[(buf[i] & 0xF0) >>> 4];
            chars[j++] = HEX_CHARS[buf[i] & 0x0F];
        }
        return new String(chars);
    }

    /**
     * 十六进制字符转化为十进制整数。
     * @param ch 字符
     * @param index 字符在字符串中的位置
     * @return 十进制整数
     */
    protected static int toDigit(char ch, int index) {
        int digit = Character.digit(ch, 16);
        if (digit == -1) {
            throw new RuntimeException("Illegal hexadecimal charcter " + ch + " at index " + index);
        }
        return digit;
    }

    /**
     * 十六进制解码。
     * @param str 解码前的字符串。
     * @return 解码后的字符串。
     */
    public static byte[] fromHex(String str) {
        char[] data = str.toCharArray();
        int len = data.length;

        if ((len & 0x01) != 0) {
            throw new RuntimeException("Odd number of characters.");
        }

        byte[] out = new byte[len >> 1];

        // two characters form the hex value.
        for (int i = 0, j = 0; j < len; i++) {
            int f = toDigit(data[j], j) << 4;
            j++;
            f = f | toDigit(data[j], j);
            j++;
            out[i] = (byte) (f & 0xFF);
        }

        return out;
    }
}
