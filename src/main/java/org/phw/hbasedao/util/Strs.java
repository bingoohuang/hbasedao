package org.phw.hbasedao.util;

public class Strs {
    public static boolean isEmpty(String s) {
        return s == null || s.length() == 0;
    }

    public static boolean isNotEmpty(String s) {
        return s != null && s.length() > 0;
    }

    public static String capitalize(CharSequence s) {
        if (null == s) {
            return null;
        }
        int len = s.length();
        if (len == 0) {
            return "";
        }
        char char0 = s.charAt(0);
        if (Character.isUpperCase(char0)) {
            return s.toString();
        }
        return new StringBuilder(len).append(Character.toUpperCase(char0))
                .append(s.subSequence(1, len)).toString();
    }

    public static boolean equals(String s1, String s2) {
        return s1 != null ? s1.equals(s2) : s2 == null;
    }

    public static String defaultString(String str, String defStr) {
        return isNotEmpty(str) ? str : defStr;
    }
}
