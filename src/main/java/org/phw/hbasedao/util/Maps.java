package org.phw.hbasedao.util;

import java.util.HashMap;
import java.util.Map;

public class Maps {

    public static <T> Map<T, T> valueOf(T... objects) {
        HashMap<T, T> hashMap = new HashMap<T, T>();
        for (int i = 0; i < objects.length; i += 2) {
            if (i + 1 < objects.length) {
                hashMap.put(objects[i], objects[i + 1]);
            }
        }

        return hashMap;
    }

}
