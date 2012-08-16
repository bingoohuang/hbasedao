package org.phw.hbasedao.util;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.phw.hbasedao.annotations.HConnectionConfig;
import org.reflections.Reflections;

public class ConfigUtils {
    private static Map<String, Method> configMap = new HashMap<String, Method>();
    static {
        Reflections reflections = new Reflections("org.phw.hbasedao.config");
        Set<Class<?>> classes = reflections.getTypesAnnotatedWith(HConnectionConfig.class);

        for (Class<?> class1 : classes) {
            for (Method method : class1.getMethods()) {
                Class<?>[] parameterTypes = method.getParameterTypes();
                if (parameterTypes.length != 0) continue;
                if (!Map.class.isAssignableFrom(method.getReturnType())) continue;
                if (!method.isAnnotationPresent(HConnectionConfig.class)) continue;
                HConnectionConfig configAnn = method.getAnnotation(HConnectionConfig.class);

                configMap.put(configAnn.value(), method);
            }
        }
    }

    public static Map<String, String> getConfig(String instanceName) {
        Method method = configMap.get(instanceName);
        if (method == null) return null;

        return invokeMethod(method);
    }

    @SuppressWarnings("unchecked")
    public static <T> T invokeMethod(Method method) {
        try {
            Object instance = null;
            if (!Modifier.isStatic(method.getModifiers())) {
                instance = Clazz.newInstance(method.getDeclaringClass());
            }

            return (T) method.invoke(instance, new Object[] {});
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
