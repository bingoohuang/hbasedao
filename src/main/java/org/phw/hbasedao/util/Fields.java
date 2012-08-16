package org.phw.hbasedao.util;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.phw.hbasedao.ex.HTableDefException;

import com.esotericsoftware.reflectasm.FieldAccess;
import com.esotericsoftware.reflectasm.MethodAccess;

public class Fields {
    public static Field getDeclaredField(Class<?> clazz, String name) throws HTableDefException {
        try {
            return clazz.getDeclaredField(name);
        }
        catch (Exception e) {
            throw new HTableDefException(name + " cannot accessed", e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T getFieldValue(MethodAccess methodAccess, FieldAccess fieldAccess, Object target, Field field) {
        try {
            String prefix = field.getType() == boolean.class || field.getType() == Boolean.class
                    ? "is" : "get";
            String methodname = prefix + Strs.capitalize(field.getName());
            return (T) methodAccess.invoke(target, methodname);
        }
        catch (IllegalArgumentException e) {
            // Ignore
        }
        try {
            return (T) fieldAccess.get(target, field.getName());
        }
        catch (IllegalArgumentException e) {
            // Ignore
        }

        return null;
    }

    public static void setFieldValue2(Object target, Field field, Object value) {
        try {
            if (value == null && field.getType().isPrimitive()) {
                return; // 原生类型，value是null，则忽略。
            }

            String methodname = "set" + Strs.capitalize(field.getName());
            Method method = target.getClass().getDeclaredMethod(methodname, field.getType());
            method.setAccessible(true);
            method.invoke(target, value);
            return;
        }
        catch (NoSuchMethodException e) {
            // Ignore
        }
        catch (InvocationTargetException e) {
            // Ignore
        }
        catch (IllegalAccessException e) {
            // Ignore
        }

        try {
            field.setAccessible(true);
            field.set(target, value);
        }
        catch (IllegalAccessException e) {
            // Ignore
        }
        catch (IllegalArgumentException e) {
            // Ignore
        }

    }

    public static void setFieldValue(MethodAccess methodAccess, FieldAccess fieldAccess, Object target, Field field,
            Object value) {
        try {
            if (value == null && field.getType().isPrimitive()) {
                return; // 原生类型，value是null，则忽略。
            }

            String methodname = "set" + Strs.capitalize(field.getName());
            methodAccess.invoke(target, methodname, value);
            return;
        }
        catch (IllegalArgumentException e) {
            // Ignore
        }

        try {
            fieldAccess.set(target, field.getName(), value);
        }
        catch (IllegalArgumentException e) {
            // Ignore
        }
    }

}
