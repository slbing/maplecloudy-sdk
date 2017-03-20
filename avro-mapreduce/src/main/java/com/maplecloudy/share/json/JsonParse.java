package com.maplecloudy.share.json;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

import org.apache.avro.AvroTypeException;

import com.google.common.collect.Maps;
import com.jayway.jsonpath.Filter;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.JsonProviderFactory;
import com.jayway.restassured.internal.path.ObjectConverter;

/**
 * 
 * @author lbsun 一个可以根据在模型上进行@JPath标记，可以从一个json解析出模型的实现 这个方法支持所有基本类型
 *         并且支持List和Map，但是在List和Map里面，value只能是基本类型，Map的key只能是String，这样是为了兼容avro
 *         同时Josn里面也有一些限制，所有不是有avro可以使用模型定义都能使用这个。
 */
@SuppressWarnings(value = {"rawtypes"})
public class JsonParse {
  
  public static <T> T parse(Class<T> typ, String str)
      throws IllegalArgumentException, IllegalAccessException,
      InstantiationException {
    Object jsonObject = JsonProviderFactory.createProvider().parse(str);
    return jsonObjectParse(typ, jsonObject);
  }
  
  public static <T> T jsonObjectParse(Class<T> typ, Object jsonObject)
      throws IllegalArgumentException, IllegalAccessException,
      InstantiationException {
    if (typ == null) return null;
    T t = typ.newInstance();
    Map<String,Field> fields = getFields(t.getClass());
    for (Entry<String,Field> entry : fields.entrySet()) {
      Type type = entry.getValue().getGenericType();
      if (type instanceof ParameterizedType) {
        ParameterizedType ptype = (ParameterizedType) type;
        Class raw = (Class) ptype.getRawType();
        java.lang.reflect.Type[] params = ptype.getActualTypeArguments();
        if (Collection.class.isAssignableFrom(raw)) { // array
          if (params.length != 1) {
            setListValue(t, jsonObject, entry.getValue(), null);
            
          } else {
            setListValue(t, jsonObject, entry.getValue(), (Class) params[0]);
            // Object value = jp.getList(path, (Class) params[0]);
            // entry.getValue().set(t, value);
          }
        } else if (Map.class.isAssignableFrom(raw)) { // map
          java.lang.reflect.Type key = params[0];
          java.lang.reflect.Type value = params[1];
          if (!(key instanceof Class && CharSequence.class
              .isAssignableFrom((Class) key))) throw new AvroTypeException(
              "Map key class not CharSequence: " + key);
          setMapValue(t, jsonObject, entry.getValue(), (Class) key,
              (Class) value);
          // Object map = jp.getMap(path, (Class) key, (Class) value);
          // entry.getValue().set(t, map);
          
        } else {
          setValue(t, jsonObject, entry.getValue(), (Class) raw);
          // Object value = jp.getObject(path, (Class) raw);
          // entry.getValue().set(t, value);
        }
      } else if (type instanceof Class) {
        Class<?> c = (Class<?>) type;
        if (isRawType(c)) {
          setValue(t, jsonObject, entry.getValue());
          // Object value = jp.getObject(path, entry.getValue().getType());
          // entry.getValue().set(t, value);
        } else {
          entry.getValue().set(t, jsonObjectParse(c, jsonObject));
        }
      }
    }
    return t;
  }
  
  private static void setListValue(Object obj, Object jsonObj, Field field,
      Class<?> clazz) throws IllegalArgumentException, IllegalAccessException,
      InstantiationException {
    JPath jpath = field.getAnnotation(JPath.class);
    if (jpath == null) return;
    // String path = jpath.value();
    JsonPath jp = JsonPath.compile(jpath.value(), new Filter[0]);
    Object o = null;
    try {
      o = jp.read(jsonObj);
    } catch (InvalidPathException e) {
      return;
    }
    if (null == o) return;
    // System.out.println(jp.getString(path));
    // 判断当前类型是否为java原始类型，若为原始类型，则标记isRawType为true，否则为false
    boolean isRawType = isRawType(clazz);
    if (o instanceof JSONArray) {
      JSONArray jarr = (JSONArray) o;
      List<Object> olst = new ArrayList<Object>(jarr.size());
      for (Object oo : jarr) {
        if (!isRawType) {
          oo = jsonObjectParse(clazz, oo);
        }
        olst.add(ObjectConverter.convertObjectTo(oo, clazz));
      }
      field.set(obj, olst);
    } else {
      // System.out.println(o.getClass().getName());
      List<Object> olst = new ArrayList<Object>(1);
      if (!isRawType) {
        o = jsonObjectParse(clazz, o);
      }
      olst.add(ObjectConverter.convertObjectTo(o, clazz));
      field.set(obj, olst);
    }
    
  }
  
  private static void setMapValue(Object obj, Object jsonObj, Field field,
      Class<?> kc, Class<?> vc) throws IllegalArgumentException,
      IllegalAccessException, InstantiationException {
    JPath jpath = field.getAnnotation(JPath.class);
    if (jpath == null) return;
    String path = jpath.value();
    JsonPath jp = JsonPath.compile(path, new Filter[0]);
    Object o = null;
    try {
      o = jp.read(jsonObj);
    } catch (InvalidPathException e) {
      return;
    }
    if (null == o) return;
    
    // 判断当前类型是否为java原始类型，若为原始类型，则标记isRawType为true，否则为false
    boolean isRawType = isRawType(vc);
    if (o instanceof JSONObject) {
      HashMap<String,Object> map = Maps.newHashMap();
      JSONObject jobj = (JSONObject) o;
      for (Object s : jobj.keySet()) {
        String key = s.toString();
        Object vo = jobj.get(key);
        if (!isRawType) {
          vo = jsonObjectParse(vc, vo);
        }
        map.put(key, ObjectConverter.convertObjectTo(vo, vc));
      }
      field.set(obj, map);
    } else {
      HashMap<String,Object> map = Maps.newHashMap();
      if (!isRawType) {
        o = jsonObjectParse(vc, o);
      }
      map.put("key", ObjectConverter.convertObjectTo(o, vc));
      field.set(obj, map);
    }
  }
  
  private static void setValue(Object obj, Object jsonObj, Field field)
      throws IllegalArgumentException, IllegalAccessException {
    setValue(obj, jsonObj, field, field.getType());
  }
  
  private static void setValue(Object obj, Object jsonObj, Field field,
      Class<?> clazz) throws IllegalArgumentException, IllegalAccessException {
    JPath jpath = field.getAnnotation(JPath.class);
    if (jpath == null) return;
    JsonPath jp = JsonPath.compile(jpath.value(), new Filter[0]);
    Object o = null;
    try {
      o = jp.read(jsonObj);
    } catch (InvalidPathException e) {
      return;
    }
    if (null == o) return;
    // System.out.println(jp.getString(path));
    
    Object value = ObjectConverter.convertObjectTo(o, clazz);
    if (null != value) field.set(obj, value);
  }
  
  public static Map<String,Field> getFields(Class recordClass) {
    Map<String,Field> fields = new LinkedHashMap<String,Field>();
    Class c = recordClass;
    do {
      if (c.getPackage() != null
          && c.getPackage().getName().startsWith("java.")) break; // skip java
                                                                  // built-in
                                                                  // classes
      for (Field field : c.getDeclaredFields())
        if ((field.getModifiers() & (Modifier.TRANSIENT | Modifier.STATIC)) == 0) if (fields
            .put(field.getName(), field) != null) throw new AvroTypeException(c
            + " contains two fields named: " + field);
      c = c.getSuperclass();
    } while (c != null);
    return fields;
  }
  
  /*
   *  判断当前类型是否为java原始类型，若为原始类型，返回ture，否则返回false
   */
  private static boolean isRawType(Class<?> clazz) {
    if (clazz.isPrimitive()
        || // primitives
        clazz == Void.class || clazz == Boolean.class || clazz == Integer.class
        || clazz == Long.class || clazz == Float.class || clazz == Double.class
        || clazz == Byte.class || clazz == Short.class
        || clazz == Character.class || clazz == String.class) {
      return true;
    } else {
      return false;
    }
  }
}
