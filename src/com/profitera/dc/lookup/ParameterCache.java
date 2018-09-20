package com.profitera.dc.lookup;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.profitera.util.ArrayUtil;

public class ParameterCache {
  public static final Object MASK_NULL = new Object();
  private final String[] lookupQueryParams;
  private Map<String, Object> cache;
  private Object specialValue = null;
  private final AtomicInteger count = new AtomicInteger(0);
  private final Integer maximumCacheSize;
  private final int defaultCacheSize;

  public ParameterCache(String[] params, int cacheSize) {
    this(params, cacheSize, null);
  }
  public ParameterCache(String[] params, int cacheSize, Integer maximumCacheSize) {
    defaultCacheSize = cacheSize;
    lookupQueryParams = (String[]) ArrayUtil.copy(params);
    cache = buildCacheMap();
    this.maximumCacheSize = maximumCacheSize;
  }
  private ConcurrentHashMap<String, Object> buildCacheMap() {
    return new ConcurrentHashMap<String, Object>(defaultCacheSize);
  }

  public void storeInCache(Map<String, Object> params, Object valueToCache) {
    Object value = valueToCache;
    String[] lookupParams = lookupQueryParams;
    Map<String, Object> tmpCache = cache;
    if (lookupParams.length == 0) {
      specialValue = value;
    } else {
      if (value == null) {
        value = MASK_NULL;
      }
      for (int i = 0; i < lookupParams.length; i++) {
        String key = getKey(i, params);
        if (i == lookupParams.length - 1) {
          count.incrementAndGet();
          tmpCache.put(key, value);
        } else {
          if (tmpCache.get(key) == null) {
            tmpCache.put(key, new ConcurrentHashMap<String, Object>());
          }
          tmpCache = (Map<String, Object>) tmpCache.get(key);
        }
      }
    }
    if (maximumCacheSize != null && maximumCacheSize <= count.get()) {
      count.set(0);
      cache = buildCacheMap();
    }
  }

  public Object getFromCache(Map<String, Object> params) {
    String[] lookupParams = lookupQueryParams;
    Map<String, Object> tmpCache = cache;
    if (lookupParams.length == 0) {
      return specialValue;
    }
    for (int i = 0; i < lookupParams.length; i++) {
      String key = getKey(i, params);
      if (i == lookupParams.length - 1) {
        Object temp = tmpCache.get(key);
        if (temp == null && tmpCache.containsKey(key)) {
          return MASK_NULL;
        } else {
          return temp;
        }
      } else {
        tmpCache = (Map<String, Object>) tmpCache.get(key);
        if (tmpCache == null) {
          break;
        }
      }
    }
    return null;
  }
  private String getKey(int index, Map<String, Object> params) {
    String lookupParam = lookupQueryParams[index];
    Object paramValue = params.get(lookupParam);
    return index + "=" + paramValue;
  }
}
