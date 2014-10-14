package com.moz.qless.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Strings;

public class MapUtils {
  public static String get(final Map<String, Object> opts, final String opt) {
    return MapUtils.get(opts, opt, null);
  }

  public static String get(final Map<String, Object> opts, final String opt,
      final String defaultValue) {
    if ((null == opts) || Strings.isNullOrEmpty(opt)) {
      return defaultValue;
    }

    final Object value = opts.get(opt);
    return null == value ? defaultValue : value.toString();
  }

  public static List<String> getList(final Map<String, Object> opts, final String opt) {
    return MapUtils.getList(opts, opt, new ArrayList<String>());
  }

  @SuppressWarnings("unchecked")
  public static List<String> getList(final Map<String, Object> opts, final String opt,
      final List<String> defaultValue) {
    if ((null == opts) || Strings.isNullOrEmpty(opt)) {
      return defaultValue;
    }

    final Object value = opts.get(opt);
    return null == value ? defaultValue : (List<String>) value;
  }
}
