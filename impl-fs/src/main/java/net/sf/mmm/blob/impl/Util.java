/* Copyright (c) The m-m-m Team, Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0 */
package net.sf.mmm.blob.impl;

/**
 * This is a simple helper class to dump {@code byte[]} data into a hexadecimal {@link String}.
 *
 * @author hohwille
 * @since 1.0.0
 */
class Util {

  private Util() {
    super();
  }

  static String toPath(String id) {

    int length = id.length();
    StringBuilder sb = new StringBuilder(length + 8);
    int start = 0;
    while (start < length) {
      int next = start + 4;
      int end = next;
      if (end > length) {
        end = length;
      }
      sb.append(id.subSequence(start, end));
      sb.append('/');
      start = next;
    }
    return sb.toString();
  }

}
