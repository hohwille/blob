/* Copyright (c) The m-m-m Team, Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0 */
package net.sf.mmm.blob.impl;

import org.assertj.core.api.Assertions;
import org.junit.Test;

/**
 * This is the test-case for {@link Util}.
 *
 * @author hohwille
 */
@SuppressWarnings("javadoc")
public class UtilTest extends Assertions {

  /**
   * Test of {@link Util#toPath(String)}.
   */
  @Test
  public void testToPath() {

    assertThat(Util.toPath("0f1e2d3c4b5a6978")).isEqualTo("0f1e/2d3c/4b5a/6978/");
    assertThat(Util.toPath("0f1e2d3c4b5a697")).isEqualTo("0f1e/2d3c/4b5a/697/");
    assertThat(Util.toPath("0f1e2d3c4b5a69")).isEqualTo("0f1e/2d3c/4b5a/69/");
    assertThat(Util.toPath("0f1e2d3c4b5a6")).isEqualTo("0f1e/2d3c/4b5a/6/");
    assertThat(Util.toPath("0f1e2d3c4b5a")).isEqualTo("0f1e/2d3c/4b5a/");
    // strange edge cases
    assertThat(Util.toPath("f")).isEqualTo("f/");
    assertThat(Util.toPath("")).isEqualTo("");
    assertThat(Util.toPath("0/2")).isEqualTo("0/2/");
  }

}
