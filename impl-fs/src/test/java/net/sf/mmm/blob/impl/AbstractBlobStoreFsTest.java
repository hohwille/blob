/* Copyright (c) The m-m-m Team, Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0 */
package net.sf.mmm.blob.impl;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import net.sf.mmm.util.data.api.id.Id;
import net.sf.mmm.util.file.api.FileUtil;
import net.sf.mmm.util.file.api.FileUtilLimited;
import net.sf.mmm.util.file.base.FileUtilImpl;
import net.sf.mmm.util.io.api.IoMode;
import net.sf.mmm.util.io.api.RuntimeIoException;
import net.sf.mmm.util.io.base.StreamUtilImpl;
import net.sf.mmm.util.resource.api.DataResource;
import net.sf.mmm.util.resource.base.ClasspathResource;

/**
 * The base class for a test-case of {@link AbstractBlobStoreFs}.
 *
 * @author hohwille
 */
public abstract class AbstractBlobStoreFsTest extends Assertions {

  /**
   * @return the {@link BlobStoreFsConfigProperties} to use for testing.
   */
  protected BlobStoreFsConfigProperties getConfig() {

    BlobStoreFsConfigProperties config = new BlobStoreFsConfigProperties();
    String directory = System.getProperty(FileUtilLimited.PROPERTY_TMP_DIR) + "/.blobs";
    config.setDirectory(directory);
    return config;
  }

  /**
   * @return the initialized {@link AbstractBlobStoreFs} to test.
   */
  protected AbstractBlobStoreFs getBlobStore() {

    AbstractBlobStoreFs store = createBlobStore();
    store.setConfig(getConfig());
    store.initialize();
    FileUtil fileUtil = FileUtilImpl.getInstance();
    fileUtil.deleteChildren(store.getIncomeDirectory());
    fileUtil.deleteChildren(store.getDataDirectory());
    return store;
  }

  /**
   * @return a new uninitialized instance of the {@link AbstractBlobStoreFs} to test.
   */
  protected abstract AbstractBlobStoreFs createBlobStore();

  /**
   * @return {@code true} if {@link #getBlobStore() BLOB store} to test is de-duplicating, {@code false} otherwise.
   */
  protected abstract boolean isDeduplicating();

  /**
   * @return {@code true} if {@link #getBlobStore() BLOB store} to test is de-duplicating with reference count,
   *         {@code false} otherwise.
   */
  protected abstract boolean isReferenceCounting();

  /**
   * Test of create, retrieve, update, and delete cycle.
   */
  @Test
  public void testCrud() {

    AbstractBlobStoreFs store = getBlobStore();
    File dataDirectory = store.getDataDirectory();
    assertThat(dataDirectory).exists().isDirectory();
    assertThat(dataDirectory.list()).isEmpty();
    File incomeDirectory = store.getIncomeDirectory();
    assertThat(incomeDirectory).exists().isDirectory();
    assertThat(incomeDirectory.list()).isEmpty();
    ClasspathResource resource = new ClasspathResource(AbstractBlobStoreFs.class, ".class", true);
    assertThat(resource.isAvailable()).isTrue();

    Id<DataResource> id = store.save(resource);
    assertThat(id).isNotNull();
    DataResource blob = store.load(id);
    assertEquals(blob, resource);

    DataResource resource2 = resource; // duplicate
    Id<DataResource> id2 = store.save(resource2);
    DataResource blob2 = store.load(id2);
    if (isDeduplicating()) {
      assertThat(blob2.getUri()).isEqualTo(blob.getUri());
    } else {
      assertThat(blob2.getUri()).isNotEqualTo(blob.getUri());
    }

    ClasspathResource resource3 = new ClasspathResource(AbstractBlobStoreFsTest.class, ".class", true);
    Id<DataResource> id3 = store.save(resource3);
    assertThat(id3).isNotEqualTo(id);
    DataResource blob3 = store.load(id3);
    assertThat(blob3.getUri()).isNotEqualTo(blob.getUri());

    boolean deleted = store.delete(id);
    assertThat(deleted).isTrue();
    if (isDeduplicating() && isReferenceCounting()) {
      assertThat(blob.isAvailable()).isTrue();
    } else {
      assertThat(blob.isAvailable()).isFalse();
    }
    assertThat(store.find(id).isAvailable()).isFalse();
    deleted = store.delete(id);
    assertThat(deleted).isFalse();

    if (isDeduplicating() && !isReferenceCounting()) {
      assertThat(blob2.isAvailable()).isFalse();
    } else {
      assertThat(blob2.isAvailable()).isTrue();
      deleted = store.delete(id2);
      assertThat(deleted).isTrue();
      assertThat(blob2.isAvailable()).isFalse();
    }

    deleted = store.delete(id3);
    assertThat(deleted).isTrue();
  }

  private void assertEquals(DataResource resource, DataResource expected) {

    assertThat(resource).isNotNull();
    assertThat(resource.isAvailable()).isTrue();
    assertThat(resource.getSize()).isEqualTo(expected.getSize());
    byte[] data1 = loadResource(resource);
    byte[] data2 = loadResource(expected);
    assertThat(data1).isEqualTo(data2);
  }

  private byte[] loadResource(DataResource resource) {

    try (InputStream in = resource.openStream()) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      StreamUtilImpl.getInstance().transfer(in, out, false);
      byte[] data = out.toByteArray();
      return data;
    } catch (IOException e) {
      throw new RuntimeIoException(e, IoMode.READ);
    }
  }

}
