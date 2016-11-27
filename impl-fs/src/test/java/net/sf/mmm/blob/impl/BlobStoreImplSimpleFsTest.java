/* Copyright (c) The m-m-m Team, Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0 */
package net.sf.mmm.blob.impl;

/**
 * The test-case for {@link BlobStoreImplSimpleFs}.
 *
 * @author hohwille
 */
public class BlobStoreImplSimpleFsTest extends AbstractBlobStoreFsTest {

  @Override
  protected AbstractBlobStoreFs createBlobStore() {

    return new BlobStoreImplSimpleFs();
  }

  @Override
  protected boolean isDeduplicating() {

    return false;
  }

  @Override
  protected boolean isReferenceCounting() {

    return false;
  }

}
