/* Copyright (c) The m-m-m Team, Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0 */
package net.sf.mmm.blob.impl;

/**
 * The test-case for {@link BlobStoreImplDeduplicatingFsWithRefCount}.
 *
 * @author hohwille
 */
public class BlobStoreImplDeduplicatingFsWithRefCountTest extends AbstractBlobStoreFsTest {

  @Override
  protected AbstractBlobStoreFs createBlobStore() {

    return new BlobStoreImplDeduplicatingFsWithRefCount();
  }

  @Override
  protected boolean isDeduplicating() {

    return true;
  }

  @Override
  protected boolean isReferenceCounting() {

    return true;
  }

}
