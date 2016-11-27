/* Copyright (c) The m-m-m Team, Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0 */
package net.sf.mmm.blob.impl;

import net.sf.mmm.blob.api.BlobStore;
import net.sf.mmm.util.resource.api.DataResource;

/**
 * This is an implementation of {@link BlobStore} that writes the BLOBs into the local file-system. For general
 * limitations see {@link AbstractBlobStoreFs}.
 *
 * @see BlobStoreImplDeduplicatingFs
 *
 * @author hohwille
 * @since 1.0.0
 */
public class BlobStoreImplSimpleFs extends AbstractBlobStoreFs {

  /**
   * The constructor.
   */
  public BlobStoreImplSimpleFs() {
    super();
  }

  @Override
  protected BlobContext createContext(DataResource blob) {

    return new BlobContext(blob);
  }

}
