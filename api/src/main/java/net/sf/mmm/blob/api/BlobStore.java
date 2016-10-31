/* Copyright (c) The m-m-m Team, Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0 */
package net.sf.mmm.blob.api;

import net.sf.mmm.util.resource.api.DataResource;

/**
 * This is the interface for a storage of <em>Binary Large OBjects</em> (BLOBs).
 *
 * @author hohwille
 * @since 8.0.0
 */
public interface BlobStore {

  /**
   * Saves a
   *
   * @param file
   * @return
   */
  String save(DataResource file);

}
