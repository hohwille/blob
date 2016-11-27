/* Copyright (c) The m-m-m Team, Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0 */
package net.sf.mmm.blob.impl;

import org.springframework.boot.context.properties.ConfigurationProperties;

import net.sf.mmm.util.file.api.FileUtilLimited;

/**
 * The {@link ConfigurationProperties} for {@link BlobStoreImplDeduplicatingFs}.
 *
 * @author hohwille
 * @since 1.0.0
 */
@ConfigurationProperties(prefix = "blob.store.fs")
public class BlobStoreFsConfigProperties {

  private String directory = System.getProperty(FileUtilLimited.PROPERTY_USER_HOME) + "/.mmm/blobstore";

  private String digest = "MD5";

  /**
   * The constructor.
   */
  public BlobStoreFsConfigProperties() {
    super();
  }

  /**
   * @return the root directory where the BLOBs are stored on the filesystem.
   */
  public String getDirectory() {

    return this.directory;
  }

  /**
   * @param directory the new value of {@link #getDirectory()}.
   */
  public void setDirectory(String directory) {

    this.directory = directory;
  }

  /**
   * @return the name of the {@link java.security.MessageDigest} used to calculate a hash of each BLOB. The default is
   *         MD5. Algorithms like SHA-256 will cause less collisions and are preferred for cryptographic use. However,
   *         all we need here is a short and simple indicator that gives a hint if two files (of the same size)
   *         <b>may</b> be identical. Further we also use the hash as part of the ID followed by a unique counter and
   *         longer hashes will result in waste of resources (longer IDs, extra folders on the disc, etc).
   */
  public String getDigest() {

    return this.digest;
  }

  /**
   * @param digest the new value of {@link #getDigest()}.
   */
  public void setDigest(String digest) {

    this.digest = digest;
  }

}
