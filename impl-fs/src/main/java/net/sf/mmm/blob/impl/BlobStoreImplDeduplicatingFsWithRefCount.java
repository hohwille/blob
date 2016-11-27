/* Copyright (c) The m-m-m Team, Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0 */
package net.sf.mmm.blob.impl;

import java.io.File;

import net.sf.mmm.util.data.api.id.Id;
import net.sf.mmm.util.resource.api.DataResource;

/**
 * This class extends {@link BlobStoreImplDeduplicatingFs} with a reference count mechanism. It creates {@link Id}s
 * including a reference counter (copy index). Further, it ensures that if {@code N} duplicates of a file have been
 * saved then only after all {@code N} of them have been deleted, the actual BLOB will be deleted physically from the
 * disc.
 *
 * @author hohwille
 * @since 1.0.0
 */
public class BlobStoreImplDeduplicatingFsWithRefCount extends BlobStoreImplDeduplicatingFs {

  /**
   * The constructor.
   */
  public BlobStoreImplDeduplicatingFsWithRefCount() {
    super();
  }

  @Override
  protected DeduplicatingBlobContextWithRefCount createContext(DataResource blob) {

    return new DeduplicatingBlobContextWithRefCount(blob);
  }

  @Override
  public boolean delete(Id<DataResource> id) {

    BlobId blobId = asBlobId(id);
    File blobFile = getBlobFile(blobId);
    if (blobFile == null) {
      return false;
    }
    File blobDirectory = blobFile.getParentFile();
    String copy = blobId.getCopy();
    if (copy == null) {
      throw new IllegalArgumentException(id.toString());
    }
    File referenceFile = new File(blobDirectory, copy);
    boolean deleted = getFileUtil().delete(referenceFile);
    File[] children = blobDirectory.listFiles();
    if (children.length > 1) {
      return deleted;
    }
    if (children.length == 1) {
      File child = children[0];
      if (child.equals(blobFile)) {
        boolean success = getFileUtil().delete(blobFile);
        if (!success) {
          getLogger().info("BLOB was already deleted at {}", blobFile);
        }
      } else {
        getLogger().warn("Unexpected file left in BLOB folder {}", child);
      }
    }
    boolean success = getFileUtil().delete(blobDirectory);
    if (!success) {
      if (blobDirectory.exists()) {
        getLogger().warn("BLOB directory could not be deleted as expected at {}", blobDirectory);
      } else {
        getLogger().info("BLOB directory was already deleted at {}", blobDirectory);
      }
    }
    return deleted;
  }

  @Override
  protected File getBlobFile(File blobDirectory, BlobId id) {

    String copy = id.getCopy();
    if (copy != null) {
      File copyFile = new File(blobDirectory, copy);
      if (!copyFile.exists()) {
        return null;
      }
    }
    return super.getBlobFile(blobDirectory, id);
  }

  /**
   * Sublcass of {@link BlobStoreImplDeduplicatingFs.DeduplicatingBlobContext} that manages unique {@link BlobId}s with
   * individual {@link BlobId#getCopy() copy identifiers}.
   */
  protected class DeduplicatingBlobContextWithRefCount extends DeduplicatingBlobContext {

    private static final String DEFAULT_COPY_REFERENCE = "1";

    private String copy;

    /**
     * The constructor.
     *
     * @param blob - see {@link #getBlob()}.
     */
    public DeduplicatingBlobContextWithRefCount(DataResource blob) {
      super(blob);
      this.copy = DEFAULT_COPY_REFERENCE;
    }

    @Override
    protected String getCopy() {

      return this.copy;
    }

    @Override
    protected void doCommit(File blobFile) {

      super.doCommit(blobFile);
      File copyReference = new File(blobFile.getParentFile(), DEFAULT_COPY_REFERENCE);
      createCopyReferenceFile(copyReference);
    }

    @Override
    protected void deduplicate(File incomeFile, File blobFile) {

      File blobDirectory = blobFile.getParentFile();
      File copyReference = findUniqueFilename(blobDirectory);
      createCopyReferenceFile(copyReference);
      super.deduplicate(incomeFile, blobFile);
    }

    private void createCopyReferenceFile(File copyReference) {

      boolean created = getFileUtil().ensureFileExists(copyReference);
      assert (created);
      this.copy = copyReference.getName();
    }

  }

}
