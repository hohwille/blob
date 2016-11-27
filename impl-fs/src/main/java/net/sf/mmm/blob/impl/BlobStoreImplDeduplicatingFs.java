/* Copyright (c) The m-m-m Team, Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0 */
package net.sf.mmm.blob.impl;

import java.io.File;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;

import net.sf.mmm.blob.api.BlobStore;
import net.sf.mmm.util.resource.api.DataResource;

/**
 * This is an implementation of {@link BlobStore} that writes the BLOBs into the local file-system. For general
 * limitations see {@link AbstractBlobStoreFs}. Additionally this implementation calculates a hash of the file on the
 * fly when writing to disk. It will then check if the file is already in the store and in that case avoid to create a
 * physical duplicate on the disc.
 *
 * @author hohwille
 * @since 1.0.0
 */
public class BlobStoreImplDeduplicatingFs extends AbstractBlobStoreFs {

  /**
   * The constructor.
   */
  public BlobStoreImplDeduplicatingFs() {
    super();
  }

  @Override
  protected DeduplicatingBlobContext createContext(DataResource blob) {

    return new DeduplicatingBlobContext(blob);
  }

  /**
   * Sublcass of {@link AbstractBlobStoreFs.BlobContext} that creates hash of BLOB and detects duplicates.
   */
  protected class DeduplicatingBlobContext extends BlobContext {

    private final MessageDigest digest;

    private String folder;

    private String hash;

    /**
     * The constructor.
     *
     * @param blob - see {@link #getBlob()}.
     */
    public DeduplicatingBlobContext(DataResource blob) {
      super(blob);
      this.folder = "1";
      try {
        this.digest = MessageDigest.getInstance(getConfig().getDigest());
      } catch (NoSuchAlgorithmException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public InputStream openStream() {

      return new DigestInputStream(super.openStream(), this.digest);
    }

    @Override
    public BlobId commit() {

      this.hash = getStringUtil().toHex(this.digest.digest());
      return super.commit();
    }

    @Override
    protected String getFolder() {

      return this.folder;
    }

    @Override
    protected File createDataFile(String id, File dataFolder) {

      return new File(dataFolder, FILE_BLOB);
    }

    /**
     * @return the hash of the BLOB. Will be {@code null} if called before {@link #commit()}.
     */
    protected final String getHash() {

      return this.hash;
    }

    @Override
    protected String getPartition() {

      return this.hash;
    }

    @Override
    protected BlobId handleCollision(File dataFile) {

      File dataFolder = dataFile.getParentFile();
      File partitionFolder = dataFolder.getParentFile();
      File incomeFile = getIncomeFile();
      long size = incomeFile.length();
      File[] children = partitionFolder.listFiles();
      File blobFile = findDuplicate(children, size, incomeFile);
      if (blobFile == null) {
        blobFile = createDataFile(partitionFolder, children);
        doCommit(blobFile);
      } else {
        deduplicate(incomeFile, blobFile);
      }
      return new BlobId(this.hash, this.folder, getCopy());
    }

    /**
     * @param incomeFile the {@link #getIncomeFile() income file} that has been detected as duplicate of
     *        {@code blobFile}.
     * @param blobFile the existing BLOB {@link File}.
     */
    protected void deduplicate(File incomeFile, File blobFile) {

      boolean deleted = incomeFile.delete();
      assert (deleted);
    }

    /**
     * @param incomeFile the {@link #getIncomeFile() income file} to save.
     * @param blobFile an existing BLOB {@link File} with the same hash.
     * @return {@code true} if both {@link File}s are equal, {@code false} otherwise.
     */
    protected boolean areFilesEqual(File incomeFile, File blobFile) {

      // currently we assume that hash collision has extremely low probability if files have the same size...
      // in theory you can find two different files of the same size with the same hash for algorithms like md5 or sha2
      // but for real (multimedia) files this will never happen. Hence, for performance and simplicity we assume
      // equality without checking. Feel free to extend and override if you prefer 100% guarantees.
      return true;
    }

    private File findDuplicate(File[] children, long size, File incomeFile) {

      for (File childFolder : children) {
        File blobFile = new File(childFolder, FILE_BLOB);
        if (blobFile.isFile() && (blobFile.length() == size)) {
          if (areFilesEqual(incomeFile, blobFile)) {
            String folderName = childFolder.getName();
            getLogger().info("BLOB {} is a duplicate of {}/{}", this.blob.getName(), this.hash, folderName);
            this.folder = folderName;
            return blobFile;
          }
        }
      }
      return null;
    }

    /**
     * @param directory the {@link File#isDirectory() directory} where the unique {@link File} shall be
     *        {@link File#getParentFile() located in}.
     * @return the {@link File} {@link File#getParentFile() located in} the given {@code directory} that does not yet
     *         {@link File#exists() exist}.
     */
    protected File findUniqueFilename(File directory) {

      return findUniqueFilename(directory, directory.listFiles());
    }

    /**
     * @param directory the {@link File#isDirectory() directory} where the unique {@link File} shall be
     *        {@link File#getParentFile() located} in.
     * @param children the {@link File#listFiles() children} of the given {@code directory}.
     * @return the {@link File} {@link File#getParentFile() located in} the given {@code directory} that does not yet
     *         {@link File#exists() exist}.
     */
    protected File findUniqueFilename(File directory, File[] children) {

      String name = Long.toString(children.length + 1);
      File file = new File(directory, name);
      if (file.exists()) {
        getLogger().debug("Unexpected collision for file {}", file);
        Set<String> existingChildren = new HashSet<>(children.length);
        for (File child : children) {
          existingChildren.add(child.getName());
        }
        name = null;
        for (int i = 1; i <= children.length; i++) {
          name = Integer.toString(i);
          if (!existingChildren.contains(name)) {
            break;
          }
        }
        file = new File(directory, name);
        assert (!file.exists());
      }
      return file;
    }

    private File createDataFile(File partitionFolder, File[] children) {

      File dataFolder = findUniqueFilename(partitionFolder, children);
      this.folder = dataFolder.getName();
      boolean success = getFileUtil().mkdirs(dataFolder);
      assert (success);
      return new File(dataFolder, FILE_BLOB);
    }

  }

}
