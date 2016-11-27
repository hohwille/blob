/* Copyright (c) The m-m-m Team, Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0 */
package net.sf.mmm.blob.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import net.sf.mmm.blob.api.BlobStore;
import net.sf.mmm.util.component.base.AbstractLoggableComponent;
import net.sf.mmm.util.data.api.id.Id;
import net.sf.mmm.util.file.api.FileCreationFailedException;
import net.sf.mmm.util.file.api.FileUtil;
import net.sf.mmm.util.file.base.FileUtilImpl;
import net.sf.mmm.util.io.api.IoMode;
import net.sf.mmm.util.io.api.RuntimeIoException;
import net.sf.mmm.util.io.api.StreamUtil;
import net.sf.mmm.util.io.base.StreamUtilImpl;
import net.sf.mmm.util.lang.api.StringUtil;
import net.sf.mmm.util.lang.base.StringUtilImpl;
import net.sf.mmm.util.resource.api.DataResource;
import net.sf.mmm.util.resource.base.FileResource;
import net.sf.mmm.util.resource.base.UnavailableResource;

/**
 * This is an implementation of {@link BlobStore} simply writing the BLOBs into the local file-system.<br>
 * <b>ATTENTION:</b><br>
 * This implementation is fast and simple, however it has various limitations:
 * <ul>
 * <li>It is not scalable (you would need to write to a remote filesystem to cluster the store what would cause other
 * problems). If you want to have a scalable {@link BlobStore} use a different implementation.</li>
 * <li>It is not (fully) transactional. Files are initially written to a temporary income folder before being moved to
 * their final destination to allow a minimal TX support. That income folder is cleaned on startup. However this
 * approach can not lead to reliable TX support. If you want to have a highly concurrent {@link BlobStore} use a
 * different implementation.</li>
 * </ul>
 * To summarize: The implementations provided with {@code mmm-blob-impl-fs} are fine to build smaller systems like a
 * home server for personal or family usage. They are also simple and fast. Further they allow you to directly use
 * (read) the data from the store directly (e.g. create symlinks to your personal directories). However, for
 * professional usage with many concurrent users you shall <b>never</b> choose this implementation.
 *
 * @author hohwille
 * @since 1.0.0
 */
public abstract class AbstractBlobStoreFs extends AbstractLoggableComponent implements BlobStore {

  /**
   * {@link File#getName() Name} of the {@link File#isDirectory() folder} for the {@link #getIncomeDirectory() income
   * directory}.
   */
  static final String FOLDER_INCOME = ".in";

  /**
   * {@link File#getName() Name} of the {@link File#isDirectory() folder} for the {@link #getDataDirectory() data
   * directory}.
   */
  static final String FOLDER_DATA = "data";

  /** Generic {@link File#getName() name} of the {@link File#isFile() file} of a BLOB. */
  static final String FILE_BLOB = "blob";

  private BlobStoreFsConfigProperties config;

  private FileUtil fileUtil;

  private StreamUtil streamUtil;

  private StringUtil stringUtil;

  private File rootDirectory;

  private File incomeDirectory;

  private File dataDirectory;

  /**
   * The constructor.
   */
  public AbstractBlobStoreFs() {
    super();
  }

  /**
   * @return the {@link BlobStoreFsConfigProperties}.
   */
  protected BlobStoreFsConfigProperties getConfig() {

    return this.config;
  }

  /**
   * @param config the {@link BlobStoreFsConfigProperties} to {@link Inject}.
   */
  @Inject
  public void setConfig(BlobStoreFsConfigProperties config) {

    this.config = config;
  }

  /**
   * @return the {@link FileUtil} instance.
   */
  protected FileUtil getFileUtil() {

    return this.fileUtil;
  }

  /**
   * @param fileUtil the {@link FileUtil} to {@link Inject}.
   */
  @Inject
  public void setFileUtil(FileUtil fileUtil) {

    this.fileUtil = fileUtil;
  }

  /**
   * @return the {@link StreamUtil} instance.
   */
  protected StreamUtil getStreamUtil() {

    return this.streamUtil;
  }

  /**
   * @param streamUtil the {@link StreamUtil} to {@link Inject}.
   */
  @Inject
  public void setStreamUtil(StreamUtil streamUtil) {

    this.streamUtil = streamUtil;
  }

  /**
   * @return the {@link StringUtil} instance.
   */
  protected StringUtil getStringUtil() {

    return this.stringUtil;
  }

  /**
   * @param stringUtil the {@link StringUtil} to {@link Inject}.
   */
  @Inject
  public void setStringUtil(StringUtil stringUtil) {

    this.stringUtil = stringUtil;
  }

  @Override
  protected void doInitialize() {

    super.doInitialize();
    if (this.config == null) {
      this.config = new BlobStoreFsConfigProperties();
    }
    if (this.fileUtil == null) {
      this.fileUtil = FileUtilImpl.getInstance();
    }
    if (this.streamUtil == null) {
      this.streamUtil = StreamUtilImpl.getInstance();
    }
    if (this.stringUtil == null) {
      this.stringUtil = StringUtilImpl.getInstance();
    }
    this.rootDirectory = new File(this.config.getDirectory());
    this.dataDirectory = new File(this.rootDirectory, FOLDER_DATA);
    this.fileUtil.mkdirs(this.dataDirectory);
    this.incomeDirectory = new File(this.rootDirectory, FOLDER_INCOME);
    boolean created = this.fileUtil.mkdirs(this.incomeDirectory);
    if (!created) {
      long age = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(24);
      this.fileUtil.deleteChildren(this.incomeDirectory, f -> (f.lastModified() < age));
    }
  }

  @Override
  public Id<DataResource> createId(String id) {

    return BlobId.of(id);
  }

  /**
   * @return the {@link File} pointing to the root directory of this store.
   */
  protected File getRootDirectory() {

    return this.rootDirectory;
  }

  /**
   * @return the {@link File} pointing to the folder for incoming BLOBs (written there temporary and then moved to the
   *         {@link #getDataDirectory() final} destination).
   */
  protected File getIncomeDirectory() {

    return this.incomeDirectory;
  }

  /**
   * @return the {@link File} pointing to the folder for the persistent data.
   */
  protected File getDataDirectory() {

    return this.dataDirectory;
  }

  /**
   * @param blob the BLOB to {@link #save(DataResource) save}.
   * @return the {@link BlobContext} with the store specific logic for saving the BLOB.
   */
  protected abstract BlobContext createContext(DataResource blob);

  @Override
  public Id<DataResource> save(DataResource blob) {

    BlobContext context = createContext(blob);
    File incomeFile = context.getIncomeFile();
    try (FileOutputStream out = new FileOutputStream(incomeFile); InputStream in = context.openStream()) {
      long size = this.streamUtil.transfer(in, out, false);
      getLogger().debug("Saved {} bytes to {}", Long.valueOf(size), incomeFile);
      BlobId id = context.commit();
      return id;
    } catch (IOException e) {
      throw new RuntimeIoException(e, IoMode.COPY);
    }
  }

  @Override
  public DataResource find(Id<DataResource> id) {

    BlobId blobId = asBlobId(id);
    File blobFile = getBlobFile(blobId);
    if (blobFile == null) {
      return UnavailableResource.INSTANCE;
    }
    return new FileResource(blobFile);
  }

  @Override
  public boolean delete(Id<DataResource> id) {

    BlobId blobId = asBlobId(id);
    File blobFile = getBlobFile(blobId);
    return this.fileUtil.delete(blobFile);
  }

  /**
   * @param id the {@link Id} to the requested BLOB.
   * @return the {@link File} pointing to the BLOB or {@code null} if not available.
   */
  protected BlobId asBlobId(Id<DataResource> id) {

    Objects.requireNonNull(id, "id");
    BlobId blobId;
    if (id instanceof BlobId) {
      blobId = (BlobId) id;
    } else {
      blobId = BlobId.of(id.getId().toString());
    }
    return blobId;
  }

  /**
   * @param id the {@link BlobId} to the requested BLOB.
   * @return the {@link File} pointing to the BLOB or {@code null} if not available.
   */
  protected File getBlobFile(BlobId id) {

    String path = Util.toPath(id.getPartition());
    String folder = id.getFolder();
    if ((folder != null) && (!folder.isEmpty())) {
      path = path + folder;
    }
    File blobFolder = new File(this.dataDirectory, path);
    return getBlobFile(blobFolder, id);
  }

  /**
   * @param blobDirectory the {@link File#isDirectory() directory} where the BLOB should be located.
   * @param id the {@link BlobId}.
   * @return the BLOB {@link File}.
   */
  protected File getBlobFile(File blobDirectory, BlobId id) {

    return new File(blobDirectory, FILE_BLOB);
  }

  /**
   * The context for the current BLOB to save.
   */
  protected class BlobContext {

    /** @see #getBlob() */
    protected final DataResource blob;

    /** @see #getIncomeFile() */
    private File incomeFile;

    /**
     * The constructor.
     *
     * @param blob - see {@link #getBlob()}.
     */
    public BlobContext(DataResource blob) {
      super();
      this.blob = blob;
    }

    /**
     * @return the unique {@link File} in {@link AbstractBlobStoreFs#getIncomeDirectory()}.
     */
    protected File createIncomeFile() {

      String partition = createUniqueId();
      File inFile = new File(AbstractBlobStoreFs.this.incomeDirectory, partition);
      if (inFile.exists()) {
        getLogger().debug("Income file collision for {}", partition);
        partition = createUniqueId();
        inFile = new File(AbstractBlobStoreFs.this.incomeDirectory, partition);
        if (inFile.exists()) {
          throw new FileCreationFailedException(inFile);
        }
      }
      return inFile;
    }

    /**
     * @return a unique ID used for file/folder names to prevent collisions.
     */
    protected String createUniqueId() {

      long id = (System.currentTimeMillis() << 16) + System.nanoTime() + (Thread.currentThread().getName().hashCode() << 32);
      return Long.toString(id, 16);
    }

    /**
     * @return the BLOB to save in the store.
     */
    public DataResource getBlob() {

      return this.blob;
    }

    /**
     * @return the {@link File} pointing to the BLOB in the {@link AbstractBlobStoreFs#getIncomeDirectory() income
     *         directory}.
     */
    public File getIncomeFile() {

      if (this.incomeFile == null) {
        this.incomeFile = createIncomeFile();
      }
      return this.incomeFile;
    }

    /**
     * @return the {@link DataResource#openStream() BLOB stream}. Method may be overridden to wrap the stream for
     *         additional features.
     */
    public InputStream openStream() {

      return this.blob.openStream();
    }

    /**
     * This method is called after the BLOB has been successfully written to the {@link #getIncomeFile() income file}.
     * It will move that file to its unique and final destination.
     *
     * @return the created {@link Id} pointing to the final BLOB.
     */
    public BlobId commit() {

      String partition = getPartition();
      File dataFile = createDataFile(partition);
      if (dataFile.exists()) {
        getLogger().debug("BLOB {} caused a collision at {}", this.blob.getName(), partition);
        return handleCollision(dataFile);
      }
      doCommit(dataFile);
      return new BlobId(partition, getFolder(), getCopy());
    }

    /**
     * @return the {@link BlobId#getPartition() partition}.
     */
    protected String getPartition() {

      return this.incomeFile.getName();
    }

    /**
     * @return the {@link BlobId#getFolder() folder}.
     */
    protected String getFolder() {

      return null;
    }

    /**
     * @return the {@link BlobId#getCopy() copy}.
     */
    protected String getCopy() {

      return null;
    }

    /**
     * @param dataFile the {@link #createDataFile(String) data file} pointing to the BLOB that already exists and caused
     *        the collision.
     * @return the new {@link File} where the BLOB has been {@link #doCommit(File) committed}.
     */
    protected BlobId handleCollision(File dataFile) {

      String partition = createUniqueId();
      File newFile = createDataFile(partition);
      if (newFile.exists()) {
        throw new FileCreationFailedException(newFile.getPath(), true);
      }
      doCommit(newFile);
      return new BlobId(partition, getFolder(), getCopy());
    }

    /**
     * @param partition the {@link BlobId#getPartition() partition} of the BLOB.
     * @return the data {@link File} to {@link #doCommit(File) commit}.
     */
    protected File createDataFile(String partition) {

      File dataFolder = new File(AbstractBlobStoreFs.this.dataDirectory, createDataPath(partition));
      AbstractBlobStoreFs.this.fileUtil.mkdirs(dataFolder);
      return createDataFile(partition, dataFolder);
    }

    /**
     * @param id the ID of the BLOB.
     * @return the path inside the {@link AbstractBlobStoreFs#getDataDirectory() data directory} where to store the
     *         BLOB.
     */
    protected String createDataPath(String id) {

      String path = Util.toPath(id);
      String folder = getFolder();
      if (folder != null) {
        path = path + folder;
      }
      return path;
    }

    /**
     * @param partition the {@link BlobId#getPartition() partition} of the BLOB.
     * @param dataFolder the {@link File#isDirectory() directory} where to place the data {@link File}.
     * @return the data {@link File} to {@link #doCommit(File) commit}.
     */
    protected File createDataFile(String partition, File dataFolder) {

      return new File(dataFolder, FILE_BLOB);
    }

    /**
     * @param blobFile the data {@link File} where to move the {@link #createIncomeFile() income file} to as the final
     *        destination for the BLOB.
     */
    protected void doCommit(File blobFile) {

      boolean success = this.incomeFile.renameTo(blobFile);
      if (!success) {
        throw new IllegalStateException("Failed to move " + this.incomeFile + " to " + blobFile);
      }
    }
  }

}
