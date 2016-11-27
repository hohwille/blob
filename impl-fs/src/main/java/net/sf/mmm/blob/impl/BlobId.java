/* Copyright (c) The m-m-m Team, Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0 */
package net.sf.mmm.blob.impl;

import java.io.File;

import net.sf.mmm.util.data.api.id.AbstractId;
import net.sf.mmm.util.data.api.id.Id;
import net.sf.mmm.util.lang.api.attribute.AttributeReadId;
import net.sf.mmm.util.resource.api.DataResource;

/**
 * This is the implementation of {@link Id} pointing to a <em>Binary Large OBject</em> (BLOB).
 *
 * @author hohwille
 * @since 1.0.0
 */
public class BlobId extends AbstractId<DataResource> implements AttributeReadId<String> {

  private static final char SEPARATOR_FOLDER = '/';

  private static final char SEPARATOR_COPY = '#';

  private final String partition;

  private final String folder;

  private final String copy;

  private final String id;

  /**
   * The constructor.
   *
   * @param partition - see {@link #getPartition()}.
   * @param folder - see {@link #getFolder()}.
   * @param copy - see {@link #getCopy()}.
   */
  public BlobId(String partition, String folder, String copy) {

    this(partition, folder, copy, VERSION_LATEST);
  }

  /**
   * The constructor.
   *
   * @param partition - see {@link #getPartition()}.
   * @param folder - see {@link #getFolder()}.
   * @param copy - see {@link #getCopy()}.
   * @param version - see {@link #getVersion()}.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public BlobId(String partition, String folder, String copy, long version) {

    this(partition, folder, copy, version, createId(partition, folder, copy));
  }

  /**
   * The constructor.
   *
   * @param type - see {@link #getType()}.
   * @param partition - see {@link #getPartition()}.
   * @param folder - see {@link #getFolder()}.
   * @param copy - see {@link #getCopy()}.
   * @param version - see {@link #getVersion()}.
   */
  private BlobId(String partition, String folder, String copy, long version, String id) {
    super(DataResource.class, version);
    this.partition = partition;
    this.folder = folder;
    this.copy = copy;
    this.id = id;
  }

  private static String createId(String partition, String folder, String copy) {

    if ((folder == null) && (copy == null)) {
      return partition;
    }
    StringBuilder sb = new StringBuilder(partition);
    if (folder != null) {
      sb.append(SEPARATOR_FOLDER);
      sb.append(folder);
    }
    if (copy != null) {
      sb.append(SEPARATOR_COPY);
      sb.append(copy);
    }
    return sb.toString();
  }

  @Override
  public String getId() {

    return this.id;
  }

  /**
   * @return the partition what is either a unique ID or an hash (MD5, SHA2, etc.) of the BLOB. May not be {@code null}.
   */
  public String getPartition() {

    return this.partition;
  }

  /**
   * @return the optional {@link File#isDirectory() folder} {@link File#getName() name} or {@code null}. If present
   *         indicates a folder within the {@link #getPartition() partition} that uniquely identifies the BLOB.
   */
  public String getFolder() {

    return this.folder;
  }

  /**
   * @return the optional copy identifier (e.g. reference count) of a de-duplicated BLOB or {@code null}.
   */
  public String getCopy() {

    return this.copy;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  protected <T> AbstractId<T> newId(Class<T> newType, long newVersion) {

    if (!DataResource.class.isAssignableFrom(newType)) {
      throw new IllegalStateException(newType.getName());
    }
    return (AbstractId) new BlobId(this.partition, this.folder, this.copy, newVersion, this.id);
  }

  /**
   * @param id the {@link #getId() ID} {@link String}.
   * @return the new {@link BlobId} instance.
   */
  public static BlobId of(String id) {

    return of(id, VERSION_LATEST);
  }

  /**
   * @param id the {@link #getId() ID} {@link String}.
   * @param version the {@link #getVersion() version}.
   * @return the new {@link BlobId} instance.
   */
  public static BlobId of(String id, long version) {

    String partition = id;
    int idEnd = id.length();
    String copy = null;
    int copyStart = id.indexOf(SEPARATOR_COPY);
    if (copyStart > 0) {
      copy = id.substring(copyStart + 1);
      idEnd = copyStart;
    }
    String folder = null;
    int folderStart = id.lastIndexOf(SEPARATOR_FOLDER, idEnd);
    if (folderStart > 0) {
      folder = id.substring(folderStart + 1, idEnd);
      idEnd = folderStart;
    }
    if ((folder != null) || (copy != null)) {
      partition = id.substring(0, idEnd);
    }
    return new BlobId(partition, folder, copy, version, id);
  }

}
