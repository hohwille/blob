/* Copyright (c) The m-m-m Team, Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0 */
package net.sf.mmm.blob.api;

import net.sf.mmm.util.data.api.id.Id;
import net.sf.mmm.util.data.base.id.StringVersionId;
import net.sf.mmm.util.exception.api.ObjectNotFoundException;
import net.sf.mmm.util.io.api.RuntimeIoException;
import net.sf.mmm.util.resource.api.DataResource;
import net.sf.mmm.util.resource.base.UnavailableResource;

/**
 * This is the interface for a storage of <em>Binary Large OBjects</em> (BLOBs). It abstracts from the technical way
 * that BLOBs are stored. A simple implementation may just write the BLOBs to the local disk what might not be fully
 * transactional. Other implementations may write the BLOB to a cloud service or a database (actually RDBMS are not a
 * good choice for BLOBs). A more sophisticated implementation might also detect duplicates (and could potentially
 * generate IDs with reference counters so the file only gets deleted when as many deletes have been called as saves of
 * the same BLOB occurred before).<br>
 * <b>NOTE:</b><br>
 * A typical usage of a {@link BlobStore} will be used in combination with another database where meta-data is stored.
 * Then the meta-data will contain the {@link Id} of the BLOB. The tricky part is that for 100% abstraction of the BLOB
 * store you will not know the implementation of the {@link Id} used by the {@link BlobStore}. If such high abstraction
 * is required you would need to store the {@link Id} as {@link String} (see {@link #createId(String)}).
 *
 * @author hohwille
 * @since 8.0.0
 */
public interface BlobStore {

  /**
   * Saves a {@link DataResource} as new BLOB in this store.
   *
   * @param blob the {@link DataResource} to save. Has to be fresh such that {@link DataResource#openStream()} ensures
   *        to provide an untouched stream where no bytes may already have been consumed. For
   *        {@link net.sf.mmm.util.resource.base.StreamResource} that may be used here in web-context this has to be
   *        ensured by the caller of this method.
   * @return the {@link Id} to uniquely identify the saved BLOB.
   */
  Id<DataResource> save(DataResource blob);

  /**
   * Loads a BLOB as {@link DataResource} from this store.
   *
   * @param id is the {@link Id} pointing to the requested BLOB. Should be retrieved from a previous call of
   *        {@link #save(DataResource)}.
   * @return the {@link DataResource} of the BLOB. It is NOT guaranteed to be reusable. Depending on the implementation
   *         of {@link BlobStore} a subsequent call to {@link DataResource#openStream()} may not be able to return a
   *         fresh stream but will return the already existing stream or cause an {@link Exception}.
   * @throws ObjectNotFoundException in case no BLOB exits for the given {@link Id}.
   */
  default DataResource load(Id<DataResource> id) throws ObjectNotFoundException {

    DataResource blob = find(id);
    if (!blob.isAvailable()) {
      throw new ObjectNotFoundException(DataResource.class.getSimpleName(), id + "[" + blob.getUri() + "]");
    }
    return blob;
  }

  /**
   * Finds a BLOB as {@link DataResource} from this store. Unlike {@link #load(Id)} this method will not throw an
   * exception if the BLOB does not exist.
   *
   * @param id is the {@link Id} pointing to the requested BLOB. Should be retrieved from a previous call of
   *        {@link #save(DataResource)}.
   * @return the {@link DataResource} of the BLOB. If the identified BLOB does NOT exist, an
   *         {@link DataResource#isAvailable() unavailable} {@link DataResource} will be returned (may
   *         {@link UnavailableResource#INSTANCE} or an individual instance). The returned BLOB is NOT guaranteed to be
   *         reusable. Subsequent call to {@link DataResource#openStream()} will have unspecified behavior. Depending on
   *         the implementation of {@link BlobStore} this may return a new fresh stream, return the already used
   *         existing stream or cause an {@link Exception}.
   */
  DataResource find(Id<DataResource> id);

  /**
   * Deletes the BLOB with the given {@link Id} from this store.
   *
   * @param id the {@link Id} pointing to the BLOB to delete. Should to be retrieved from a previous call of
   *        {@link #save(DataResource)}.
   * @return {@code true} if the BLOB was successfully deleted, {@code false} if no such BLOB exists (idempotent).
   * @throws RuntimeException a further unspecified {@link RuntimeException} (e.g. {@link RuntimeIoException}) if the
   *         BLOB exists, but form some reason the deletion failed.
   */
  boolean delete(Id<DataResource> id);

  /**
   * Allows safe de-serialization of the {@link Object#toString() string representation} of an {@link Id} from this
   * {@link BlobStore}.
   *
   * @param id the {@link String} representation of an {@link Id} (initially received from {@link #save(DataResource)}).
   *        May also be {@code null}.
   * @return the given {@code id} {@link String} converted to an {@link Id} accepted by this {@link BlobStore}.
   */
  default Id<DataResource> createId(String id) {

    return StringVersionId.of(DataResource.class, id);
  }

}
