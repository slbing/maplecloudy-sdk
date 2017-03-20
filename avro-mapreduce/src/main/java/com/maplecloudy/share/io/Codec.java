package com.maplecloudy.share.io;

import java.io.IOException;
import java.nio.ByteBuffer;

/** 
 * Interface for Avro-supported compression codecs for data files.
 *
 * This is currently exclusively an internal-facing API.
 */
public abstract class Codec {
  /** Name of the codec; written to the file's metadata. */
  abstract String getName();
  /** Compresses the input data */
  public abstract ByteBuffer compress(ByteBuffer uncompressedData) throws IOException;
  /** Decompress the data  */
  public abstract ByteBuffer decompress(ByteBuffer compressedData) throws IOException;
  /** 
   * Codecs must implement an equals() method.  Two codecs, A and B are equal
   * if: the result of A and B decompressing content compressed by A is the same
   * AND the retult of A and B decompressing content compressed by B is the same
   **/
  @Override
  public abstract boolean equals(Object other);
  /** 
   * Codecs must implement a hashCode() method that is consistent with equals().*/
  @Override
  public abstract int hashCode();
  @Override
  public String toString() {
    return getName();
  }
}
