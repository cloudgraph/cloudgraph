/**
 *        CloudGraph Community Edition (CE) License
 * 
 * This is a community release of CloudGraph, a dual-license suite of
 * Service Data Object (SDO) 2.1 services designed for relational and 
 * big-table style "cloud" databases, such as HBase and others. 
 * This particular copy of the software is released under the 
 * version 2 of the GNU General Public License. CloudGraph was developed by 
 * TerraMeta Software, Inc.
 * 
 * Copyright (c) 2013, TerraMeta Software, Inc. All rights reserved.
 * 
 * General License information can be found below.
 * 
 * This distribution may include materials developed by third
 * parties. For license and attribution notices for these
 * materials, please refer to the documentation that accompanies
 * this distribution (see the "Licenses for Third-Party Components"
 * appendix) or view the online documentation at 
 * <http://cloudgraph.org/licenses/>. 
 */
package org.cloudgraph.hbase.key;

import java.nio.charset.Charset;

import org.apache.hadoop.hbase.util.Hash;

/**
 * Delegate for composite row and column key hashing.
 * 
 * @author Scott Cinnamond
 * @since 0.5.3
 */
public class Hashing {
	protected Charset charset;
	protected Hash hash;
	@SuppressWarnings("unused")
	private Hashing() {
	}
	public Hashing(Hash hash, Charset charset) {
		this.hash = hash;
		this.charset = charset;
	}

	/**
	 * Returns the integral hash for the given value based on the current hash
	 * algorithm.
	 * 
	 * @param value
	 *            the bytes value
	 * @return the integral hash for the given value based on the current hash
	 *         algorithm.
	 */
	public final int toInt(byte[] value) {
		return this.hash.hash(value);
	}

	/**
	 * Returns the integral hash, incremented by the given increment, for the
	 * given value based on the current hash algorithm.
	 * 
	 * @param value
	 *            the bytes value
	 * @param increment
	 *            the value by which to automatically increment the resulting
	 *            integral hash
	 * @return the integral hash, incremented by the given increment, for the
	 *         given value based on the current hash algorithm.
	 */
	public int toInt(byte[] value, int increment) {
		return this.hash.hash(value) + increment;
	}

	/**
	 * Returns the integral hash for the given value based on the current hash
	 * algorithm.
	 * 
	 * @param value
	 *            the bytes value
	 * @return the integral hash for the given value based on the current hash
	 *         algorithm.
	 */
	public final int toInt(String value) {
		return this.hash.hash(value.getBytes(this.charset));
	}

	/**
	 * Returns the integral hash, incremented by the given increment, for the
	 * given value based on the current hash algorithm.
	 * 
	 * @param value
	 *            the bytes value
	 * @param increment
	 *            the value by which to automatically increment the resulting
	 *            integral hash
	 * @return the integral hash, incremented by the given increment, for the
	 *         given value based on the current hash algorithm.
	 */
	public final int toInt(String value, int increment) {
		return this.hash.hash(value.getBytes(this.charset)) + increment;
	}

	/**
	 * Returns the integral hash for the given value as a string based on the
	 * current hash algorithm.
	 * 
	 * @param value
	 *            the bytes value
	 * @return the integral hash for the given value as a string based on the
	 *         current hash algorithm.
	 */
	public final String toString(byte[] value) {
		return String.valueOf(this.toInt(value));
	}

	/**
	 * Returns the integral hash, incremented by the given increment, for the
	 * given value as a string based on the current hash algorithm.
	 * 
	 * @param value
	 *            the bytes value
	 * @return the integral hash, incremented by the given increment, for the
	 *         given value as a string based on the current hash algorithm.
	 */
	public final String toString(byte[] value, int increment) {
		return String.valueOf(this.toInt(value, increment));
	}

	/**
	 * Returns the integral hash for the given value as a string based on the
	 * current hash algorithm.
	 * 
	 * @param value
	 *            the string value
	 * @return the integral hash for the given value as a string based on the
	 *         current hash algorithm.
	 */
	public final String toString(String value) {
		return String.valueOf(this.toInt(value));
	}

	/**
	 * Returns the integral hash, incremented by the given increment, for the
	 * given value as a string based on the current hash algorithm.
	 * 
	 * @param value
	 *            the string value
	 * @return the integral hash, incremented by the given increment, for the
	 *         given value as a string based on the current hash algorithm.
	 */
	public final String toString(String value, int increment) {
		return String.valueOf(this.toInt(value, increment));
	}

	/**
	 * Returns the given bytes as the string representation of an integer hash,
	 * converted to bytes based on the current character set.
	 * 
	 * @param value
	 *            the bytes value
	 * @return the given bytes as the string representation of an integer hash,
	 *         converted to bytes based on the current character set.
	 */
	public final byte[] toStringBytes(byte[] value) {
		return this.toString(value).getBytes(this.charset);
	}

	/**
	 * Returns the given bytes as the string representation of an integer hash,
	 * incremented by the given increment, converted to bytes based on the
	 * current character set.
	 * 
	 * @param value
	 *            the bytes value
	 * @return the given bytes as the string representation of an integer hash,
	 *         converted to bytes based on the current character set.
	 */
	public final byte[] toStringBytes(byte[] value, int increment) {
		return this.toString(value, increment).getBytes(this.charset);
	}

	/**
	 * Returns the given bytes as the string representation of an integer hash,
	 * converted to bytes based on the current character set.
	 * 
	 * @param value
	 *            the bytes value
	 * @return the given bytes as the string representation of an integer hash,
	 *         converted to bytes based on the current character set.
	 */
	public final byte[] toStringBytes(String value) {
		return this.toString(value).getBytes(this.charset);
	}

	/**
	 * Returns the given bytes as the string representation of an integer hash,
	 * incremented by the given increment, converted to bytes based on the
	 * current character set.
	 * 
	 * @param value
	 *            the bytes value
	 * @return the given bytes as the string representation of an integer hash,
	 *         converted to bytes based on the current character set.
	 */
	public final byte[] toStringBytes(String value, int increment) {
		return this.toString(value, increment).getBytes(this.charset);
	}

}
