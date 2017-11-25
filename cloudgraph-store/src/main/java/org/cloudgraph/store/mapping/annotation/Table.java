package org.cloudgraph.store.mapping.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.cloudgraph.store.mapping.HashAlgorithmName;
import org.atteo.classindex.IndexAnnotated;

/**
 * Runtime discoverable mapping to a physical data store table.
 * 
 * @author Scott Cinnamond
 * @since 1.0.8
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@IndexAnnotated
public @interface Table {
  /**
   * The physical data store table name.
   * 
   * @return The physical data store table name.
   */
  public String name();

  /**
   * The data column family name.
   * 
   * @return The data column family name.
   */
  public String dataColumnFamilyName() default "f1";

  /**
   * The character used to delimit composite row keys for the table
   * 
   * @return The character used to delimit composite row keys for the table
   */
  public String rowKeyFieldDelimiter() default "|";

  /**
   * The hash algorithm used for hashing any row key fields.
   * 
   * @return The hash algorithm used for hashing any row key fields.
   */
  public HashAlgorithmName hashAlgorithm() default HashAlgorithmName.NONE;

  /**
   * Whether to first check for an existing matching row key before
   * creating/inserting new rows.
   * 
   * @return Whether to first check for an existing matching row key before
   *         creating/inserting new rows.
   */
  public boolean uniqueChecks() default true;

  /**
   * Whether to create a tombstone rows when deleting a graph roots. Warning
   * setting this property to false could cause dangling references for models
   * with "one way" associations navigating to the concerned root which are
   * non-navigable from the root.
   * 
   * @return Whether to create a tombstone rows when deleting a graph roots.
   *         Warning setting this property to false could cause dangling
   *         references for models with "one way" associations navigating to the
   *         concerned root which are non-navigable from the root.
   */
  public boolean tombstoneRows() default true;

  /**
   * Whether to ignore and overwrite tombstone rows when creating new graph
   * roots.
   * 
   * @return Whether to ignore and overwrite tombstone rows when creating new
   *         graph roots.
   */
  public boolean tombstoneRowsOverwriteable() default true;

  /**
   * The path to prepend to all MAPRD-DB (M7) table names for tables found
   * within a configuration. Used for only MAPR-DB (M7) tables. To fully qualify
   * Apache HBase tables, use table namespaces.
   * 
   * @return The path to prepend to all MAPRD-DB (M7) table names for tables
   *         found within a configuration.
   */
  public String maprdbTablePathPrefix() default "";

  /**
   * Returns an array of property name value pairs delimited by the '=' char.
   * E.g. String[] = { foo=bar }
   * 
   * @return an array of property name value pairs
   */
  public String[] properties() default {};
}
