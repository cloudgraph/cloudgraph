package org.cloudgraph.hbase.client;

import org.cloudgraph.core.client.Admin;

public class HBaseAdmin implements Admin {
  private org.apache.hadoop.hbase.client.Admin admin;

  @SuppressWarnings("unused")
  private HBaseAdmin() {
  }

  public HBaseAdmin(org.apache.hadoop.hbase.client.Admin admin) {
    super();
    this.admin = admin;
  }

  public org.apache.hadoop.hbase.client.Admin getAdmin() {
    return admin;
  }

}
