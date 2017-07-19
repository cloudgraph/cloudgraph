<img src="images/media/image2.png" alt="http://cloudgraph.org/images/text_logo_stack.png;jsessionid=040E7D42488825E3DBDF3B6888B3EED7" width="299" height="79" />

TerraMeta Software, Inc.

Overview
========

<span id="_Toc135028939" class="anchor"><span id="_Toc495460092" class="anchor"><span id="_Toc498843305" class="anchor"><span id="_Toc24906349" class="anchor"></span></span></span></span>

**Introduction**
================

<span id="_Toc135028943" class="anchor"></span>

CloudGraph<sup>®</sup> is a suite of Java™ data-graph wide-row mapping and ad hoc query services for big-table sparse, columnar "cloud" and other databases. It provides services and infrastructure to impose the structure of your business domain model, regardless of its size or complexity, as a data-graph oriented service layer over Apache HBase, relational database (RDB) and a growing list of others, providing data store vendor independence and a number of other important features.

**How It Works**
================

Despite often vast differences between data store physical structure, the rich metadata and configuration layers underlying CloudGraph support the integration of a wide range of vendor data stores while not sacrificing any important characteristics of the underlying product. Under HBase for instance, a Table is a large grained object which may have billions of rows and millions of columns. And rather than requiring every entity in your data model to be represented as a separate HBase table, model entities may be grouped in several ways and entity data graphs may be persisted within one row or across any number of rows and tables, then subsequently queried or "sliced" ad hoc using XPath or a generated Domain Specific Language (DSL) based on your domain model. For data stores with accessible metadata or schema, CloudGraph services leverage this underlying schema, but for schema-less data stores, the metadata layer bridges any gaps necessary to implement the query and data graph service API.

**Queries and Query Languages**. The CloudGraph service API and services are not bound to one specific query language, but to a flexible and granular abstract query model and associated federated query infrastructure, PlasmaQuery<sup>®</sup> which supports multiple source query and domain specific languages. This level of abstraction provides several query language options for data stores without a query language as well as the capability to define queries which span or federate multiple heterogeneous data stores.

**Results Data**. Results under the CloudGraph service API are mapped to business domain model specific, directed acyclic graph structures called data graphs. Data graphs provide rich domain context, change capture and 100% compile time checking against all model entities, relationships and data fields.

**Requirements**
================

**Java Runtime**

The latest [JDK or JRE version 1.7.xx or 1.8.xx](http://www.java.com/en/download/manual.jsp) for Linux, Windows, or Mac OS X must be installed in your environment; we recommend the Oracle JDK.

To check the Java version installed, run the command:

$ java -version

CloudGraph is tested with the Oracle JDKs; it may work with other JDKs such as Open JDK, but it has not been tested with them.

Once you have installed the JDK, you'll need to set the JAVA\_HOME environment variable.

### **Hadoop/HBase Environment**

CloudGraph HBase and CloudGraph MAPR-DB require a working HBase and HDFS or MAPR M7 environment in order to operate. CloudGraph supports these component versions:

| **Component** | **Source**    | **Supported Versions**   |
|---------------|---------------|--------------------------|
| **HDFS**      | Apache Hadoop | 1.0.2 through 2.7.3      |
|               | MapR          | 5.2 (with MapR-FS)       |
| **HBase**     | Apache        | 0.98.x, 1.0.x, and 1.1.x |
|               | MapR-DB (M7)  | 5.2                      |
| **Zookeeper** | Apache        | 3.4.6                    |

Note: Components versions shown in this table are those that we have tested and are confident of their suitability and compatibility. Later versions of components may work, but have not necessarily been either tested or confirmed compatible.

**Getting Started**
===================

Add the following dependencies to any Apache Maven POM files (or your build system's equivalent configuration), in order to make use of CloudGraph classes. The below dependency is for CloudGraph HBase. Other CloudGraph data store library artifacts have similar extensions.

**CloudGraph HBase**

For the CloudGraph HBase data store library use the following artefact.

&lt;dependency&gt;

&lt;groupId&gt;org.cloudgraph&lt;/groupId&gt;

&lt;artifactId&gt;cloudgraph-hbase&lt;/artifactId&gt;

&lt;version&gt;0.6.6&lt;/version&gt;

&lt;/dependency&gt;

**CloudGraph MAPRDB**

For the CloudGraph MAPRDB data store library use the following artefact.

&lt;dependency&gt;

&lt;groupId&gt;org.cloudgraph&lt;/groupId&gt;

&lt;artifactId&gt;cloudgraph-maprdb&lt;/artifactId&gt;

&lt;version&gt;0.6.6&lt;/version&gt;

&lt;/dependency&gt;

**CloudGraph RDB**

For the CloudGraph RDB data store library use the following artefact.

&lt;dependency&gt;

&lt;groupId&gt;org.cloudgraph&lt;/groupId&gt;

&lt;artifactId&gt;cloudgraph-rdb&lt;/artifactId&gt;

&lt;version&gt;0.6.6&lt;/version&gt;

&lt;/dependency&gt;

**Detailed Documentation**
==========================

| [**CloudGraph Architecture Overview**](http://terrameta.github.io/cloudgraph/arch_overview/CloudGraph-Architecture-Overview) |
|------------------------------------------------------------------------------------------------------------------------------|
| [**CloudGraph API DOCS**](http://terrameta.github.io/cloudgraph/apidocs/index.html)                                          |


