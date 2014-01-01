cloudgraph-source
=================

CloudGraph production source repository. This repository is under construction. Please see our beta repository at https://github.com/terrameta/cloudgraph. 

Distributed “cloud” databases allow for a new level of scalability and flexibility. These new sparse, columnar databases support a practically unlimited number of columns within a single table, and managing hundreds or even thousands of column name/qualifiers within a client application can be a significant challenge. CloudGraph provides services and infrastructure to impose the structure of your business domain model, regardless of its complexity, as a service layer over various supported big-table style “cloud” databases, such as HBase and Cassandra. Application developer’s deal with higher-level typed structures with meaning within the application domain, rather than row and column qualifiers and values, typically manipulated as un-typed Java byte arrays.
