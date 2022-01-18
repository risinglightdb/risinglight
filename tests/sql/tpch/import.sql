COPY CUSTOMER FROM 'target/tpch-dbgen/tbl/customer.tbl' ( DELIMITER '|' );
COPY NATION FROM 'target/tpch-dbgen/tbl/nation.tbl' ( DELIMITER '|' );
COPY ORDERS FROM 'target/tpch-dbgen/tbl/orders.tbl' ( DELIMITER '|' );
COPY PART FROM 'target/tpch-dbgen/tbl/part.tbl' ( DELIMITER '|' );
COPY PARTSUPP FROM 'target/tpch-dbgen/tbl/partsupp.tbl' ( DELIMITER '|' );
COPY REGION FROM 'target/tpch-dbgen/tbl/region.tbl' ( DELIMITER '|' );
COPY SUPPLIER FROM 'target/tpch-dbgen/tbl/supplier.tbl' ( DELIMITER '|' );
COPY LINEITEM FROM 'target/tpch-dbgen/tbl/lineitem.tbl' ( DELIMITER '|' );
