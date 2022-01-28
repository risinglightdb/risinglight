COPY CUSTOMER FROM 'tpch-dbgen/tbl/customer.tbl' ( DELIMITER '|' );
COPY NATION FROM 'tpch-dbgen/tbl/nation.tbl' ( DELIMITER '|' );
COPY ORDERS FROM 'tpch-dbgen/tbl/orders.tbl' ( DELIMITER '|' );
COPY PART FROM 'tpch-dbgen/tbl/part.tbl' ( DELIMITER '|' );
COPY PARTSUPP FROM 'tpch-dbgen/tbl/partsupp.tbl' ( DELIMITER '|' );
COPY REGION FROM 'tpch-dbgen/tbl/region.tbl' ( DELIMITER '|' );
COPY SUPPLIER FROM 'tpch-dbgen/tbl/supplier.tbl' ( DELIMITER '|' );
COPY LINEITEM FROM 'tpch-dbgen/tbl/lineitem.tbl' ( DELIMITER '|' );
