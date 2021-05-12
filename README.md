# ent-explore-schema

This provides a utility for exploring an ent schema. It populates the database based on the schema. This should probably not be run in production, only in a development environment. 

It takes the following arguments:
* `path` (required): path to schema file e.g. `pathToRepo/src/schema`
* `connString` (required): postgres connection string where data should be inserted
* `restrict` (optional): csv list of node names that should be populated. If not provided, it generates for every schema found in the `path` provided. If a schema is provided which has dependencies e.g. a foreign key on a different schema, rows are created for the dependency to fulfill the database constraint.
* `edgeName` (optional): used to create edges from id1 to id2 of the given edge. The edgeName should match the edgeName in the `edge_name` column in the `assoc_edge_config` table. It should be the same as the `EdgeType.Foo` + `Edge` in the generated `src/ent/const.ts`. If the edge is symmetric or inverse, it also creates the other edge in the db.
* `rowCount` (optional): number of rows to generate. Current default is 10000. Can provide more or less to see behavior. It doesn't always provide the exact amount but something approximate especially when there are dependencies. When `edgeName` is provided, it determines the number of edges to create.
