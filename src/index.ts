import minimist from "minimist";
import { execSync } from "child_process"
import { DBType, Field, AssocEdge, InverseAssocEdge } from "@snowtop/ent/schema";
import { pascalCase } from "pascal-case"
import { Data } from "@snowtop/ent";
import * as fs from "fs"
import * as path from "path"
import { writeToStream } from '@fast-csv/format';
import pg from "pg"
import pluralize from "pluralize"
import Graph from "graph-data-structure";
import { inspect } from "util"
import { snakeCase } from "snake-case";
import { Info, dependency, EdgeInfo, ProcessedSchema, ParsedSchema, QueryInfo } from "./interfaces"
import { getDbColFromName, getDbCol, generateBulkRows } from "./row";

const scriptPath = "./node_modules/@snowtop/ent/scripts/read_schema";

// default is 10000 if doing all the objects...
// recommended to pass rowCount and restrict if we want a large number e.g. 100,000+
const RowCount = 10000;

let restrict: Map<string, boolean> | undefined;
let summaries: string[] = [];

function findTsConfigJSONFile(dirPath: string): string | undefined {
  while (dirPath != '/') {
    const check = path.join(dirPath, 'tsconfig.json');
    if (fs.existsSync(check)) {
      return check;
    }
    dirPath = path.join(dirPath, '..');
  }

  return undefined;
}

async function main() {
  const options = minimist(process.argv.slice(2));

  //  console.log(options)
  if (!options.path) {
    throw new Error("path required");
  }

  if (!options.connString) {
    throw new Error("connection string required")
  }

  const rowCount = options.rowCount ? parseInt(options.rowCount, 10) : RowCount;

  if (options.restrict) {
    restrict = new Map();
    let strs: string[] = options.restrict.split(",");
    for (const str of strs) {
      restrict.set(str, true);
    }
  }

  const dir = ensureDir();
  const parsedSchema = parseSchema(options.path, dir);

  const client = new pg.Client(options.connString);

  try {
    await client.connect();

    let globalRows: Map<string, Data[]>;
    let edgeQueryInfo: QueryInfo | undefined;
    if (options.edgeName) {

      const ret = await generateEdges(parsedSchema, options.edgeName, client, rowCount);
      globalRows = ret.globalRows;
      edgeQueryInfo = ret.queryInfo;
    } else {
      globalRows = await generateRows(parsedSchema, rowCount);
    }

    await writeFiles(parsedSchema, globalRows, edgeQueryInfo)

    await writeQueries(parsedSchema, globalRows, client, edgeQueryInfo);

  } catch (err) {
    console.error("err: ", err);
  } finally {
    await client.end();
  }

  // TODO flag to disable this
  cleanup();

  console.log("SUMMARY:")
  console.log(summaries.join("\n"))
}

function ensureDir() {
  const dir = path.join(process.cwd(), `/data/inserts`);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true })
  }
  return dir;
}

function cleanup() {
  let dir = path.join(process.cwd(), `/data`);
  fs.rmSync(dir, { force: true, recursive: true })
}

function parseSchema(schemaPath: string, dir: string): ParsedSchema {
  const parts: any[] = ['ts-node'];
  let tsconfigPath = findTsConfigJSONFile(schemaPath);
  if (tsconfigPath !== undefined) {
    parts.push("--project", tsconfigPath, "-r", "./node_modules/tsconfig-paths/register")
  }
  parts.push(scriptPath, '--path', schemaPath);
  //  parts.push()
  const result = execSync(parts.join(" "));
  const nodes = JSON.parse(result.toString());

  const infos: Map<string, Info> = new Map();

  let graph = Graph();
  let deps: Map<string, dependency[]> = new Map();
  const allEdges: Map<string, EdgeInfo> = new Map();

  // parse and gather data step
  for (const key in nodes) {
    graph.addNode(key)
    let generate = true;
    if (restrict && !restrict.has(key)) {
      generate = false;
    }

    const tableName = pluralize(snakeCase(key))
    const filePath = path.join(dir, `${tableName}.csv`)
    const obj = nodes[key] as ProcessedSchema;
    const fields = obj.fields as Field[];
    const cols: string[] = [];

    // we have rows we want added by default
    // for now, let's just do nothing and assume that the ent framework
    // has correctly handled this
    // most use cases is enums but could be other things too...
    if (obj.dbRows) {
      generate = false;
    }

    for (const f of fields) {
      const col = getDbCol(f);
      cols.push(col)
      if (f.foreignKey != null) {
        graph.addEdge(f.foreignKey.schema, key)

        if (f.type.dbType === DBType.UUID) {
          let deps2 = deps.get(key) || [];
          deps2.push({
            schema: f.foreignKey.schema,
            col,
            inverseCol: getDbColFromName(f.foreignKey.column),
            unique: f.unique,
          });
          deps.set(key, deps2)
        }
      }

      if (f.polymorphic) {
        //        console.log('polymorphic', typeof f.polymorphic)
        // just polymorphic so any field goes here...
        let schema: string | string[] | undefined;
        if (typeof f.polymorphic === "boolean") {
          //          console.log("schema *")
          schema = "*";
        } else if (f.polymorphic.types) {
          //          console.log(f.polymorphic)
          schema = [];
          for (const typ of f.polymorphic.types) {
            // convert nodeType to Schema name e.g. user -> User, address -> Address
            const pascalTyp = pascalCase(typ);
            schema.push(pascalTyp)
            graph.addEdge(pascalTyp, key);
          }
        } else {
          // this is converted to {} by read_schema for go...
          schema = "*";

          //          console.log(f.polymorphic, "polymorphic")
        }

        if (schema) {
          let deps2 = deps.get(key) || [];
          deps2.push({
            schema,
            col,
            inverseCol: "id",
            unique: f.unique,
          });
          deps.set(key, deps2);
          //          console.log(key, deps2)
        }
      }

      // add derived fields
      // we don't go super nested because doesn't happen yet
      if (f.derivedFields) {
        for (const f2 of f.derivedFields) {
          const col2 = getDbCol(f2);
          cols.push(col2);
        }
      }
    }

    function processEdges(key: string, edges: AssocEdge[]) {
      for (const edge of edges) {
        //        console.log(key, edge.schemaName, edge.name);

        const name = getEdgeName(key, edge)
        let inverseEdge: string | undefined;
        if (edge.inverseEdge) {
          inverseEdge = getInverseEdgeName(edge, edge.inverseEdge)
          allEdges.set(inverseEdge, {
            edgeName: inverseEdge,
            symmetric: false,
            id1Type: edge.schemaName,
            id2Type: key,
            inverseEdge: name,
          })
        }
        allEdges.set(name, {
          edgeName: name,
          symmetric: edge.symmetric || false,
          id1Type: key,
          id2Type: edge.schemaName,
          inverseEdge: inverseEdge,
        })
      }
    }
    if (obj.assocEdges) {
      processEdges
      for (const edge of obj.assocEdges) {
        processEdges(key, obj.assocEdges)
      }
    }

    if (obj.assocEdgeGroups) {
      for (const group of obj.assocEdgeGroups) {
        processEdges(key, group.assocEdges)
      }
    }

    infos.set(key, {
      name: key,
      schema: obj,
      tableName,
      path: filePath,
      cols,
      generate,
    })
  }

  return { graph, allEdges, infos, deps, rootDir: dir }
}

async function generateRows(parsedSchema: ParsedSchema, rowCount: number): Promise<Map<string, Data[]>> {
  const { graph, infos, deps } = parsedSchema;
  const order: string[] = graph.topologicalSort(graph.nodes());
  // console.log(order)
  // console.log(deps)

  let globalRows: Map<string, Data[]> = new Map();

  // prepare data step...
  // do in the right order
  // so dependencies would have already been created
  for (const key of order) {

    const info = infos.get(key);
    if (!info) {
      throw new Error(`couldn't get info for ${key}`)
    }
    if (!info.generate) {
      continue;
    }

    generateBulkRows(
      parsedSchema,
      {
        schema: key,
        rowCount,
        globalRows,
        info,
        summaries,
      }
    )
  }

  return globalRows;
}

async function generateEdges(
  parsedSchema: ParsedSchema,
  edgeName: string,
  client: pg.Client,
  rowCount: number,
) {
  const { allEdges, infos, deps } = parsedSchema;
  const edgeInfo = allEdges.get(edgeName);
  if (!edgeInfo) {
    throw new Error(`couldn't load edge info for ${edgeName}`);
  }


  const r = await client.query('SELECT * FROM assoc_edge_config where edge_name = $1', [edgeInfo.edgeName]);
  if (r.rowCount !== 1) {
    throw new Error(`couldn't load data for edge ${edgeInfo.edgeName}`)
  }
  const row = r.rows[0];
  if (row.symmetric_edge != edgeInfo.symmetric) {
    throw new Error(`row and edgeInfo don't match. row: ${inspect(row, undefined, 2)} edgeInfo: ${inspect(edgeInfo, undefined, 2)}`);
  }
  if (row.inverse_edge_type && !edgeInfo.inverseEdge) {
    throw new Error(`row and edgeInfo don't match. row: ${inspect(row, undefined, 2)} edgeInfo: ${inspect(edgeInfo, undefined, 2)}`)
  }


  const id1Type = edgeInfo.id1Type;
  const id2Type = edgeInfo.id2Type;

  const edgeRows: Data[] = [];

  const date = new Date().toISOString();

  // separate map is used for id1s (which is smaller) so that we use a 
  // fixed number of ids for the id2s and then just create the
  // mapping as needed
  //  const globalRowsID1s = new Map<string, Data[]>();

  const id1Info = infos.get(id1Type);
  if (!id1Info) {
    throw new Error(`couldn't get info for ${id1Type}`);
  }
  const id2Info = infos.get(id2Type);
  if (!id2Info) {
    throw new Error(`couldn't get info for ${id2Type}`);
  }
  const globalRowsID1s = new Map<string, Data[]>();
  const globalRows = new Map<string, Data[]>();

  // pregenerate id2s 
  const obj2s = generateBulkRows(
    parsedSchema,
    {
      schema: id2Type,
      rowCount: Math.ceil(rowCount / 2),
      info: id2Info,
      disableSummary: true,
      globalRows,
      summaries,
    }
  )

  let start = rowCount;
  let i = -1;

  do {
    start = Math.ceil(start / 2);
    i++;

    // generate new "bulk" of 1
    // could have generated this above but can't quickly get Math.log to work.
    //  Math.ceil(Math.log2(rowCount)),
    // so we just generate "bulk" of 1 each time.
    const obj1s = generateBulkRows(
      parsedSchema,
      {
        schema: id1Type,
        info: id1Info,
        rowCount: 1,
        globalRows: globalRowsID1s,
        disableSummary: true,
        summaries,
      }
    )
    const obj1 = obj1s[0]


    for (let j = 0; j < start; j++) {
      const obj2 = obj2s[j];
      if (!obj2) {
        throw new Error(`couldn't load obj2 at index ${j}`)
      }

      edgeRows.push({
        id1: obj1.id,
        id1_type: id1Type,
        edge_type: row.edge_type,
        i2: obj2.id,
        id2_type: id2Type,
        time: date,
        data: null,
      })
      if (edgeInfo.symmetric) {
        edgeRows.push({
          id1: obj2.id,
          id1_type: id2Type,
          edge_type: row.edge_type,
          i2: obj1.id,
          id2_type: id1Type,
          time: date,
          data: null,
        })
      }
      if (edgeInfo.inverseEdge) {
        edgeRows.push({
          id1: obj2.id,
          id1_type: id2Type,
          edge_type: row.inverse_edge_type,
          i2: obj1.id,
          id2_type: id1Type,
          time: date,
          data: null,
        })
      }
    }

    summaries.push(
      `${start}` +
      `${edgeInfo.symmetric ? " symmetric" : ""}` +
      `${edgeInfo.inverseEdge ? " inverse" : ""}` +
      ` edges created from id1 ${obj1.id} with edge_type: ${row.edge_type}`)
  } while (start > 1);

  // move the objects that were in id1 map into main map
  for (const [key, dupRows] of globalRowsID1s) {
    const rows = globalRows.get(key) || [];
    rows.push(...dupRows);
    globalRows.set(key, rows);
  }

  // add edges to globalRows
  globalRows.set(row.edge_table, edgeRows);

  //  console.log(globalRows)
  return {
    globalRows, queryInfo: {
      tableName: row.edge_table,
      path: path.join(parsedSchema.rootDir, `${row.edge_table}.csv`),
      cols: ["id1", "id1_type", "edge_type", "id2", "id2_type", "time", "data"],
    }
  };
}

async function writeFiles(
  parsedSchema: ParsedSchema,
  globalRows: Map<string, Data[]>,
  edgeQueryInfo?: QueryInfo,
) {

  const newPromise = (filePath: string, rows: Data[]) => {
    const writeStream = fs.createWriteStream(filePath);

    return new Promise((resolve, reject) => {
      writeToStream(writeStream, rows, {
        headers: true,
        includeEndRowDelimiter: true,
      })
        .on("error", (err) => {
          console.error(err);
          reject(err);
        })
        .on("finish", () => {
          //          console.log("done writing to ", filePath);
          resolve(true);
        });

    })
  }

  const { infos } = parsedSchema;

  let promises: Promise<any>[] = [];

  // write data to csv step
  for (const [key, info] of infos) {
    const rows = globalRows.get(info.tableName) || [];
    // if we have any rows, whether generated or not, use it
    // we may have rows because of dependencies...
    if (!rows.length) {
      continue;
    }

    promises.push(newPromise(info.path, rows));
  }

  if (edgeQueryInfo) {
    const rows = globalRows.get(edgeQueryInfo.tableName);
    if (rows) {

      promises.push(newPromise(edgeQueryInfo.path, rows));
    }
  }

  await Promise.all(promises)
}

async function writeQueries(
  parsedSchema: ParsedSchema,
  globalRows: Map<string, Data[]>,
  client: pg.Client,
  edgeQueryInfo?: QueryInfo) {
  const { infos, graph } = parsedSchema;
  const order = graph.topologicalSort(graph.nodes());

  try {
    await client.query('BEGIN')
    for (const o of order) {
      const info = infos.get(o);

      if (!info) {
        throw new Error(`couldn't get info for schema: ${o}`);
      }
      const rows = globalRows.get(info.tableName) || [];
      if (!rows.length) {
        continue;
      }

      //      console.log(generateQuery(info))
      await client.query(generateQuery(info))
    }

    if (edgeQueryInfo) {
      const edgeQuery = generateQuery(edgeQueryInfo)
      //      console.log(edgeQuery);
      await client.query(edgeQuery);
    }
    await client.query('COMMIT')

  } catch (err) {
    await client.query("ROLLBACK");
    console.error('err: ', err)
  }
}

function generateQuery(info: QueryInfo): string {
  return `COPY ${info.tableName}(${info.cols.join(",")}) FROM '${info.path}' CSV HEADER;`;
}

function getEdgeName(sourceNode: string, assocEdge: AssocEdge): string {
  const prefix = pascalCase(sourceNode) + "To";
  const suffix = pascalCase(assocEdge.name) + "Edge";
  if (pascalCase(assocEdge.name).indexOf(prefix) === 0) {
    return suffix;
  }
  return prefix + suffix;
}

function getInverseEdgeName(assocEdge: AssocEdge, inverseEdge: InverseAssocEdge): string {
  const prefix = pascalCase(assocEdge.schemaName) + "To";
  const suffix = pascalCase(inverseEdge.name) + "Edge";
  // already starts with UserTo or something along those lines
  if (pascalCase(inverseEdge.name).indexOf(prefix) === 0) {
    return suffix;
  }
  return prefix + suffix;
}

Promise.resolve(main());
