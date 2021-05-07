import minimist from "minimist";
import { execSync } from "child_process"
import { getValue } from "./value"
import { DBType, Field, Schema } from "@lolopinto/ent/schema";
import { snakeCase } from "snake-case";
import { Data } from "@lolopinto/ent";
import * as fs from "fs"
import * as path from "path"
import { writeToStream } from '@fast-csv/format';
import pg from "pg"
import pluralize from "pluralize"
import Graph from "graph-data-structure";

const scriptPath = "./node_modules/@lolopinto/ent/scripts/read_schema";

//const RowCount = 100000;
const RowCount = 100;

let restrict: Map<string, boolean> | undefined;

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

  if (options.restrict) {
    restrict = new Map();
    let strs: string[] = options.restrict.split(",");
    for (const str of strs) {
      restrict.set(str, true);
    }
  }

  const dir = ensureDir();
  const [infos, graph, globalRows] = await readDataAndWriteFiles(
    options.path,
    dir,
    options.rowCount ? parseInt(options.rowCount, 10) : RowCount,
  );

  const client = new pg.Client(options.connString);
  await client.connect()

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

      console.log(generateQuery(info))
      await client.query(generateQuery(info))
      await client.query('COMMIT')

    }
  } catch (err) {
    await client.query("ROLLBACK");
    console.error('err: ', err)
  } finally {
    await client.end();
  }
  // TODO flag to disable this
  cleanup();
}


function getDbColFromName(name: string): string {
  return snakeCase(name).toLowerCase();
}

function getDbCol(field: Field): string {
  return field.storageKey || getDbColFromName(field.name);
}

async function getRow(fields: Field[], partial?: {}): Promise<Data> {
  partial = partial || {};
  const ret = {};
  for (const field of fields) {
    const col = getDbCol(field);
    if (partial[col] !== undefined) {
      ret[col] = partial[col];
    } else {
      ret[col] = await getValue(field, col);
    }
  }
  return ret;
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

interface Info {
  name: string;
  schema: Schema;
  path: string;
  cols: string[];
  tableName: string;
  generate: boolean;
}

interface dependency {
  schema: string;
  col: string;
  inverseCol: string;
}


async function readDataAndWriteFiles(
  schemaPath: string,
  dir: string,
  rowCount: number,
): Promise<[Map<string, Info>, any, Map<string, Data[]>]> {
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

  // parse and gather data step
  for (const key in nodes) {
    graph.addNode(key)
    let generate = true;
    if (restrict && !restrict.has(key)) {
      generate = false;
    }

    const tableName = pluralize(snakeCase(key))
    const filePath = path.join(dir, `${tableName}.csv`)
    const obj = nodes[key] as Schema;
    const fields = obj.fields as Field[];
    const cols: string[] = []
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
          });
          deps.set(key, deps2)
        }
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

  const order = graph.topologicalSort(graph.nodes());

  // need rows to be global based on order here...
  let promises: Promise<any>[] = [];

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

    const fields = info.schema.fields as Field[];

    let deps2 = deps.get(key);
    let rows: Data[] = [];

    // for now just assume simple and no dependencies...
    // we start with user...    
    // no dependencies, nothing to do here...
    if (!deps2) {
      // no dep
      for (let i = 0; i < rowCount; i++) {
        const row = await getRow(fields);
        rows.push(row);
      }
    } else {
      // dependencies
      let start = rowCount;
      let i = -1;
      do {
        start = Math.ceil(start / 2);
        i++;
        let partialRow = {};
        for (const deps3 of deps2) {
          const row = await getRowFor(infos, globalRows, deps3.schema, i)
          const val = row[deps3.inverseCol];
          if (val === undefined) {
            console.log(row)
            throw new Error(`got undefined for col ${deps3.inverseCol} in row at index ${i} in table ${info.tableName}`);
          }
          partialRow[deps3.col] = val;
        }

        for (let j = 0; j < start; j++) {
          const row = await getRow(fields, partialRow);
          rows.push(row);
        }
      } while (start > 1);
    }

    globalRows.set(info.tableName, rows);
  }

  // write data to csv step
  for (const [key, info] of infos) {
    const rows = globalRows.get(info.tableName) || [];
    // if we have any rows, whether generated or not, use it
    // we may have rows because of dependencies...
    if (!rows.length) {
      continue;
    }

    const writeStream = fs.createWriteStream(info.path);

    promises.push(
      new Promise((resolve, reject) => {
        writeToStream(writeStream, rows, {
          headers: true,
          includeEndRowDelimiter: true,
        })
          .on("error", (err) => {
            console.error(err);
            reject(err);
          })
          .on("finish", () => {
            console.log("done writing to ", info.path);
            resolve(true);
          });

      })
    )
  }
  await Promise.all(promises)
  return [infos, graph, globalRows];
}

function generateQuery(info: Info): string {
  return `COPY ${info.tableName}(${info.cols.join(",")}) FROM '${info.path}' CSV HEADER;`;
}

async function getRowFor(
  infos: Map<string, Info>,
  globalRows: Map<string, Data[]>,
  schema: string,
  i: number,
) {
  const info = infos.get(schema);
  if (!info) {
    throw new Error(`couldn't get info for schema: ${schema}`);
  }
  const rows = globalRows.get(info.tableName) || [];
  const row = rows[i];
  if (row) {
    return row;
  }
  // dependency...
  // create a new one...
  const newRow = await getRow(info.schema.fields);
  rows.push(newRow);
  globalRows.set(info.tableName, rows)
  return newRow;
}

Promise.resolve(main());
