import { Info, dependency, EdgeInfo, ProcessedSchema, ParsedSchema, QueryInfo } from "./interfaces"
import { DBType, Field, AssocEdge, InverseAssocEdge } from "@snowtop/ent/schema";
import { getValue } from "./value"
import { snakeCase } from "snake-case";
import { Data } from "@snowtop/ent";
import { inspect } from "util"

export function getDbColFromName(name: string): string {
  return snakeCase(name).toLowerCase();
}

export function getDbCol(field: Field): string {
  return field.storageKey || getDbColFromName(field.name);
}

function getRow(fields: Field[], infos: Map<string, Info>, partial?: {}, derivedIDType?: string): Data {
  partial = partial || {};
  const ret = {};
  for (const field of fields) {
    const col = getDbCol(field);

    if (partial[col] !== undefined) {
      ret[col] = partial[col];
    } else {
      ret[col] = getValue(field, col, infos);
    }

    if (field.derivedFields) {
      let type: string;
      if (field.name.endsWith("_id")) {
        let idx = field.name.indexOf("_id");
        type = field.name.substring(0, idx) + "_type";

      } else if (field.name.endsWith("ID")) {
        let idx = field.name.indexOf("ID");
        type = field.name.substring(0, idx) + "Type";
      } else {
        throw new Error(`unsupported field ${field.name} with derived fields`)
      }
      for (const f2 of field.derivedFields) {
        if (f2.name !== type) {
          throw new Error(`unsupported derived field with name ${f2.name}`)
        }
        if (!derivedIDType) {
          //          console.log(ret, partial)
          //          console.trace()

          throw new Error(`cannot set field ${f2.name} without derivedIDType being passed in ${inspect(ret, undefined, 4)} ${inspect(partial, undefined, 4)}`);
        }
        ret[getDbCol(f2)] = derivedIDType;
      }
    }
  }
  return ret;
}

function getPartialRow(
  deps: Map<string, dependency[]>,
  deps2: dependency[],
  info: Info,
  infos: Map<string, Info>,
  globalRows: Map<string, Data[]>,
  i: number,
) {
  let partialRow = {};
  let derivedIDType: string | undefined;

  for (const deps3 of deps2) {
    const depSchema = deps3.schema;
    let schema: string;

    // polymorphic types.
    if (Array.isArray(depSchema)) {
      const idx = Math.floor(Math.random() * depSchema.length);
      schema = depSchema[idx];
      derivedIDType = schema;
    } else if (depSchema === "*") {
      // wildcard polymorphic

      schema = findStarSchema(infos);
      derivedIDType = schema;

    } else {
      // common case.
      schema = depSchema;
    }

    const row = getRowFor(infos, deps, globalRows, schema, i, derivedIDType);
    const val = row[deps3.inverseCol];
    if (val === undefined) {
      throw new Error(`got undefined for col ${deps3.inverseCol} in row at index ${i} in table ${info.tableName}`);
    }
    partialRow[deps3.col] = val;
  }
  return { partialRow, derivedIDType };
}

function findStarSchema(infos: Map<string, Info>) {
  while (true) {
    const keys = Array.from(infos.keys());
    const idx = Math.floor(Math.random() * keys.length);
    let schema = keys[idx]
    const info = infos.get(schema);
    if (!info) {
      throw new Error(`couldn't find info for schema ${schema}`)
    }
    // found schema...
    if (info.cols.find((v) => v == "id") !== undefined) {
      //                console.log('found schema', schema)
      return schema;
    }
  }
}

function getRowFor(
  infos: Map<string, Info>,
  deps: Map<string, dependency[]>,
  globalRows: Map<string, Data[]>,
  schema: string,
  i: number,
  derivedIDType?: string,
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
  //  const newRow = getRow(info.schema.fields, infos, undefined, derivedIDType);
  // this needs to be aware of dependencies...
  const newRows = generateBulkRowsImpl(schema, deps, 1, infos, info, globalRows, [], true, derivedIDType)
  return newRows[0];
}


interface generateRowsOptions {
  schema: string;
  rowCount: number;
  globalRows: Map<string, Data[]>,
  summaries: string[],
  disableSummary?: boolean,
  derivedIDType?: string,
  info: Info,
}

export function generateBulkRows(
  parsedSchema: ParsedSchema,
  opts: generateRowsOptions,
) {
  return generateBulkRowsImpl(
    opts.schema,
    parsedSchema.deps,
    opts.rowCount,
    parsedSchema.infos,
    opts.info,
    opts.globalRows,
    opts.summaries,
    opts.disableSummary,
    opts.derivedIDType,
  )
}

function generateBulkRowsImpl(
  key: string,
  deps: Map<string, dependency[]>,
  rowCount: number,
  infos: Map<string, Info>,
  info: Info,
  globalRows: Map<string, Data[]>,
  summaries: string[],
  disableSummary?: boolean,
  derivedIDType?: string,
) {
  const fields = info.schema.fields;
  let deps2 = deps.get(key);
  let rows: Data[] = [];

  // no dependencies, simple...
  if (!deps2) {
    // no dep
    for (let i = 0; i < rowCount; i++) {
      const row = getRow(fields, infos, undefined, derivedIDType);
      rows.push(row);
    }
    if (!disableSummary) {
      summaries.push(`${rowCount} rows created in table ${info.tableName}`);
    }
  } else {
    // dependencies

    const unique = deps2.some(dep => dep.unique);
    // has a unique field so just create a new one every time
    if (unique) {
      for (let i = 0; i < rowCount; i++) {
        const { partialRow, derivedIDType } = getPartialRow(deps, deps2, info, infos, globalRows, i);

        const row = getRow(fields, infos, partialRow, derivedIDType);
        rows.push(row);
      }
      if (!disableSummary) {
        summaries.push(`${rowCount} rows created in table ${info.tableName}`);
      }
    } else {

      let start = rowCount;
      let i = -1;
      do {
        start = Math.ceil(start / 2);
        i++;

        const { partialRow, derivedIDType } = getPartialRow(deps, deps2, info, infos, globalRows, i);

        for (let j = 0; j < start; j++) {
          const row = getRow(fields, infos, partialRow, derivedIDType);
          rows.push(row);
        }
        if (!disableSummary) {
          let summary = `${start} rows created in table ${info.tableName} with commonality: ${inspect(partialRow, undefined, 2)}`;
          if (derivedIDType) {
            summary += ` of polymorphic type ${derivedIDType}`;
          }
          summaries.push(summary);
        }
      } while (start > 1);
    }
  }
  const existingRows = globalRows.get(info.tableName) || [];
  if (existingRows.length < rows.length) {
    rows.push(...existingRows);
    globalRows.set(info.tableName, rows)
  } else {
    existingRows.push(...rows);
    globalRows.set(info.tableName, existingRows)
  }
  return rows;
}