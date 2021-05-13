import { Schema, AssocEdge, AssocEdgeGroup } from "@lolopinto/ent/schema";

export interface Info {
  name: string;
  schema: Schema;
  path: string;
  cols: string[];
  tableName: string;
  generate: boolean;
}

export interface dependency {
  schema: string | string[]; // * wildcard, randomly pick one...
  col: string;
  unique?: boolean;
  inverseCol: string;
}

export interface EdgeInfo {
  id1Type: string;
  id2Type: string;
  symmetric: boolean;
  edgeName: string;
  inverseEdge?: string;
}

export interface ProcessedSchema extends Schema {
  assocEdges: AssocEdge[];
  assocEdgeGroups: AssocEdgeGroup[];
}

export interface ParsedSchema {
  infos: Map<string, Info>;
  allEdges: Map<string, EdgeInfo>;
  graph: any; // result of Graph()
  deps: Map<string, dependency[]>;
  rootDir: string;
}

export interface QueryInfo {
  tableName: string;
  cols: string[];
  path: string;
}
