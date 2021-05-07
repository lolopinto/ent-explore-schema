import { Table, table, TempDB, text, timestamp, uuid } from "@lolopinto/ent/testutils/db/test_db"
import { execSync } from "child_process"
import * as path from "path"
import { DB, DenyIfEdgeDoesNotExistRule } from "@lolopinto/ent"
import { Client } from "pg"

let tdb: TempDB;

beforeAll(async () => {
  tdb = new TempDB();
  await tdb.beforeAll()
});

afterAll(async () => {
  await tdb.afterAll();
})

interface Test {
  // needs to be passed in order of how it should be created.
  // will be dropped in reverse order...
  tables: Table | Table[];
  path: string;
  doTest: (pool: Client) => Promise<void>,
  rowCount?: number,
}

async function doTest(t: Test) {
  let tables: Table[] = []
  if (Array.isArray(t.tables)) {
    tables = t.tables
  } else {
    tables = [t.tables]
  }

  try {
    await tdb.create(...tables)

    const connString = process.env.DB_CONNECTION_STRING;

    const schemaPath = path.join(__dirname, t.path);
    const parts: any[] = [
      'ts-node',
      'src/index.ts',
      '--path',
      schemaPath,
      '--connString',
      connString,
    ]
    if (t.rowCount) {
      parts.push('--rowCount', t.rowCount);
    }
    execSync(parts.join(" "))

    const client = tdb.getDBClient();
    await t.doTest(client);
  } catch (err) {
    fail(err)
  } finally {

    const names = tables.map(t => t.name).reverse()
    await tdb.drop(...names)
  }
}

function getBaseUserTable() {
  // this sadly has to be created separately from the fixtures (for now...)
  return table("users",
    uuid("id", { primaryKey: true }),
    timestamp("created_at"),
    timestamp("updated_at"),
    text("first_name"),
    text("last_name"),
  );
}

test("simple schema", async () => {
  await doTest({
    tables: getBaseUserTable(),
    path: "fixtures/simple",
    rowCount: 10,
    doTest: async (pool: Client) => {
      const r = await pool.query("SELECT count(1) from users")
      expect(r.rowCount).toBe(1)
      const row = r.rows[0];
      expect(row.count).toBe("10")
    }
  })
})

test("foreign key schema", async () => {
  // this sadly has to be created separately from the fixtures (for now...)
  const userTable = getBaseUserTable();
  const contactsTable = table("contacts",
    uuid("id", { primaryKey: true }),
    timestamp("created_at"),
    timestamp("updated_at"),
    text("first_name"),
    text("last_name"),
    uuid("user_id", { foreignKey: { table: "users", col: "id" } })
  );

  await doTest({
    tables: [userTable, contactsTable],
    path: "fixtures/foreign_key",
    rowCount: 10,
    doTest: async (pool: Client) => {
      const r = await pool.query("SELECT count(1) from users")
      expect(r.rowCount).toBe(1)
      const row = r.rows[0];
      expect(row.count).toBe("10")

      const r2 = await pool.query("SELECT count(1) from contacts")
      expect(r2.rowCount).toBe(1)
      const row2 = r.rows[0];
      expect(parseInt(row2.count, 10)).toBe(10);
    }
  })
})