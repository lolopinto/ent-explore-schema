import { Table, table, TempDB, text, timestamp, uuid } from "@lolopinto/ent/testutils/db/test_db"
import { execSync } from "child_process"
import * as path from "path"
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
  preTest?: (pool: Client) => Promise<void>;
  doTest: (pool: Client) => Promise<void>;
  rowCount?: number;
  restrict?: string;
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
    const client = tdb.getDBClient();

    if (t.preTest) {
      await t.preTest(client)
    }

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
    if (t.restrict) {
      parts.push('--restrict', t.restrict)
    }
    const r = execSync(parts.join(" "));
    //    console.log(r)

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

interface SchemaItem {
  name: string;
}

function uniqueIndex(col) {
  return {
    name: "",//ignore...
    generate() {
      return `UNIQUE (${col})`;
    },
  };
}
function getContactsTable(...constraints: SchemaItem[]) {
  return table("contacts",
    uuid("id", { primaryKey: true }),
    timestamp("created_at"),
    timestamp("updated_at"),
    text("first_name"),
    text("last_name"),
    uuid("user_id", { foreignKey: { table: "users", col: "id" } }),
    ...constraints,
  );
}

function getRequestOutcomeTable() {
  return table("request_outcomes", text("outcome", { primaryKey: true }))
}

function getForeignKeyTables() {
  // this sadly has to be created separately from the fixtures (for now...)

  return [getBaseUserTable(), getContactsTable()];
}

function getUniqueTables() {
  // this sadly has to be created separately from the fixtures (for now...)

  return [getBaseUserTable(), getContactsTable(uniqueIndex('user_id'))];
}


function getAddressesTable(...constraints: SchemaItem[]) {
  return table("addresses",
    uuid("id", { primaryKey: true }),
    timestamp("created_at"),
    timestamp("updated_at"),
    text("street"),
    text("city"),
    text("state"),
    text("zip_code"),
    text("apartment", { nullable: true }),
    uuid("owner_id"),
    text("owner_type"),
    ...constraints,
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
  await doTest({
    tables: getForeignKeyTables(),
    path: "fixtures/foreign_key",
    rowCount: 10,
    doTest: async (pool: Client) => {
      const r = await pool.query("SELECT count(1) from users")
      expect(r.rowCount).toBe(1)
      const row = r.rows[0];
      expect(row.count).toBe("10")

      const r2 = await pool.query("SELECT count(1) from contacts")
      expect(r2.rowCount).toBe(1)
      const row2 = r2.rows[0];
      expect(parseInt(row2.count, 10)).toBeGreaterThanOrEqual(10);
    }
  })
})

test("unique", async () => {
  await doTest({
    tables: getUniqueTables(),
    path: "fixtures/unique_field",
    rowCount: 10,
    doTest: async (pool: Client) => {
      const r = await pool.query("SELECT count(1) from users")
      expect(r.rowCount).toBe(1)
      const row = r.rows[0];
      expect(row.count).toBe("10")

      const r2 = await pool.query("SELECT count(1) from contacts")
      expect(r2.rowCount).toBe(1)
      const row2 = r2.rows[0];
      expect(parseInt(row2.count, 10)).toBeGreaterThanOrEqual(10);
    }
  })
})

test("restrict without dependency included", async () => {
  await doTest({
    tables: getForeignKeyTables(),
    path: "fixtures/foreign_key",
    rowCount: 10,
    // only do contact. we'll load some users because contacts needs it
    restrict: "Contact",
    doTest: async (pool: Client) => {
      const r = await pool.query("SELECT count(1) from users")
      expect(r.rowCount).toBe(1)
      const row = r.rows[0];
      expect(row.count).toBe("4")

      const r2 = await pool.query("SELECT count(1) from contacts")
      expect(r2.rowCount).toBe(1)
      const row2 = r2.rows[0];
      // we doing the ceiling approach so may end up with slightly more than one...
      expect(parseInt(row2.count, 10)).toBeGreaterThanOrEqual(10);
    }
  })
})

test("tsconfig", async () => {
  await doTest({
    tables: getBaseUserTable(),
    path: "fixtures/with_tsconfig/src/schema",
    rowCount: 10,
    doTest: async (pool: Client) => {
      const r = await pool.query("SELECT count(1) from users")
      expect(r.rowCount).toBe(1)
      const row = r.rows[0];
      expect(row.count).toBe("10")
    }
  })
})

test("enum table with dbrows", async () => {
  const confirm = async (pool: Client) => {
    const r = await pool.query("SELECT count(1) from request_outcomes")
    expect(r.rowCount).toBe(1)
    const row = r.rows[0];
    // expect 0 because we're not doing anything here.
    // just assume that in real life, ent framework handles this
    expect(row.count).toBe("0")
  }
  await doTest({
    tables: getRequestOutcomeTable(),
    path: "fixtures/enum_with_dbrows",
    preTest: confirm,
    doTest: confirm,
  })
})

test("enum type", async () => {
  const values = ["'UNVERIFIED'", "'VERIFIED'", "'DEACTIVATED'", "'DISABLED'"];

  // TODO support these in temp_db
  //  https://github.com/lolopinto/ent/issues/297
  const fakeEnumTable: Table = {
    name: "user_status",
    columns: [],
    drop() {
      return `DROP TYPE user_status`;
    },
    create() {
      return `CREATE TYPE user_status as ENUM(${values.join(", ")})`;
    },
  }
  const enumTypeCol = {
    name: "status",
    datatype() {
      return 'user_status'
    },
  };

  const userTable = table("users",
    uuid("id", { primaryKey: true }),
    timestamp("created_at"),
    timestamp("updated_at"),
    text("first_name"),
    text("last_name"),
    enumTypeCol,
  );

  await doTest({
    tables: [fakeEnumTable, userTable],
    path: "fixtures/with_enum_type",
    rowCount: 10,
    doTest: async (pool: Client) => {
      const r = await pool.query("SELECT count(1) from users")
      expect(r.rowCount).toBe(1)
      const row = r.rows[0];
      expect(row.count).toBe("10")
    }
  })
})

describe("polymorphic", () => {
  test("polymorphic. boolean", async () => {
    await doTest({
      tables: [getBaseUserTable(), getAddressesTable()],
      path: "fixtures/polymorphic_star",
      rowCount: 10,
      doTest: async (pool: Client) => {
        const r = await pool.query("SELECT count(1) from addresses");
        expect(r.rowCount).toBe(1);
        const row = r.rows[0];
        expect(parseInt(row.count, 10)).toBeGreaterThanOrEqual(10)
      }
    })
  })

  test("polymorphic. types", async () => {
    await doTest({
      tables: [getBaseUserTable(), getAddressesTable(), getContactsTable()],
      path: "fixtures/polymorphic_types",
      rowCount: 10,
      doTest: async (pool: Client) => {
        const r = await pool.query("SELECT * from addresses");
        expect(r.rowCount).toBeGreaterThanOrEqual(10);
        for (const r2 of r.rows) {
          expect(r2.owner_type).toMatch(/User|Contact/);
        }
      }
    })
  })

  test("polymorphic. types. unique", async () => {
    await doTest({
      tables: [getBaseUserTable(), getAddressesTable(uniqueIndex("owner_id")), getContactsTable()],
      path: "fixtures/polymorphic_types_unique",
      rowCount: 10,
      doTest: async (pool: Client) => {
        const r = await pool.query("SELECT * from addresses");
        expect(r.rowCount).toBeGreaterThanOrEqual(10);
        for (const r2 of r.rows) {
          expect(r2.owner_type).toMatch(/User|Contact/);
        }
      }
    })
  })

})