import { Table, table, TempDB, text, timestamp, uuid, assoc_edge_config_table, assoc_edge_table } from "@lolopinto/ent/testutils/db/test_db"
import { execSync } from "child_process"
import * as path from "path"
import { Client } from "pg"
import { v4 } from "uuid";

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
  edgeName?: string;
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
      parts.push('--restrict', t.restrict);
    }
    if (t.edgeName) {
      parts.push('--edgeName', t.edgeName);
    }
    const r = execSync(parts.join(" "));
    //    console.log(r.toString())

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

function getEventsTable() {
  return table("events",
    uuid("id", { primaryKey: true }),
    timestamp("created_at"),
    timestamp("updated_at"),
    text("name"),
    uuid("creator_id", { foreignKey: { table: "users", col: "id" } }),
    timestamp("start_time"),
    timestamp("end_time", { nullable: true }),
  )
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

function getUniqueTables() {
  // this sadly has to be created separately from the fixtures (for now...)

  return [getBaseUserTable(), getContactsTable(uniqueIndex('user_id'))];
}

function getForeignKeySuperNestedTables() {
  // this sadly has to be created separately from the fixtures (for now...)

  return [
    table("users",
      uuid("id", { primaryKey: true }),
      timestamp("created_at"),
      timestamp("updated_at"),
      text("first_name"),
      text("last_name"),
      text("default_profile"),
    ),
    getEventsTable(),
    table("event_addresses",
      uuid("id", { primaryKey: true }),
      timestamp("created_at"),
      timestamp("updated_at"),
      text("street"),
      text("city"),
      text("state"),
      text("zip_code"),
      text("apartment", { nullable: true }),
      uuid("owner_id", { foreignKey: { table: "events", col: "id" } }),
    ),
    table("profiles",
      uuid("id", { primaryKey: true }),
      timestamp("created_at"),
      timestamp("updated_at"),
      text("name"),
    ),
  ];
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


// this encompasses foreign keys, restrict without dependency, fkey super nested
test("foreign key super nested", async () => {
  await doTest({
    tables: getForeignKeySuperNestedTables(),
    path: "fixtures/foreign_key",
    restrict: "EventAddress",
    rowCount: 10,
    doTest: async (pool: Client) => {
      const r = await pool.query("SELECT count(1) from event_addresses")
      expect(r.rowCount).toBe(1)
      const row = r.rows[0];
      expect(parseInt(row.count, 10)).toBeGreaterThanOrEqual(10)

      const r2 = await pool.query("SELECT count(1) from events")
      expect(r2.rowCount).toBe(1)
      const row2 = r2.rows[0];
      expect(parseInt(row2.count, 10)).toBeGreaterThanOrEqual(1);

      const r3 = await pool.query("SELECT count(1) from users")
      expect(r3.rowCount).toBe(1)
      const row3 = r3.rows[0];
      expect(parseInt(row3.count, 10)).toBeGreaterThanOrEqual(1);


      const r4 = await pool.query("SELECT count(1) from profiles")
      expect(r4.rowCount).toBe(1)
      const row4 = r4.rows[0];
      expect(parseInt(row4.count, 10)).toBeGreaterThanOrEqual(1);

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

describe('edges', () => {
  const friendsEdge = v4();
  const followersEdge = v4();
  const followeesEdge = v4();
  const eventToHostsEdge = v4();
  const userToHostedEvents = v4();
  const createEdges = async (client: Client) => {
    const date = new Date();
    const edges = [
      {
        edge_type: friendsEdge,
        edge_name: "UserToFriendsEdge",
        edge_table: "user_friends_edges",
        symmetric_edge: true,
        inverse_edge_type: null,
        created_at: date,
        updated_at: date,
      },
      {
        edge_type: followersEdge,
        edge_name: "UserToFollowersEdge",
        edge_table: "user_followers_edges",
        symmetric_edge: false,
        inverse_edge_type: followeesEdge,
        created_at: date,
        updated_at: date,
      },
      {
        edge_type: followeesEdge,
        edge_name: "UserToFolloweesEdge",
        edge_table: "user_followers_edges",
        symmetric_edge: false,
        inverse_edge_type: followersEdge,
        created_at: date,
        updated_at: date,
      },
      {
        edge_type: eventToHostsEdge,
        edge_name: "EventToHostsEdge",
        edge_table: "event_hosts_edge",
        symmetric_edge: false,
        inverse_edge_type: userToHostedEvents,
        created_at: date,
        updated_at: date,
      },
      {
        edge_type: userToHostedEvents,
        edge_name: "UserToHostedEventsEdge",
        edge_table: "event_hosts_edge",
        symmetric_edge: false,
        inverse_edge_type: eventToHostsEdge,
        created_at: date,
        updated_at: date,
      },
    ];

    let query = `INSERT INTO assoc_edge_config(edge_type, edge_name, edge_table, symmetric_edge, inverse_edge_type, created_at, updated_at) VALUES`;
    let valuesList: string[] = [];
    let idx = 1;
    let values: any[] = [];
    for (const edge of edges) {
      let pos: string[] = [];
      for (const key in edge) {
        pos.push(`$${idx}`);
        idx++;
        values.push(edge[key]);
      }
      valuesList.push(`(${pos.join(",")})`)
    }
    query += valuesList.join(",");

    //    console.log(query, values)

    await client.query(query, values)
  }

  test("symmetric edge", async () => {
    await doTest({
      tables: [
        getBaseUserTable(),
        assoc_edge_config_table(),
        assoc_edge_table("user_friends_edges"),
      ],
      path: "fixtures/edges",
      edgeName: "UserToFriendsEdge",
      rowCount: 10,
      preTest: createEdges,
      doTest: async (pool: Client) => {
        const r = await pool.query("SELECT * from user_friends_edges");
        // symmetric so by 2...
        expect(r.rowCount).toBeGreaterThanOrEqual(10 * 2);
        const id1s: string[] = [];
        const id2s: string[] = [];
        for (const r2 of r.rows) {
          expect(r2.id1_type).toBe("User")
          expect(r2.id2_type).toBe("User")
          expect(r2.edge_type).toBe(friendsEdge);
          id1s.push(r2.id1);
          id2s.push(r2.id2);
        }
        id1s.sort();
        id2s.sort();
        expect(id1s).toStrictEqual(id2s);
      }
    })
  })

  test("inverse edge", async () => {
    await doTest({
      tables: [
        getBaseUserTable(),
        assoc_edge_config_table(),
        assoc_edge_table("user_followers_edges"),
      ],
      path: "fixtures/edges",
      edgeName: "UserToFollowersEdge",
      rowCount: 10,
      preTest: createEdges,
      doTest: async (pool: Client) => {
        const r = await pool.query("SELECT * from user_followers_edges");
        // inverse edge so multiply by 2...
        expect(r.rowCount).toBeGreaterThanOrEqual(20);

        const id1Followers: string[] = [];
        const id2Followers: string[] = [];
        const id1Followees: string[] = [];
        const id2Followees: string[] = [];

        for (const r2 of r.rows) {
          expect(r2.id1_type).toBe("User")
          expect(r2.id2_type).toBe("User")
          //          if (r2.edge_t)
          if (r2.edge_type === followersEdge) {
            id1Followers.push(r2.id1);
            id2Followers.push(r2.id2);

          } else if (r2.edge_type === followeesEdge) {
            id1Followees.push(r2.id1);
            id2Followees.push(r2.id2);
          } else {
            fail(`unexpected edge type ${r2.edge_type}`)
          }
        }
        id1Followers.sort();
        id2Followers.sort();
        id1Followees.sort();
        id2Followees.sort();
        expect(id1Followees).toStrictEqual(id2Followers);
        expect(id2Followees).toStrictEqual(id1Followers);
      }
    })
  })

  test("inverse edge different types + dependency", async () => {
    await doTest({
      tables: [
        getBaseUserTable(),
        getEventsTable(),
        assoc_edge_config_table(),
        assoc_edge_table("event_hosts_edge"),
      ],
      path: "fixtures/edges",
      edgeName: "EventToHostsEdge",
      rowCount: 10,
      preTest: createEdges,
      doTest: async (pool: Client) => {
        const r = await pool.query("SELECT * from event_hosts_edge");
        // inverse edge so multiply by 2...
        expect(r.rowCount).toBeGreaterThanOrEqual(20);

        const id1EventToHosts: string[] = [];
        const id2EventToHosts: string[] = [];
        const id1UserToHostedEvents: string[] = [];
        const id2UserToHostedEvents: string[] = [];

        for (const r2 of r.rows) {
          //          if (r2.edge_t)
          if (r2.edge_type === eventToHostsEdge) {
            id1EventToHosts.push(r2.id1);
            id2EventToHosts.push(r2.id2);
            expect(r2.id1_type).toBe("Event")
            expect(r2.id2_type).toBe("User")

          } else if (r2.edge_type === userToHostedEvents) {
            id1UserToHostedEvents.push(r2.id1);
            id2UserToHostedEvents.push(r2.id2);
            expect(r2.id1_type).toBe("User")
            expect(r2.id2_type).toBe("Event")

          } else {
            fail(`unexpected edge type ${r2.edge_type}`)
          }
        }
        id1EventToHosts.sort();
        id2EventToHosts.sort();
        id1UserToHostedEvents.sort();
        id2UserToHostedEvents.sort();
        expect(id1UserToHostedEvents).toStrictEqual(id2EventToHosts);
        expect(id2UserToHostedEvents).toStrictEqual(id1EventToHosts);

      }
    })
  })
})