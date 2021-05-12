import { IntegerType, FieldOptions, StringOptions, StringType, TimestampType, FloatType, BooleanType, UUIDType, TimeType, TimetzType, TimestamptzType, DateType, EnumType, Schema, Field } from "@lolopinto/ent";
import bcryptjs from "bcryptjs"
import { getValue } from "./value";
import { validate as validateUUid } from "uuid"
import { DateTime } from "luxon"

function strField(opts?: StringOptions) {
  return StringType({ name: "foo", ...opts });
}

async function testString(col: string, opts?: StringOptions) {
  const val = await getValue(strField(opts), col);
  expect(val).toBeDefined();
  expect(typeof val).toBe("string")
  return val;
}

function isUpper(l: string) {
  return l.toLowerCase() != l.toUpperCase();
}
function intField(opts?: FieldOptions) {
  return IntegerType({ name: "foo", ...opts })
}

function floatField(opts?: FieldOptions) {
  return FloatType({ name: "foo", ...opts })
}

function boolField(opts?: FieldOptions) {
  return BooleanType({ name: "foo", ...opts })
}

describe("strings", () => {
  test("string", async () => {
    await testString("foo")
  })

  test("email", async () => {
    const val = await testString("email_address", { name: "email_address" })
    expect(val.indexOf("@email.com")).toBeGreaterThan(0)
  })

  test("email at end", async () => {
    const val = await testString("primary_email", { name: "primary_email" })
    expect(val.indexOf("@email.com")).toBeGreaterThan(0)
  })

  test("phone number", async () => {
    const val = await testString("phone_number", { name: "phone_number" })
    expect(val.indexOf("+1")).toBe(0)
  })

  test("phone number at end", async () => {
    const val = await testString("secondary_phone_number", { name: "secondary_phone_number" })
    expect(val.indexOf("+1")).toBe(0)
  })

  test("phone", async () => {
    const val = await testString("phone", { name: "phone" })
    expect(val.indexOf("+1")).toBe(0)
  })

  test("password", async () => {
    // password is just a string because of speed issues since we may be generating a lot of rows
    await testString("password", { name: "password" })
  })

  test("first name", async () => {
    const val = await testString("first_name");
    expect(isUpper(val[0])).toBeTruthy()
  })

  test("last name", async () => {
    const val = await testString("last_name");
    expect(isUpper(val[0])).toBeTruthy()
  })
});

test("int", async () => {
  const val = await getValue(intField(), "col");
  expect(typeof val).toBe("number")
  expect(Number.isInteger(val)).toBe(true)
})

test("float", async () => {
  const val = await getValue(floatField(), "col");
  expect(typeof val).toBe("number")
  expect(Number.isInteger(val)).toBe(false)
})

test("bool", async () => {
  const val = await getValue(boolField(), "col");
  expect(typeof val).toBe("boolean")
})

test("uuid", async () => {
  const val = await getValue(UUIDType({ name: "id" }), "id");
  expect(typeof val).toBe("string")
  expect(validateUUid(val)).toBe(true)
})

test("time", async () => {
  const val = await getValue(TimeType({ name: "time" }), "time")
  expect(typeof val).toBe("string");
  expect(DateTime.fromSQL(val).isValid).toBe(true)
})

test("timetz", async () => {
  const val = await getValue(TimetzType({ name: "time" }), "time")
  expect(typeof val).toBe("string");
  expect(DateTime.fromSQL(val).isValid).toBe(true)
})

test("timestamp", async () => {
  const val = await getValue(TimestampType({ name: "time" }), "time")
  expect(typeof val).toBe("string");
  expect(DateTime.fromISO(val).isValid).toBe(true)
})

test("timestamptz", async () => {
  const val = await getValue(TimestamptzType({ name: "time" }), "time")
  expect(typeof val).toBe("string");
  expect(DateTime.fromISO(val).isValid).toBe(true)
})

test("date", async () => {
  const val = await getValue(DateType({ name: "date" }), "date")
  expect(typeof val).toBe("string");
  // yyyy-mm-dd
  expect(val).toMatch(/\d\d\d\d-\d\d-\d\d/)
})

test("enum", async () => {
  const values = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
  const typ = EnumType({ name: "foo", values: values })
  const val = await getValue(typ, "col");
  expect(typeof val).toBe("string");
  expect(values.indexOf(val)).toBeGreaterThanOrEqual(0)
})

describe("fkey enum", () => {

  test("valid", async () => {
    class RequestStatus implements Schema {
      fields: Field[] = [
        StringType({
          name: "status",
          primaryKey: true,
        }),
      ];

      enumTable = true;

      dbRows = [
        {
          status: "OPEN",
        },
        {
          status: "PENDING_FULFILLMENT",
        },
        {
          status: "CLOSED",
        },
      ];
    }

    const typ = EnumType({ name: "foo", foreignKey: { schema: "RequestStatus", column: "status" } })

    const m = new Map<string, {
      schema: Schema,
    }>();
    m.set("RequestStatus", {
      schema: new RequestStatus()
    })
    const val = await getValue(typ, "col", m);
    expect(typeof val).toBe("string");
    expect(["OPEN", "PENDING_FULFILLMENT", "CLOSED"].indexOf(val)).toBeGreaterThanOrEqual(0)
  });

  test("invalid", async () => {
    const typ = EnumType({ name: "foo", foreignKey: { schema: "RequestStatus", column: "status" } })


    try {
      await getValue(typ, "col");
      fail("should have thrown")
    } catch (err) {
      expect(err.message).toMatch(/infos required for enum with foreignKey/)
    }
  })

})