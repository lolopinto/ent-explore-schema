import { parsePhoneNumberFromString } from "libphonenumber-js";
import { DBType, Field, Schema, Type } from "@lolopinto/ent/schema"
import { v4 } from "uuid";
import { DateType, TimeType, TimetzType, TimestampType, TimestamptzType, } from "@lolopinto/ent/schema"
import { EmailType } from "@lolopinto/ent-email"
import { PhoneNumberType } from "@lolopinto/ent-phonenumber";
import { PasswordType } from "@lolopinto/ent-password";

function random(): string {
  return Math.random()
    .toString(16)
    .substring(2);
}

function randomEmail(domain?: string): string {
  domain = domain || "email.com";

  return `test+${random()}@${domain}`;
}

function randomPhoneNumber(): string {
  let phone = Math.random()
    .toString(10)
    .substring(2, 11);

  // always put a leading 1
  phone = "1" + phone;
  const phoneNumber = parsePhoneNumberFromString(phone, "US");
  if (!phoneNumber) {
    throw new Error(`parsePhoneNumber returned invalid phone number ${phone}`)
  }
  return phoneNumber.format("E.164");
}

function coinFlip() {
  return Math.floor(Math.random() * 10) >= 5;
}

async function specialType(typ: Type, col: string) {
  let list = m.get(typ.dbType);
  if (list?.length) {
    for (const l of list) {
      let regex: RegExp[] = [];
      if (Array.isArray(l.regex)) {
        regex = l.regex;
      } else {
        regex = [l.regex]
      }

      for (const r of regex) {
        if (r.test(col)) {
          return await l.newValue();
        }
      }
    }
  }
  return undefined;
}

interface Info {
  schema: Schema;
}

export async function getValue(f: Field, col: string, infos?: Map<string, Info>): Promise<any> {
  // half the time, return null for nullable
  if (f.nullable && coinFlip()) {
    return null;
  }

  const specialVal = await specialType(f.type, col);
  if (specialVal !== undefined) {
    return specialVal
  }

  let typ = f.type;
  switch (typ.dbType) {
    case DBType.UUID:
      return v4();
    case DBType.Boolean:
      return coinFlip();
    case DBType.Date:
      return DateType({ name: "foo" }).format(new Date());
    case DBType.Time:
      return TimeType({ name: "foo" }).format(new Date());
    case DBType.Timetz:
      return TimetzType({ name: "foo" }).format(new Date());
    case DBType.Timestamp:
      return TimestampType({ name: "foo" }).format(new Date());
    case DBType.Timestamptz:
      return TimestamptzType({ name: "foo" }).format(new Date());
    case DBType.String:
      return random();
    case DBType.Int:
      return Math.floor(Math.random() * 100000000);
    case DBType.Float:
      return Math.random() * 100000000;
    case DBType.Enum:
    case DBType.StringEnum:
      if (typ.values) {
        const idx = Math.floor(Math.random() * typ.values.length);
        return typ.values[idx]
      }
      if (f.foreignKey) {
        const schema = f.foreignKey.schema;
        const col = f.foreignKey.column;
        if (!infos) {
          throw new Error(`infos required for enum with foreignKey`)
        }
        const info = infos.get(schema)
        if (!info) {
          throw new Error(`couldn't load data for schema ${schema}`)
        }
        if (!info.schema.dbRows) {
          throw new Error(`no dbRows for schema ${schema}`)
        }
        const idx = Math.floor(Math.random() * info.schema.dbRows.length);
        return info.schema.dbRows[idx][col];
      }
      throw new Error("TODO: enum without values not currently supported");
    default:
      throw new Error(`unsupported type ${typ.dbType}`)
  }
}

interface commonType {
  dbType: DBType;
  newValue: () => any;
  regex: [RegExp] | RegExp;
}

const emailType = {
  dbType: DBType.String,
  newValue: () => {
    return EmailType({ name: "foo" }).format(randomEmail());
  },
  regex: /^email(_address)|_email$/,
};

const pdt = PhoneNumberType({ name: "foo" }).validateForRegion(false);
const phoneType = {
  dbType: DBType.String,
  newValue: () => {
    const p = randomPhoneNumber();
    if (!pdt.valid(p)) {
      throw new Error(`invalid phone number :${p}`)
    }
    return pdt.format(p);
  },
  regex: /^phone(_number)?|_phone$|_phone_number$/,
};

const passwordType = {
  dbType: DBType.String,
  newValue: () => {
    return PasswordType({ name: "foo" }).format(random());
  },
  regex: /^password/,
};

const firstNames = [
  "Daenerys",
  "Jon",
  "Arya",
  "Sansa",
  "Eddard",
  "Khal",
  "Robb",
  "Joffrey",
  "Ramsay",
  "Cersei",
  "Bolton",
  "Oberyn",
  "Jojen",
  "Petyr",
  "Brienne",
  "Ygritte",
  "Missandei",
  "Shae",
  "Sandor",
  "Theon",
  "Catelyn",
  "Gilly",
  "Samwell",
  "Jaime",
  "Stannis",
  "Tyene",
  "Obara",
  "Nymeria",
  "Elia",
  "Ellaria",
  "Myrcella",
  "Hodor",
  "Osha",
  "Meera",
  "Davos",
  "Gendry"
]

const lastNames = [
  "Stark",
  "Targaryen",
  "Lannister",
  "Drogo",
  "Baratheon",
  "Reed",
  "Martell",
  "Tyrell",
  "Clegane",
  "Baelish",
  "Greyjoy",
  "Tarly",
  "Sand",
  "Snow",
  "Bolton",
  "Frey",
  "Tarth",
  'Payne',
  "Seaworth"
]

const firstNameType = {
  dbType: DBType.String,
  newValue: () => {
    let idx = Math.floor(firstNames.length * Math.random());
    return firstNames[idx];
  },
  regex: /^first_?(name)?/,
};

const lastNameType = {
  dbType: DBType.String,
  newValue: () => {
    let idx = Math.floor(lastNames.length * Math.random());
    return lastNames[idx];
  },
  regex: /^last_?(name)?/,
};

let types: commonType[] = [
  phoneType, emailType, passwordType, firstNameType, lastNameType
];

let m: Map<DBType, commonType[]> = new Map();
for (const type of types) {
  let list = m.get(type.dbType) || [];
  list.push(type);
  m.set(type.dbType, list);
}