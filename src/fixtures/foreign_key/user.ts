import {
  Schema,
  Field,
  BaseEntSchema,
  StringType,
  UUIDType,
} from "@snowtop/ent/schema";

export default class User extends BaseEntSchema implements Schema {
  fields: Field[] = [
    StringType({ name: "FirstName" }),
    StringType({ name: "LastName" }),
    UUIDType({ name: "DefaultProfile", foreignKey: { schema: "Profile", column: "ID" } }),
  ];
}