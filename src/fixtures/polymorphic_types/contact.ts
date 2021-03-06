import {
  Schema,
  Field,
  BaseEntSchema,
  StringType,
  UUIDType,
} from "@snowtop/ent/schema";

export default class Contact extends BaseEntSchema implements Schema {
  fields: Field[] = [
    StringType({ name: "FirstName" }),
    StringType({ name: "LastName" }),
    UUIDType({ name: "userID", foreignKey: { schema: "User", column: "ID" } })
  ];
}