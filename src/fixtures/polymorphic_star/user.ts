import {
  Schema,
  Field,
  BaseEntSchema,
  StringType,
} from "@snowtop/ent/schema";

export default class User extends BaseEntSchema implements Schema {
  fields: Field[] = [
    StringType({ name: "FirstName" }),
    StringType({ name: "LastName" }),
  ];
}