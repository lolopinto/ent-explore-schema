import {
  Schema,
  Field,
  BaseEntSchema,
  StringType,
} from "@lolopinto/ent/schema";

export default class User extends BaseEntSchema implements Schema {
  fields: Field[] = [
    StringType({ name: "FirstName" }),
    StringType({ name: "LastName" }),
  ];
}