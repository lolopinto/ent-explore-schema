import {
  Schema,
  Field,
  BaseEntSchema,
  StringType,
  EnumType,
} from "@lolopinto/ent/schema";

export default class User extends BaseEntSchema implements Schema {
  fields: Field[] = [
    StringType({ name: "FirstName" }),
    StringType({ name: "LastName" }),
    EnumType({
      name: "status",
      values: ["UNVERIFIED", "VERIFIED", "DEACTIVATED", "DISABLED"],
      createEnumType: true,
      tsType: "UserStatus",
      graphQLType: "UserStatus",
    }),
  ];
}