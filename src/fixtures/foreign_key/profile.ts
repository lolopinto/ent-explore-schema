import {
  Schema,
  Field,
  BaseEntSchema,
  StringType,
} from "@lolopinto/ent/schema";

export default class Profile extends BaseEntSchema implements Schema {
  fields: Field[] = [
    StringType({ name: "Name" }),
  ];
}