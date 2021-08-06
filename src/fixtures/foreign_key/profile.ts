import {
  Schema,
  Field,
  BaseEntSchema,
  StringType,
} from "@snowtop/ent/schema";

export default class Profile extends BaseEntSchema implements Schema {
  fields: Field[] = [
    StringType({ name: "Name" }),
  ];
}