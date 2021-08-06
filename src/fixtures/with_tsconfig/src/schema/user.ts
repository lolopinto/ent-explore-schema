import {
  Schema,
  Field,
  BaseEntSchema,
  StringType,
} from "@snowtop/ent/schema";
import { Enum } from "src/enum";

export default class User extends BaseEntSchema implements Schema {
  fields: Field[] = [
    StringType({ name: "FirstName" }),
    StringType({ name: "LastName" }),
  ];
}