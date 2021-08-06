import {
  Schema,
  BaseEntSchema,
  Field,
  StringType,
  UUIDType,
} from "@snowtop/ent/schema";

export default class EventAddress extends BaseEntSchema implements Schema {
  fields: Field[] = [
    StringType({ name: "Street" }),
    StringType({ name: "City" }),
    StringType({ name: "State" }),
    StringType({ name: "ZipCode" }),
    StringType({ name: "Apartment", nullable: true }),
    UUIDType({
      name: "OwnerID",
      foreignKey: { schema: "Event", column: "ID" },
    }),
  ];
}
