import {
  Schema,
  Field,
  Edge,
  BaseEntSchema,
  StringType,
  TimestampType,
  UUIDType,
} from "@snowtop/ent/schema/";

export default class Event extends BaseEntSchema implements Schema {
  fields: Field[] = [
    StringType({ name: "name" }),
    UUIDType({
      name: "creatorID",
      foreignKey: { schema: "User", column: "ID" },
    }),
    TimestampType({ name: "start_time" }),
    TimestampType({ name: "end_time", nullable: true }),

  ];
}
