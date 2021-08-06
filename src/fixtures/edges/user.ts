import {
  Schema,
  Field,
  BaseEntSchema,
  Edge,
  StringType,
} from "@snowtop/ent/schema";

export default class User extends BaseEntSchema implements Schema {
  fields: Field[] = [
    StringType({ name: "FirstName" }),
    StringType({ name: "LastName" }),
  ];

  edges: Edge[] = [
    {
      schemaName: "User",
      name: "friends",
      symmetric: true,
    },
    {
      schemaName: "User",
      name: "followers",
      inverseEdge: {
        name: "followees",
      },
    },
  ];
}