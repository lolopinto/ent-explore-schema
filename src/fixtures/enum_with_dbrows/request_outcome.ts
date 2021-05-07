import { Schema, Field, StringType } from "@lolopinto/ent/schema";

export default class RequestOutcome implements Schema {
  fields: Field[] = [
    StringType({
      name: "outcome",
      primaryKey: true,
    }),
  ];

  enumTable = true;

  dbRows = [
    {
      outcome: "CANCELLED",
    },
    {
      outcome: "COMPLETED",
    },
    {
      outcome: "FAILED",
    },
  ];
}
