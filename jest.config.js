module.exports = {
  preset: "ts-jest",
  testEnvironment: "node",
  transform: {
    "^.+\\.tsx?$": "ts-jest",
  },
  testRegex: "(/tests/.*|(\\.|/)(test|spec))\\.(tsx?)$",
  moduleFileExtensions: ["ts", "tsx", "js", "jsx", "json", "node"],
  //  projects: ["<rootDir>", "<rootDir>/examples/simple/"],
  // ignore subdirectories until I figure out how to get projects and root files/module name mapper working correctly
  testPathIgnorePatterns: [
    "<rootDir>/examples",
    "<rootDir>/packages",
    "<rootDir>/dist",
  ],
};
