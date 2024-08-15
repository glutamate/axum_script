import {} from "./other.js";

await connectToDatabase("sqlite://sqlite.db");

await execute(`create table if not exists person (
   id INTEGER PRIMARY KEY,
   name TEXT NOT NULL,
   age INTEGER
);`);

sessions({
  store: "table",
  table: "sessions",
});

authentication({
  sessions: {
    store: "table",
    table: "sessions",
  },
  database_table: {
    table: "users",
    user_field: "name",
    password_field: "password",
  },
});

await createCache(async () => {
  console.log("creating cache");
  const name_rows = await query("select name from person order by id");
  const c = { akey: 1, bkey: 2, names: name_rows.map((row) => row.name) };
  console.log("new cache", c);

  return c;
});

route("/db-txt", async () => {
  const n = await query("select 1 as mynum");
  return `hello from the function foo ${n[0].mynum}`;
});

route("/db-json", async () => {
  const n = await query("select 1 as mynum");
  return { json: n };
});

route("/teapot", async () => {
  return { html: "short and stout", status: 418 };
});

route("/get-cache", async () => {
  return {
    json: {
      all: getCache(),
      akey: getCache("akey"),
      list: getCache(["akey"]),
      sum: getCache((c) => c.akey + c.bkey),
    },
  };
});

route("/baz/:id", async ({ params: { id } }) => {
  return `hello from the baz with arg ${id}`;
});

route("/insert-name/:name/:age", async ({ params: { name, age } }) => {
  await execute(`insert into person(name, age) values ($1, $2);`, [name, age]);
  await flushCache();
  return `OK`;
});

route("/get-age/:name", async ({ params: { name } }) => {
  const rows = await query(`select * from person where name=$1;`, [name]);
  if (rows.length > 0) return { json: rows[0] };
  else return { json: { error: "not found" } };
});

// to test for multithreading: autocannon -c 10 -d 5 -p 10 http://127.0.0.1:4000/sleep
route("/sleep", async () => {
  await sleep(100);
  return "hello from sleep";
});
