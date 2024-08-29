((globalThis) => {
  const core = Deno.core;

  globalThis.query = (sql, pars = []) => Deno.core.ops.op_query(sql, pars);
  globalThis.execute = (sql, pars = []) => Deno.core.ops.op_execute(sql, pars);
  globalThis.connectToDatabase = Deno.core.ops.op_connect_db;
})(globalThis);
