//import { op_route } from "ext:core/ops";

console.log("core");

route("/foo", async () => {
  const n = await query("select 1 as mynum");
  console.log(n);
  return "hello from the function foo";
});

route("/", async () => {
  return "hello from the function in  main";
});
