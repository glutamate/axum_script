((globalThis) => {
  const core = Deno.core;

  globalThis.createCache = Deno.core.ops.op_create_cache;
  globalThis.flushCache = Deno.core.ops.op_flush_cache;
  globalThis.getCache = (subset) =>
    typeof subset === "function"
      ? Deno.core.ops.op_with_cache(subset)
      : subset
      ? Deno.core.ops.op_get_cache_subset_value(subset)
      : Deno.core.ops.op_get_cache_value();
})(globalThis);
