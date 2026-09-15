# Watermark assigner

**Status:** native whenever it can help.

The watermark assigner has no admission conditions of its own beyond one placement rule: it is
substituted only when its input is **already** a columnar producer.

If the input is still row-wise, the assigner is left on the host on purpose — substituting it there
would just insert a transpose immediately followed by another transpose back, a pure round-trip
with no work done natively in between. That's a no-op, not a real fallback: nothing about the
watermark logic itself is unsupported, and the moment an upstream operator in the same query starts
producing Arrow batches, the assigner joins the native island with it.

## Watermark expressions

`SOURCE_WATERMARK()` uses the source's existing watermark forwarding. `CURRENT_WATERMARK(rt)` in a
native Calc reads that Calc's last received watermark and returns NULL before the first one. It also
works in a pure filter, which uses the Calc runtime when a watermark context is needed. The value is
scoped to the synchronous evaluation, with the previous context restored afterwards, so it cannot leak between operators
sharing a task thread. See [temporal functions](temporal-functions.md) for contextual restrictions.
