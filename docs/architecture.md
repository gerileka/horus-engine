# Architecture

## Domain model

The domain layer uses immutable, Decimal-backed financial value objects.
`Price` represents a binary-market price, `Quantity` represents a strictly
positive number of contracts, and `NonNegativeQuantity` represents quantities
that may be zero, including fills and aggregate visible depth. `TickSize`
provides exact tick-alignment validation without float arithmetic.

Orders are immutable domain records. `OrderRequest.create` (and its validating
constructor) require a tick size to validate price alignment, but do not retain
that validation input as state.
Tracked `Order` records carry a validated internal identifier, lifecycle status,
and fill quantities. Pending, open, and rejected orders cannot have fills;
cancelled orders may be unfilled or partially filled but cannot be completely
filled; fully filled orders use the `FILLED` status.

`OrderBook` is an immutable snapshot of aggregate `OrderBookLevel` values. It
copies and normalizes input levels into price-ordered tuples rather than acting
as a mutable exchange cache. This makes every snapshot deterministic and safe
to share while retaining the exact state that was observed. Locked and crossed
books are represented, rather than rejected, because they are useful evidence
of an exchange state that downstream consumers may need to inspect or discard.
