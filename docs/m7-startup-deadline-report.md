# M7 startup deadline correction

## Trigger

A zero-Pod startup probe in `US-MO-2` found no current viable offer. The
configured 300-second runner-readiness limit completed at 308.922 seconds. The
cause was a fixed 15-second offer-retry sleep near the end of the limit.

## Correction

The production preflight path now uses the earlier of its offer-retry deadline
and the benchmark absolute readiness deadline. Before each request, it checks
for positive remaining time. It passes that remaining time as the upper bound
for the HTTP timeout. It also limits each retry sleep to the positive remaining
time.

The benchmark runner uses the same rule for worker-quiescence polls, readiness
polls, scenario polls, campaign polls, and cleanup verification waits. It does
not call sleep with zero or negative time.

The runner passes the same absolute deadline into production event, snapshot,
provider-inventory, and active-worker observations. It checks time again after
each blocking call. A slow call can be recorded as an overrun, but it cannot
start another poll after the deadline.

Coordinator HTTP retries divide the positive remaining time between connect and
read timeouts. Provider-inventory and HTTP retries check time before each attempt
and before each backoff. Cleanup always sends one termination request for every
exact known paid resource; only retries, waits, and absence proof are deadline
bounded.

## Safety behavior

- No preflight request occurs at or after the deadline.
- A clock jump beyond the deadline causes no retry and no sleep.
- A provider request timeout is returned without an extra backoff.
- A readiness timeout still cancels the current job.
- The active scenario time cannot overrun by one full poll or retry interval.
- Provider mutation remains behind the final deadline and quote guard.

## Verification

Fake-clock tests cover zero and negative remaining time, a short final retry,
slow blocking calls, clock jumps, bounded connect/read allocation,
request-timeout propagation, cancellation, and mandatory first termination.
These deterministic tests use no provider or credential service.
