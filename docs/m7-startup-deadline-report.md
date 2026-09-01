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
and before each backoff. The provider-inventory deadline passes through the
connector interface into the RunPod HTTP connect/read allocation. Connectors
that do not yet control transport timeouts keep the compatible interface.

Worker-quiescence uses only the deadline-aware active-worker adapter. Cleanup
uses only deadline-aware provider inventory for its initial read and its proof
reads. Cleanup always sends one termination request for every exact known paid
resource. The deadline limits all later termination attempts, waits, and absence
proof. The bounded cleanup receipt is also the final audit, so the campaign does
not start a second unbounded provider read after cleanup.

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
The tests also use the real Coordinator and RunPod connector paths with a fake
HTTP transport. They prove that the transport timeout is not larger than the
positive remaining deadline. These deterministic tests use no provider or
credential service.
