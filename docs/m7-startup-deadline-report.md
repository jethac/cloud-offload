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

## Safety behavior

- No preflight request occurs at or after the deadline.
- A clock jump beyond the deadline causes no retry and no sleep.
- A provider request timeout is returned without an extra backoff.
- A readiness timeout still cancels the current job.
- The active scenario time cannot overrun by one full poll or retry interval.
- Provider mutation remains behind the final deadline and quote guard.

## Verification

Fake-clock tests cover zero and negative remaining time, a short final retry,
a clock jump, request-timeout propagation, cancellation, and Windows timing
tolerance. These tests use no provider or credential service.
