# Planning Material

These notes and board screenshots describe historical scope, proposals and
milestones, not live operational configuration or authorization to mutate nodes.
Use [cval-technical-overview.md](../cval-technical-overview.md) and
[components_design](../components_design/) for the current implementation boundary.

The initial-scope notebook/dry-run descriptions are historical. The timeout-based
uncordon idea retained in the original 3.0 notes conflicts with their fail-closed
acceptance requirement: it is not an approved policy and must not be implemented.
No current raw c-val or evaluation worker automatically cordons or uncordons nodes.