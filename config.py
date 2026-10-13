"""Shared configuration for the multi-tenant-streaming PoC."""

BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPIC = "multi-tenant.ops.events"
NUM_PARTITIONS = 6

# Three tenants. `acme` is the noisy neighbor: most of the volume AND slow to process.
TENANTS = {
    "acme":    {"share": 0.70, "slow": True},
    "globex":  {"share": 0.20, "slow": False},
    "initech": {"share": 0.10, "slow": False},
}

EVENT_TYPES = [
    "ticket.created",
    "ticket.updated",
    "conversation.message_added",
    "issue.status_changed",
]

# Simulated downstream cost per event.
FAST_PROCESS_MS = 5      # knowledge-graph update path
SLOW_PROCESS_MS = 150    # slow downstream (e.g. webhook delivery with retries)

STAGED_WORKERS = 4
TENANT_QUEUE_MAX = 500
