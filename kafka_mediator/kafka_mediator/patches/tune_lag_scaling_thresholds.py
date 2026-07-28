import frappe

# Auto-scaling policy: when a consumer group's lag crosses max_lag_threshold,
# scale proportionally to backlog (one consumer per `messages_per_consumer`),
# capped at max_consumers (20). This patch aligns existing docs that still hold
# the old thresholds with the updated JSON defaults, leaving any deliberate
# per-consumer overrides untouched.
#
#   max_lag_threshold      1000 -> 100
#   min_lag_threshold       100 ->  20   (settings singleton also had 50)
#   messages_per_consumer   500 -> 100   (must track the threshold or scaling
#                                          never adds a consumer below 500 lag)

NEW_MAX_LAG = 100
NEW_MIN_LAG = 20
NEW_MSGS_PER = 100
OLD_MAX_LAGS = (1000,)
OLD_MIN_LAGS = (50, 100)
OLD_MSGS_PER = (500,)


def execute():
    # Kafka Settings singleton
    settings = frappe.get_single("Kafka Settings")
    changed = False
    if (settings.max_lag_threshold or 0) in OLD_MAX_LAGS:
        settings.max_lag_threshold = NEW_MAX_LAG
        changed = True
    if (settings.min_lag_threshold or 0) in OLD_MIN_LAGS:
        settings.min_lag_threshold = NEW_MIN_LAG
        changed = True
    if (settings.messages_per_consumer or 0) in OLD_MSGS_PER:
        settings.messages_per_consumer = NEW_MSGS_PER
        changed = True
    if not (settings.max_consumers or 0):
        settings.max_consumers = 20
        changed = True
    if changed:
        settings.save()

    # Per-consumer overrides still sitting on the old defaults
    for old in OLD_MAX_LAGS:
        frappe.db.set_value(
            "Kafka Consumer",
            {"max_lag_threshold": old},
            "max_lag_threshold",
            NEW_MAX_LAG,
            update_modified=False,
        )
    for old in OLD_MIN_LAGS:
        frappe.db.set_value(
            "Kafka Consumer",
            {"min_lag_threshold": old},
            "min_lag_threshold",
            NEW_MIN_LAG,
            update_modified=False,
        )

    frappe.db.commit()
