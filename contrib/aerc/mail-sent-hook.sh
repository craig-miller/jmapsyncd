#!/bin/sh
# aerc mail-sent hook — post-send desktop notification.
#
# Wire in ~/.config/aerc/aerc.conf:
#   [hooks]
#   mail-sent = ~/.config/aerc/mail-sent-hook.sh
#
# aerc exports:
#   AERC_ACCOUNT         account name that sent the message
#   AERC_FROM_NAME       sender display name
#   AERC_FROM_ADDRESS    sender email address
#   AERC_SUBJECT         message subject (may be empty)
#   AERC_TO              recipient addresses (comma-separated)
#
# Note: this fires when aerc's :send returns success — i.e. jmapqueue exited
# 0.  With jmapqueue, "sent" here means "spooled locally"; actual delivery
# happens when the daemon drains.  To check delivery status:
#   jmapsyncd submissions list --state pending    # still spooled
#   jmapsyncd submissions list --state scheduled  # server holding for sendAt

notify-send -a aerc \
    "Queued from $AERC_ACCOUNT" \
    "${AERC_SUBJECT:-(no subject)}"
