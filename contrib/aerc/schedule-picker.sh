#!/usr/bin/env bash
# aerc-schedule-picker — pick an X-JMAP-Send-At value via fzf.
#
# Invoked by aerc's :menu command:
#   :menu -c aerc-schedule-picker :header -f X-JMAP-Send-At
#
# aerc runs this in a popover terminal with stdout redirected to a temp
# file; whatever the picker prints becomes the trailing argument to
# `:header -f X-JMAP-Send-At <output>`, updating the compose header.
#
# Emits either the literal `send-now` sentinel (jmapqueue treats this as
# immediate delivery, same as no header at all) or a full ISO8601 timestamp
# with timezone offset (schedules delivery — jmapqueue passes to JMAP
# EmailSubmission.sendAt, server holds until the timestamp).
#
# `End of day` (17:00) and `This evening` (19:00) are same-day relative
# times; they're suppressed if the current local time is already past
# them (avoids offering a past timestamp the server would reject).
#
# For arbitrary times not in the preset list, type the ISO8601 timestamp
# directly into the header via `:header -f X-JMAP-Send-At <ISO8601>`.

set -euo pipefail

now=$(date +%s)
fmt()    { date -d "$1" -Iseconds; }               # GNU; local TZ; ISO8601
row()    { printf '%s\t%s\n' "$1" "$(fmt "$2")"; }
future() { [[ $(date -d "$1" +%s) -gt $now ]]; }

{
    printf 'Send now\tsend-now\n'
    row 'In 15 minutes'  '+15 min'
    row 'In 1 hour'      '+1 hour'
    row 'In 3 hours'     '+3 hours'
    future 'today 17:00' && row 'End of day'   'today 17:00' || true
    future 'today 19:00' && row 'This evening' 'today 19:00' || true
    row 'Tomorrow 8am'   'tomorrow 08:00'
    row 'Tomorrow 9am'   'tomorrow 09:00'
    row 'Monday 9am'     'next monday 09:00'
    row 'Friday 5pm'     'next friday 17:00'
} | fzf --delimiter=$'\t' --with-nth=1 \
        --prompt='Send at: ' --no-info --layout=reverse --height=100% \
    | cut -f2
