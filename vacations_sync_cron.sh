#!/usr/bin/env bash

# Cron entry:
# 44 * * * * /home/bitrix/www/local/cron/vacations_sync_cron.sh
#
# The lock file is only a rendezvous point for flock(1). Its age and presence do
# not mean that the lock is held: the kernel releases the lock when fd 9 closes.

set -u

PHP_BIN="${PHP_BIN:-/usr/bin/php}"
SCRIPT="${VACATIONS_SYNC_SCRIPT:-/home/bitrix/www/local/cron/vacations_sync_log.php}"
LOCK_FILE="${VACATIONS_SYNC_LOCK:-/tmp/vacations_sync_log.lock}"
LOG_FILE="${VACATIONS_SYNC_LOG:-/home/bitrix/www/upload/logs/vacations_sync_cron.log}"

timestamp() {
    date '+%Y-%m-%d %H:%M:%S'
}

if ! exec 9>"${LOCK_FILE}"; then
    printf '%s vacations_sync_log.php failed: cannot open lock file %s\n' \
        "$(timestamp)" "${LOCK_FILE}" >>"${LOG_FILE}"
    exit 1
fi

if ! /usr/bin/flock -n 9; then
    printf '%s vacations_sync_log.php skipped: previous run is still active\n' \
        "$(timestamp)" >>"${LOG_FILE}"
    exit 0
fi

"${PHP_BIN}" -f "${SCRIPT}" >>"${LOG_FILE}" 2>&1
status=$?

if (( status != 0 )); then
    printf '%s vacations_sync_log.php failed: PHP exit code %d\n' \
        "$(timestamp)" "${status}" >>"${LOG_FILE}"
fi

exit "${status}"
