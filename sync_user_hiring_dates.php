<?php
/**
 * Заполняет дату приёма активных пользователей по данным GateDB.
 *
 * Установка в cron (ежедневно в 04:15):
 * 15 4 * * * /usr/bin/php /home/bitrix/www/local/cron/sync_user_hiring_dates.php >/dev/null 2>&1
 *
 * Тестовый запуск без изменения пользователей:
 * php sync_user_hiring_dates.php --dry-run
 */

use Bitrix\Main\Application;

$_SERVER['DOCUMENT_ROOT'] = '/home/bitrix/www';
$DOCUMENT_ROOT = $_SERVER['DOCUMENT_ROOT'];

define('NO_KEEP_STATISTIC', true);
define('NOT_CHECK_PERMISSIONS', true);
define('BX_NO_ACCELERATOR_RESET', true);
define('BX_CRONTAB', true);

require_once $_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/main/include/prolog_before.php';

const HIRING_DATE_FIELD = 'UF_WEBSLON_ABSENCE_DATE_OF_HIRING';
const LOG_FILE = '/home/bitrix/www/upload/logs/sync_user_hiring_dates.log';
const LOCK_FILE = '/home/bitrix/www/upload/logs/sync_user_hiring_dates.lock';

$dryRun = in_array('--dry-run', $argv ?? [], true);

function logHiringDateMessage(string $message): void
{
    $directory = dirname(LOG_FILE);
    if (!is_dir($directory)) {
        @mkdir($directory, 0775, true);
    }

    $line = '[' . date('Y-m-d H:i:s') . '] ' . $message . PHP_EOL;
    file_put_contents(LOG_FILE, $line, FILE_APPEND | LOCK_EX);
    echo $line;
}

/**
 * Возвращает дату в формате, который принимает пользовательское поле типа «Дата».
 */
function formatHiringDate($value): ?string
{
    if ($value instanceof DateTimeInterface) {
        return $value->format('d.m.Y');
    }

    $value = trim((string)$value);
    if ($value === '') {
        return null;
    }

    try {
        return (new DateTimeImmutable($value))->format('d.m.Y');
    } catch (Exception $exception) {
        return null;
    }
}

function acquireHiringDateLock()
{
    $directory = dirname(LOCK_FILE);
    if (!is_dir($directory)) {
        @mkdir($directory, 0775, true);
    }

    $handle = fopen(LOCK_FILE, 'c');
    if ($handle === false || !flock($handle, LOCK_EX | LOCK_NB)) {
        throw new RuntimeException('Предыдущий запуск скрипта ещё не завершён.');
    }

    ftruncate($handle, 0);
    fwrite($handle, (string)getmypid());

    return $handle;
}

$lockHandle = null;

try {
    $lockHandle = acquireHiringDateLock();
    $connection = Application::getConnection('gatedb');
    $sqlHelper = $connection->getSqlHelper();

    $users = CUser::GetList(
        $by = 'ID',
        $order = 'ASC',
        [
            'ACTIVE' => 'Y',
            '!UF_1C_GUID' => false,
            HIRING_DATE_FIELD => false,
        ],
        [
            'FIELDS' => ['ID'],
            'SELECT' => ['UF_1C_GUID', HIRING_DATE_FIELD],
        ]
    );

    $processed = 0;
    $updated = 0;
    $skipped = 0;
    $errors = 0;

    while ($userRow = $users->Fetch()) {
        ++$processed;
        $userId = (int)$userRow['ID'];
        $guid = trim((string)$userRow['UF_1C_GUID']);

        if ($guid === '') {
            ++$skipped;
            continue;
        }

        $escapedGuid = $sqlHelper->forSql($guid, 100);
        $staffRow = $connection->query(
            "SELECT Staff_HiringDate FROM GateDB.dbo.Staff_1CZUP WHERE Staff_ID = '"
            . $escapedGuid
            . "'"
        )->fetch();

        $hiringDate = $staffRow ? formatHiringDate($staffRow['Staff_HiringDate'] ?? null) : null;
        if ($hiringDate === null) {
            ++$skipped;
            logHiringDateMessage('SKIP user ID=' . $userId . ': дата приёма не найдена, GUID=' . $guid);
            continue;
        }

        if ($dryRun) {
            ++$updated;
            logHiringDateMessage('DRY-RUN user ID=' . $userId . ', дата приёма=' . $hiringDate);
            continue;
        }

        $user = new CUser();
        if (!$user->Update($userId, [HIRING_DATE_FIELD => $hiringDate])) {
            ++$errors;
            logHiringDateMessage('ERROR user ID=' . $userId . ': ' . (string)$user->LAST_ERROR);
            continue;
        }

        ++$updated;
        logHiringDateMessage('UPDATED user ID=' . $userId . ', дата приёма=' . $hiringDate);
    }

    logHiringDateMessage(
        'DONE processed=' . $processed
        . ', updated=' . $updated
        . ', skipped=' . $skipped
        . ', errors=' . $errors
        . ($dryRun ? ', dry-run=Y' : '')
    );

    exit($errors > 0 ? 2 : 0);
} catch (Throwable $exception) {
    logHiringDateMessage('FATAL: ' . $exception->getMessage());
    exit(1);
} finally {
    if (is_resource($lockHandle)) {
        flock($lockHandle, LOCK_UN);
        fclose($lockHandle);
    }
}
