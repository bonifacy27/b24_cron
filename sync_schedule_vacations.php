<?php

use Bitrix\Highloadblock\HighloadBlockTable;
use Bitrix\Main\Loader;
use Bitrix\Main\Type\Date;

if (PHP_SAPI !== 'cli') {
    http_response_code(403);
    exit;
}

const HL_SCHEDULE_ID = 91;
const HL_VACATIONS_ID = 83;
const VACATION_PLAN = 4;
const VACATION_LOCKED = 5;
const VACATION_ACTUAL = 6;
const PRESENCE_REMOTE = 2;
const PRESENCE_VACATION_PLAN = 4;
const PRESENCE_VACATION_ACTUAL = 5;
const STATUS_CORRECTION = 1;
const BATCH_SIZE = 500;

function getHlDataClass(int $id): string
{
    $block = HighloadBlockTable::getById($id)->fetch();
    if (!$block) {
        throw new RuntimeException("HL-блок {$id} не найден");
    }

    return HighloadBlockTable::compileEntity($block)->getDataClass();
}

function logLine(string $message, bool $error = false): void
{
    fwrite($error ? STDERR : STDOUT, '[' . date('c') . '] ' . $message . PHP_EOL);
}

try {
    $apply = false;
    $userId = null;
    foreach (array_slice($argv, 1) as $argument) {
        if ($argument === '--apply') {
            $apply = true;
        } elseif (preg_match('/^--user=([1-9][0-9]*)$/D', $argument, $match)
            && filter_var($match[1], FILTER_VALIDATE_INT) !== false
            && $userId === null) {
            $userId = (int)$match[1];
        } else {
            throw new InvalidArgumentException('Использование: php sync_schedule_vacations.php [--user=6165] [--apply]');
        }
    }

    // /home/bitrix/www/local/cron находится на два уровня ниже корня сайта.
    $docRoot = realpath(getenv('BITRIX_DOCUMENT_ROOT') ?: dirname(__DIR__, 2));
    $prolog = $docRoot . '/bitrix/modules/main/include/prolog_before.php';
    if ($docRoot === false || !is_file($prolog) || !chdir($docRoot)) {
        throw new RuntimeException('Проверьте BITRIX_DOCUMENT_ROOT. Пролог Bitrix недоступен.');
    }

    define('NO_KEEP_STATISTIC', true);
    define('NOT_CHECK_PERMISSIONS', true);
    define('BX_CRONTAB', true);
    $_SERVER['DOCUMENT_ROOT'] = $docRoot;
    require $prolog;

    date_default_timezone_set('Europe/Moscow');
    if (!Loader::includeModule('highloadblock')) {
        throw new RuntimeException('Модуль highloadblock недоступен');
    }

    $scheduleClass = getHlDataClass(HL_SCHEDULE_ID);
    $vacationsClass = getHlDataClass(HL_VACATIONS_ID);

    $today = new DateTimeImmutable('today');
    $lastDay = $today->modify('+45 days');
    $firstDate = new Date($today->format('Y-m-d'), 'Y-m-d');
    $lastDate = new Date($lastDay->format('Y-m-d'), 'Y-m-d');

    $scheduleFilter = ['>=UF_DATE' => $firstDate, '<=UF_DATE' => $lastDate];
    if ($userId !== null) {
        $scheduleFilter['=UF_USER'] = $userId;
    }

    $mode = $apply ? 'APPLY' : 'DRY RUN';
    logLine(sprintf('%s, период %s — %s, пользователь %s', $mode,
        $today->format('Y-m-d'), $lastDay->format('Y-m-d'), $userId ?? 'все'));

    $lastId = 0;
    $checked = $changed = $failed = 0;
    while (true) {
        $rows = $scheduleClass::getList([
            'select' => ['ID', 'UF_USER', 'UF_DATE', 'UF_PRESENCE_TYPE', 'UF_STATUS'],
            'filter' => $scheduleFilter + ['>ID' => $lastId],
            'order' => ['ID' => 'ASC'],
            'limit' => BATCH_SIZE,
        ])->fetchAll();
        if (!$rows) {
            break;
        }
        $lastId = (int)$rows[count($rows) - 1]['ID'];

        $vacations = $vacationsClass::getList([
            'select' => ['UF_EMPLOYEE', 'UF_DATE_BEGIN', 'UF_DATE_END', 'UF_STATE'],
            'filter' => [
                '@UF_EMPLOYEE' => array_values(array_unique(array_column($rows, 'UF_USER'))),
                '@UF_STATE' => [VACATION_PLAN, VACATION_LOCKED, VACATION_ACTUAL],
                '<=UF_DATE_BEGIN' => $lastDate,
                '>=UF_DATE_END' => $firstDate,
            ],
        ]);
        $vacationsByUser = [];
        while ($vacation = $vacations->fetch()) {
            $vacationsByUser[(int)$vacation['UF_EMPLOYEE']][] = [
                $vacation['UF_DATE_BEGIN']->format('Y-m-d'),
                $vacation['UF_DATE_END']->format('Y-m-d'),
                (int)$vacation['UF_STATE'],
            ];
        }

        foreach ($rows as $row) {
            ++$checked;
            $id = (int)$row['ID'];
            $employeeId = (int)$row['UF_USER'];
            $day = $row['UF_DATE']->format('Y-m-d');
            $currentPresence = (int)$row['UF_PRESENCE_TYPE'];
            $presence = null;
            foreach ($vacationsByUser[$employeeId] ?? [] as [$begin, $end, $state]) {
                if ($day < $begin || $day > $end) {
                    continue;
                }
                if ($state === VACATION_ACTUAL) {
                    $presence = PRESENCE_VACATION_ACTUAL;
                    break;
                }
                $presence = PRESENCE_VACATION_PLAN;
            }

            if ($presence === null) {
                if (!in_array($currentPresence, [PRESENCE_VACATION_PLAN, PRESENCE_VACATION_ACTUAL], true)) {
                    continue;
                }
                $presence = PRESENCE_REMOTE;
            }

            $updates = [];
            if ($currentPresence !== $presence) {
                $updates['UF_PRESENCE_TYPE'] = $presence;
            }
            if ($presence === PRESENCE_REMOTE && (int)$row['UF_STATUS'] !== STATUS_CORRECTION) {
                $updates['UF_STATUS'] = STATUS_CORRECTION;
            }
            if (!$updates) {
                continue;
            }

            if ($apply) {
                $result = $scheduleClass::update($id, $updates);
                if (!$result->isSuccess()) {
                    ++$failed;
                    logLine("Ошибка обновления ID={$id}, сотрудник={$employeeId}, дата={$day}. "
                        . implode(', ', $result->getErrorMessages()), true);
                    continue;
                }
            }

            ++$changed;
            $details = [];
            foreach ($updates as $field => $value) {
                $details[] = $field . ' ' . ($row[$field] ?? 'NULL') . ' -> ' . $value;
            }
            logLine("{$mode} ID={$id}, сотрудник={$employeeId}, дата={$day}, " . implode(', ', $details));
        }
    }

    logLine("{$mode}, проверено={$checked}, изменений={$changed}, ошибок={$failed}");
    exit($failed > 0 ? 1 : 0);
} catch (Throwable $error) {
    logLine($error->getMessage(), true);
    exit(1);
}
