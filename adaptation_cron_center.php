<?php
/**
 * adaptation_cron_center.php
 * Единый cron-центр запуска бизнес-процессов по адаптации сотрудников.
 *
 * Поддерживаемые окна запуска:
 *  - ежедневно 10:35: задачи 1,2,3,4,7
 *  - ежедневно 17:30: задача 5 (3/13 р.д.)
 *  - вторник 16:30: задача 6 (ДМС)
 *
 * Запуск:
 *   php adaptation_cron_center.php
 *   php adaptation_cron_center.php --job=all
 *   php adaptation_cron_center.php --job=day_start
 *   php adaptation_cron_center.php --job=plan_reminders
 *   php adaptation_cron_center.php --job=dms
 */

use Bitrix\Main\Loader;

if (!defined('NO_KEEP_STATISTIC')) {
    define('NO_KEEP_STATISTIC', true);
}
if (!defined('NOT_CHECK_PERMISSIONS')) {
    define('NOT_CHECK_PERMISSIONS', true);
}
if (!defined('BX_CRONTAB')) {
    define('BX_CRONTAB', true);
}
if (!defined('BX_NO_ACCELERATOR_RESET')) {
    define('BX_NO_ACCELERATOR_RESET', true);
}

@set_time_limit(0);
@ini_set('memory_limit', '512M');

$docRoot = realpath(__DIR__ . '/..');
if (!is_dir($docRoot . '/bitrix')) {
    $docRoot = realpath(__DIR__ . '/../../');
}

chdir($docRoot);
$_SERVER['DOCUMENT_ROOT'] = $docRoot;
$_SERVER['HTTP_HOST'] = $_SERVER['HTTP_HOST'] ?? 'localhost';
$_SERVER['SERVER_NAME'] = $_SERVER['SERVER_NAME'] ?? 'localhost';

require_once $docRoot . '/bitrix/modules/main/include/prolog_before.php';

const EMPLOYEE_IBLOCK_ID = 196;
const PVD_TASKS_IBLOCK_ID = 360;
const KPI_TASKS_IBLOCK_ID = 363;
const PVD_PLAN_IBLOCK_ID = 359;

const ORG_NSK_ID = 3197820;
const STOPPED_STATUS_ID = 735;
const DATE_CREATE_MIN = '21.10.2025 00:00:00';

const STATUS_ADAPTATION_10RD_DONE = 3417329;
const STATUS_TASK_INIT = 3396791;

const OBLIGATIONS_EXISTS = 716;
const OBLIGATIONS_DONE = 6168;

const PLAN_FILLED_NO = 6198;

const EMP_STATUS_NEW = 691;
const EMP_STATUS_TRANSFER = 692;
const EMP_STATUS_TEST = 6207;

const DMS_PROP_IS_DONE = 'PROPERTY_1094';
const DMS_IS_DONE_YES = 815;
const DMS_PROP_SENT = 'PROPERTY_3018';
const DMS_SENT_N = 'N';

// ID шаблонов БП (из предоставленных ссылок)
const BP_WAIT_ACCOUNT_WELCOME = 1135;
const BP_OBLIGATIONS_CONTROL = 1307;
const BP_REMIND_PLAN_1RD = 1285;
const BP_IS_EVAL_AND_BANNER = 1087;
const BP_KPI_TASK = 1013;
const BP_PVD_TASK = 1014;
const BP_REMIND_PLAN_3RD = 1100;
const BP_REMIND_PLAN_13RD = 1101;
const BP_DMS_MAILING = 999999; // замените на реальный ID шаблона, если будет отдельный БП
const BP_OBLIGATIONS_REMINDER = 1096;

$logDir = $_SERVER['DOCUMENT_ROOT'] . '/upload/logs';
if (!is_dir($logDir)) {
    @mkdir($logDir, 0775, true);
}
$logFile = $logDir . '/adaptation_cron_center_' . date('Ymd') . '.log';

function logLine(string $message): void
{
    global $logFile;
    file_put_contents($logFile, '[' . date('Y-m-d H:i:s') . '] ' . $message . PHP_EOL, FILE_APPEND);
}

function parseCliOptions(): array
{
    global $argv;
    $opts = [
        'job' => null,
        'dry-run' => false,
    ];

    foreach ($argv as $arg) {
        if (strpos($arg, '--job=') === 0) {
            $opts['job'] = trim(substr($arg, 6));
        }
        if ($arg === '--dry-run') {
            $opts['dry-run'] = true;
        }
    }

    return $opts;
}

function detectRunWindow(?string $forcedJob): ?string
{
    if ($forcedJob !== null && $forcedJob !== '') {
        return $forcedJob;
    }

    $now = new DateTime();
    $weekDay = (int)$now->format('N');
    $time = $now->format('H:i');

    if ($time === '10:35') {
        return 'day_start';
    }
    if ($time === '17:30') {
        return 'plan_reminders';
    }
    if ($time === '16:30' && $weekDay === 2) {
        return 'dms';
    }

    return null;
}

function ensureModules(): void
{
    if (!Loader::includeModule('iblock')) {
        throw new RuntimeException('Не удалось подключить модуль iblock');
    }
    if (!Loader::includeModule('bizproc')) {
        throw new RuntimeException('Не удалось подключить модуль bizproc');
    }
}

function getEmployeeDocId(int $elementId): array
{
    return ['lists', 'BizprocDocument', 'iblock_' . EMPLOYEE_IBLOCK_ID . '_' . $elementId];
}

function getTaskDocId(int $iblockId, int $elementId): array
{
    return ['lists', 'BizprocDocument', 'iblock_' . $iblockId . '_' . $elementId];
}

function startBpForItems(int $templateId, array $items, callable $docResolver, bool $dryRun): array
{
    $started = 0;
    $errors = 0;

    foreach ($items as $itemId) {
        $itemId = (int)$itemId;
        if ($itemId <= 0) {
            continue;
        }

        if ($dryRun) {
            logLine("[DRY] BP {$templateId} -> ITEM {$itemId}");
            $started++;
            continue;
        }

        $docId = $docResolver($itemId);
        $arErrorsTmp = [];

        $workflowId = \CBPDocument::StartWorkflow(
            $templateId,
            $docId,
            [],
            $arErrorsTmp
        );

        if (!$workflowId) {
            $errors++;
            $errorText = !empty($arErrorsTmp) ? json_encode($arErrorsTmp, JSON_UNESCAPED_UNICODE) : 'unknown';
            logLine("BP {$templateId} FAILED for {$itemId}: {$errorText}");
            continue;
        }

        $started++;
        logLine("BP {$templateId} STARTED for {$itemId}, workflow={$workflowId}");
    }

    return ['started' => $started, 'errors' => $errors];
}

function fetchIdsByFilter(int $iblockId, array $filter, array $select = ['ID']): array
{
    $res = \CIBlockElement::GetList([], $filter, false, ['nPageSize' => 1000], $select);
    $items = [];

    while ($ob = $res->GetNext()) {
        $items[] = (int)$ob['ID'];
    }

    return $items;
}

function collectDayStartEmployees(): array
{
    $today = date('Y-m-d');
    $items = [];

    // Скрипт 1
    $items['first_day'] = fetchIdsByFilter(EMPLOYEE_IBLOCK_ID, [
        'IBLOCK_ID' => EMPLOYEE_IBLOCK_ID,
        'PROPERTY_ORGANIZATSIYA' => [ORG_NSK_ID],
        'PROPERTY_DATA_PRIEMA' => $today,
        '!PROPERTY_STATUS_ANKETY' => STOPPED_STATUS_ID,
        '>=DATE_CREATE' => DATE_CREATE_MIN,
    ]);

    // Скрипт 2
    $checkDate10 = addworkday_1c(date('d.m.Y'), 10);
    $dt10 = DateTime::createFromFormat('d.m.Y', $checkDate10);
    $checkDate10Ymd = $dt10 ? $dt10->format('Y-m-d') : null;

    $items['is_eval_10rd'] = $checkDate10Ymd ? fetchIdsByFilter(EMPLOYEE_IBLOCK_ID, [
        'IBLOCK_ID' => EMPLOYEE_IBLOCK_ID,
        'PROPERTY_ORGANIZATSIYA' => [ORG_NSK_ID],
        '!PROPERTY_STATUS_ANKETY' => STOPPED_STATUS_ID,
        '!PROPERTY_STATUS_ADAPTATSII' => STATUS_ADAPTATION_10RD_DONE,
        'PROPERTY_DATA_OKONCHANIYA_IS' => $checkDate10Ymd,
        '>=DATE_CREATE' => DATE_CREATE_MIN,
    ]) : [];

    // Скрипт 3
    $items['kpi_tasks'] = collectKpiTaskIds();

    // Скрипт 4
    $items['pvd_main_tasks'] = collectMainPvdTaskIds();

    // Скрипт 7
    $items['obligations_7_30_60'] = collectObligationReminderIds();

    return $items;
}

function collectKpiTaskIds(): array
{
    $currentDate = date('d.m.Y');

    $res = \CIBlockElement::GetList([], [
        'IBLOCK_ID' => KPI_TASKS_IBLOCK_ID,
        'PROPERTY_STATUS' => STATUS_TASK_INIT,
        '!PROPERTY_SROK' => false,
    ], false, false, ['ID', 'PROPERTY_SROK']);

    $items = [];
    while ($ob = $res->GetNext()) {
        $srok = (string)$ob['PROPERTY_SROK_VALUE'];
        $threeBefore = addworkday_1c($srok, -3);

        $currentTs = MakeTimeStamp($currentDate, 'DD.MM.YYYY');
        $threeBeforeTs = MakeTimeStamp($threeBefore, 'DD.MM.YYYY');

        if ($currentTs >= $threeBeforeTs) {
            $items[] = (int)$ob['ID'];
        }
    }

    return $items;
}

function collectMainPvdTaskIds(): array
{
    $resTasks = \CIBlockElement::GetList([], [
        'IBLOCK_ID' => PVD_TASKS_IBLOCK_ID,
        'PROPERTY_STATUS_ZADACHI' => STATUS_TASK_INIT,
        '!PROPERTY_PLAN_VVODA_V_DOLZHNOST' => false,
    ], false, false, ['ID', 'PROPERTY_PLAN_VVODA_V_DOLZHNOST']);

    $taskIds = [];

    while ($task = $resTasks->GetNext()) {
        $planId = (int)$task['PROPERTY_PLAN_VVODA_V_DOLZHNOST_VALUE'];
        if ($planId <= 0) {
            continue;
        }

        $resPlan = \CIBlockElement::GetList([], [
            'IBLOCK_ID' => PVD_PLAN_IBLOCK_ID,
            'ID' => $planId,
        ], false, false, ['ID']);

        if ($resPlan->GetNext()) {
            $taskIds[] = (int)$task['ID'];
        }
    }

    return array_reverse($taskIds);
}

function getTargetWorkDate(int $workdays): string
{
    $targetDate = date('d.m.Y');
    $daysCount = 0;

    while ($daysCount < $workdays) {
        $targetDate = date('d.m.Y', strtotime($targetDate . ' -1 day'));
        if (isworkday_1c($targetDate)) {
            $daysCount++;
        }
    }

    return date('Y-m-d', strtotime($targetDate));
}

function collectPlanReminderIds(int $workdays): array
{
    return fetchIdsByFilter(EMPLOYEE_IBLOCK_ID, [
        'IBLOCK_ID' => EMPLOYEE_IBLOCK_ID,
        '!PROPERTY_PLAN_VVODA_V_DOLZHNOST' => false,
        'PROPERTY_STATUS_SOTRUDNIKA' => [EMP_STATUS_NEW, EMP_STATUS_TRANSFER, EMP_STATUS_TEST],
        'PROPERTY_PLAN_VVODA_V_DOLZHNOST_ZAPOLNEN' => PLAN_FILLED_NO,
        'PROPERTY_DATA_PRIEMA' => getTargetWorkDate($workdays),
        '>=DATE_CREATE' => DATE_CREATE_MIN,
    ]);
}

function collectDmsIds(): array
{
    return fetchIdsByFilter(EMPLOYEE_IBLOCK_ID, [
        'IBLOCK_ID' => EMPLOYEE_IBLOCK_ID,
        'ACTIVE' => 'Y',
        DMS_PROP_IS_DONE => DMS_IS_DONE_YES,
        DMS_PROP_SENT => DMS_SENT_N,
        '>=DATE_CREATE' => DATE_CREATE_MIN,
    ], ['ID']);
}

function collectObligationReminderIds(): array
{
    $today = new DateTime();
    $dates = [
        (clone $today)->sub(new DateInterval('P7D'))->format('Y-m-d'),
        (clone $today)->sub(new DateInterval('P30D'))->format('Y-m-d'),
        (clone $today)->sub(new DateInterval('P60D'))->format('Y-m-d'),
    ];

    return fetchIdsByFilter(EMPLOYEE_IBLOCK_ID, [
        'IBLOCK_ID' => EMPLOYEE_IBLOCK_ID,
        'PROPERTY_ORGANIZATSIYA' => ORG_NSK_ID,
        'PROPERTY_DATA_PRIEMA' => $dates,
        'PROPERTY_EST_LI_OBYAZATELSTVO_LST' => OBLIGATIONS_EXISTS,
        'PROPERTY_OBYAZATELSTVA_ISPOLNENY' => OBLIGATIONS_DONE,
        '!PROPERTY_STATUS_ANKETY' => STOPPED_STATUS_ID,
        '>=DATE_CREATE' => DATE_CREATE_MIN,
    ]);
}

function runDayStart(bool $dryRun): void
{
    $sets = collectDayStartEmployees();

    logLine('day_start sets: ' . json_encode(array_map('count', $sets), JSON_UNESCAPED_UNICODE));

    startBpForItems(BP_WAIT_ACCOUNT_WELCOME, $sets['first_day'], 'getEmployeeDocId', $dryRun);
    startBpForItems(BP_OBLIGATIONS_CONTROL, $sets['first_day'], 'getEmployeeDocId', $dryRun);
    startBpForItems(BP_REMIND_PLAN_1RD, $sets['first_day'], 'getEmployeeDocId', $dryRun);

    startBpForItems(BP_IS_EVAL_AND_BANNER, $sets['is_eval_10rd'], 'getEmployeeDocId', $dryRun);

    startBpForItems(BP_KPI_TASK, $sets['kpi_tasks'], function (int $id) {
        return getTaskDocId(KPI_TASKS_IBLOCK_ID, $id);
    }, $dryRun);

    startBpForItems(BP_PVD_TASK, $sets['pvd_main_tasks'], function (int $id) {
        return getTaskDocId(PVD_TASKS_IBLOCK_ID, $id);
    }, $dryRun);

    startBpForItems(BP_OBLIGATIONS_REMINDER, $sets['obligations_7_30_60'], 'getEmployeeDocId', $dryRun);
}

function runPlanReminders(bool $dryRun): void
{
    $ids3 = collectPlanReminderIds(3);
    $ids13 = collectPlanReminderIds(13);

    logLine('plan_reminders: 3rd=' . count($ids3) . '; 13rd=' . count($ids13));

    startBpForItems(BP_REMIND_PLAN_3RD, $ids3, 'getEmployeeDocId', $dryRun);
    startBpForItems(BP_REMIND_PLAN_13RD, $ids13, 'getEmployeeDocId', $dryRun);
}

function runDms(bool $dryRun): void
{
    $ids = collectDmsIds();
    logLine('dms: ids=' . count($ids));

    if (BP_DMS_MAILING === 999999) {
        logLine('dms: BP_DMS_MAILING не настроен, пропуск запуска.');
        return;
    }

    startBpForItems(BP_DMS_MAILING, $ids, 'getEmployeeDocId', $dryRun);
}

try {
    ensureModules();
    $opts = parseCliOptions();
    $runWindow = detectRunWindow($opts['job']);

    if ($runWindow === null) {
        logLine('Пропуск: текущее время не попадает в окно и --job не указан.');
        exit(0);
    }

    logLine('START runWindow=' . $runWindow . '; dry=' . ($opts['dry-run'] ? 'Y' : 'N'));

    switch ($runWindow) {
        case 'all':
            runDayStart($opts['dry-run']);
            runPlanReminders($opts['dry-run']);
            runDms($opts['dry-run']);
            break;
        case 'day_start':
            runDayStart($opts['dry-run']);
            break;
        case 'plan_reminders':
            runPlanReminders($opts['dry-run']);
            break;
        case 'dms':
            runDms($opts['dry-run']);
            break;
        default:
            throw new RuntimeException('Неизвестный job: ' . $runWindow);
    }

    logLine('END OK');
} catch (Throwable $e) {
    logLine('FATAL: ' . $e->getMessage());
    logLine($e->getTraceAsString());
    exit(1);
}
