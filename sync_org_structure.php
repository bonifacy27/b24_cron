<?php
/**
 * Импорт организационной структуры сотрудников из 1С в HL-блок.
 *
 * Скрипт проходит по активным пользователям портала с заполненным UF_1C_GUID,
 * запрашивает данные 1С по адресу GetStaffInfo/{UF_1C_GUID}/ и создает/обновляет
 * запись HL-блока по каждому сотруднику.
 *
 * Для отчетов дополнительно сохраняются данные руководителя подразделения:
 *  - UF_MANAGER_GUID — GUID руководителя из поля 1С managerGuid;
 *  - UF_MANAGER — пользователь портала, найденный по UF_1C_GUID = managerGuid.
 */

use Bitrix\Highloadblock as HL;
use Bitrix\Main\Loader;

const NO_KEEP_STATISTIC = true;
const NOT_CHECK_PERMISSIONS = true;
const BX_CRONTAB = true;
const BX_NO_ACCELERATOR_RESET = true;

@set_time_limit(0);
@ini_set('memory_limit', '512M');

$docRoot = realpath(__DIR__ . '/..');
if (!is_dir($docRoot . '/bitrix')) {
    $docRoot = realpath(__DIR__ . '/../../');
}
if (!$docRoot || !is_dir($docRoot . '/bitrix')) {
    $docRoot = '/home/bitrix/www';
}

chdir($docRoot);
$_SERVER['DOCUMENT_ROOT'] = $docRoot;
$_SERVER['HTTP_HOST'] = $_SERVER['HTTP_HOST'] ?? 'localhost';
$_SERVER['SERVER_NAME'] = $_SERVER['SERVER_NAME'] ?? 'localhost';

require_once $docRoot . '/bitrix/modules/main/include/prolog_before.php';

if (!Loader::includeModule('main')) {
    throw new RuntimeException('Не подключен модуль main');
}
if (!Loader::includeModule('highloadblock')) {
    throw new RuntimeException('Не подключен модуль highloadblock');
}

// ===================== НАСТРОЙКИ =====================
// Если ID не указан, скрипт попробует найти HL-блок автоматически по полям UF_MANAGER_GUID и UF_MANAGER.
const HL_ORG_STRUCTURE_ID = 0;
const STAFF_INFO_URL_TEMPLATE = 'https://srv-off-1c01.nsc.ru/zup2/hs/Bitrix24Exchange/GetStaffInfo/%s/';
const REQUEST_TIMEOUT_SEC = 20;
const LOG_FILE_PREFIX = 'sync_org_structure_';
const DRY_RUN = false;
// =====================================================

$logDir = $_SERVER['DOCUMENT_ROOT'] . '/upload/logs';
if (!is_dir($logDir)) {
    @mkdir($logDir, 0775, true);
}
$logFile = $logDir . '/' . LOG_FILE_PREFIX . date('Ymd') . '.log';

function logLine(string $message): void
{
    global $logFile;
    file_put_contents($logFile, '[' . date('Y-m-d H:i:s') . '] ' . $message . "\n", FILE_APPEND);
}

function normalizeGuid(?string $guid): string
{
    $guid = strtolower(trim((string)$guid));
    return trim($guid, "{} \t\n\r\0\x0B");
}

function normalizeNullableString($value): string
{
    $value = trim((string)$value);
    return strcasecmp($value, 'null') === 0 ? '' : $value;
}

function getHlFieldNames(int $hlBlockId): array
{
    $fields = [];
    $rs = CUserTypeEntity::GetList(['FIELD_NAME' => 'ASC'], ['ENTITY_ID' => 'HLBLOCK_' . $hlBlockId]);
    while ($field = $rs->Fetch()) {
        $fields[(string)$field['FIELD_NAME']] = true;
    }

    return $fields;
}

function detectOrgStructureHlBlockId(): int
{
    $candidates = [];
    $rs = HL\HighloadBlockTable::getList(['select' => ['ID', 'NAME', 'TABLE_NAME']]);
    while ($hlBlock = $rs->fetch()) {
        $hlBlockId = (int)$hlBlock['ID'];
        $fields = getHlFieldNames($hlBlockId);
        if (isset($fields['UF_MANAGER_GUID'], $fields['UF_MANAGER'])) {
            $score = 0;
            foreach (['UF_USER', 'UF_1C_GUID', 'UF_PERSONNEL_NUMBER', 'UF_POSITION', 'UF_CEO1'] as $fieldName) {
                if (isset($fields[$fieldName])) {
                    $score++;
                }
            }
            $candidates[] = ['ID' => $hlBlockId, 'SCORE' => $score, 'NAME' => $hlBlock['NAME']];
        }
    }

    usort($candidates, static function (array $a, array $b): int {
        return $b['SCORE'] <=> $a['SCORE'] ?: $a['ID'] <=> $b['ID'];
    });

    if (!$candidates) {
        throw new RuntimeException('Не найден HL-блок оргструктуры: укажите HL_ORG_STRUCTURE_ID или проверьте поля UF_MANAGER_GUID/UF_MANAGER');
    }

    logLine('HL-блок оргструктуры определен автоматически: ID=' . $candidates[0]['ID'] . ', NAME=' . $candidates[0]['NAME']);
    return (int)$candidates[0]['ID'];
}

function getHlDataClass(int $hlBlockId): string
{
    $hlBlock = HL\HighloadBlockTable::getById($hlBlockId)->fetch();
    if (!$hlBlock) {
        throw new RuntimeException('HL-блок не найден: ID=' . $hlBlockId);
    }

    $entity = HL\HighloadBlockTable::compileEntity($hlBlock);
    return $entity->getDataClass();
}

function getUsersByOneCGuid(): array
{
    $users = [];
    $rs = CUser::GetList(
        $by = 'id',
        $order = 'asc',
        ['ACTIVE' => 'Y'],
        ['FIELDS' => ['ID'], 'SELECT' => ['UF_1C_GUID']]
    );

    while ($user = $rs->Fetch()) {
        $userId = (int)$user['ID'];
        $guid = normalizeGuid($user['UF_1C_GUID'] ?? '');
        if ($userId > 0 && $guid !== '' && $guid !== '0') {
            $users[$guid] = $userId;
        }
    }

    return $users;
}

function fetchStaffInfo(string $employeeGuid): ?array
{
    $url = sprintf(STAFF_INFO_URL_TEMPLATE, rawurlencode($employeeGuid));
    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_CONNECTTIMEOUT => REQUEST_TIMEOUT_SEC,
        CURLOPT_TIMEOUT => REQUEST_TIMEOUT_SEC,
        CURLOPT_FOLLOWLOCATION => true,
        CURLOPT_SSL_VERIFYPEER => false,
        CURLOPT_SSL_VERIFYHOST => false,
    ]);

    $response = curl_exec($ch);
    $errno = curl_errno($ch);
    $error = curl_error($ch);
    $status = (int)curl_getinfo($ch, CURLINFO_RESPONSE_CODE);
    curl_close($ch);

    if ($errno) {
        logLine("WARN: ошибка запроса 1С для {$employeeGuid}: {$error}");
        return null;
    }
    if ($status < 200 || $status >= 300) {
        logLine("WARN: 1С вернула HTTP {$status} для {$employeeGuid}");
        return null;
    }

    $data = json_decode((string)$response, true);
    if (!is_array($data)) {
        logLine("WARN: некорректный JSON от 1С для {$employeeGuid}");
        return null;
    }
    if (array_key_exists('success', $data) && $data['success'] !== true && $data['success'] !== 'true' && $data['success'] !== 1) {
        logLine("WARN: 1С вернула success=false для {$employeeGuid}");
        return null;
    }

    return $data;
}

function filterPayloadByExistingFields(array $payload, array $existingFields): array
{
    return array_intersect_key($payload, $existingFields);
}

function findExistingHlRow(string $dataClass, array $existingFields, int $userId, string $employeeGuid, string $personnelNumber): ?array
{
    $filters = [];
    if (isset($existingFields['UF_USER'])) {
        $filters[] = ['=UF_USER' => $userId];
    }
    if (isset($existingFields['UF_1C_GUID'])) {
        $filters[] = ['=UF_1C_GUID' => $employeeGuid];
    }
    if ($personnelNumber !== '' && isset($existingFields['UF_PERSONNEL_NUMBER'])) {
        $filters[] = ['=UF_PERSONNEL_NUMBER' => $personnelNumber];
    }

    foreach ($filters as $filter) {
        $row = $dataClass::getList([
            'select' => ['ID'],
            'filter' => $filter,
            'limit' => 1,
        ])->fetch();
        if ($row) {
            return $row;
        }
    }

    return null;
}

$startedAt = microtime(true);
logLine('=== Старт импорта оргструктуры из 1С ===');

$hlBlockId = HL_ORG_STRUCTURE_ID > 0 ? HL_ORG_STRUCTURE_ID : detectOrgStructureHlBlockId();
$dataClass = getHlDataClass($hlBlockId);
$existingFields = getHlFieldNames($hlBlockId);
$usersByGuid = getUsersByOneCGuid();

$processed = 0;
$created = 0;
$updated = 0;
$skipped = 0;
$errors = 0;

foreach ($usersByGuid as $employeeGuid => $userId) {
    $staffInfo = fetchStaffInfo($employeeGuid);
    if ($staffInfo === null) {
        $skipped++;
        continue;
    }

    $managerGuid = normalizeGuid($staffInfo['managerGuid'] ?? '');
    $managerUserId = $managerGuid !== '' && isset($usersByGuid[$managerGuid]) ? (int)$usersByGuid[$managerGuid] : null;
    $personnelNumber = normalizeNullableString($staffInfo['personnelNumber'] ?? '');

    $payload = [
        'UF_USER' => $userId,
        'UF_1C_GUID' => $employeeGuid,
        'UF_LAST_NAME' => normalizeNullableString($staffInfo['lastName'] ?? ''),
        'UF_FIRST_NAME' => normalizeNullableString($staffInfo['firstName'] ?? ''),
        'UF_MIDDLE_NAME' => normalizeNullableString($staffInfo['middleName'] ?? ''),
        'UF_POSITION' => normalizeNullableString($staffInfo['position'] ?? ''),
        'UF_POSITION_GUID' => normalizeGuid($staffInfo['positionGuid'] ?? ''),
        'UF_POSITION_CATEGORY' => normalizeNullableString($staffInfo['positionCategory'] ?? ''),
        'UF_PERSONNEL_NUMBER' => $personnelNumber,
        'UF_CEO1' => normalizeNullableString($staffInfo['ceo1'] ?? ''),
        'UF_CEO2' => normalizeNullableString($staffInfo['ceo2'] ?? ''),
        'UF_CEO3' => normalizeNullableString($staffInfo['ceo3'] ?? ''),
        'UF_CEO4' => normalizeNullableString($staffInfo['ceo4'] ?? ''),
        'UF_CEO5' => normalizeNullableString($staffInfo['ceo5'] ?? ''),
        'UF_MANAGER_GUID' => $managerGuid,
        'UF_MANAGER' => $managerUserId,
    ];

    if ($managerGuid !== '' && $managerUserId === null) {
        logLine("INFO: руководитель для user_id={$userId}, managerGuid={$managerGuid} не найден в портале по UF_1C_GUID");
    }

    $payload = filterPayloadByExistingFields($payload, $existingFields);
    $existingRow = findExistingHlRow($dataClass, $existingFields, $userId, $employeeGuid, $personnelNumber);

    if (DRY_RUN) {
        logLine(($existingRow ? 'DRY update' : 'DRY add') . ": user_id={$userId}, guid={$employeeGuid}, manager_guid={$managerGuid}, manager_user=" . ($managerUserId ?: ''));
        $processed++;
        continue;
    }

    if ($existingRow) {
        $result = $dataClass::update((int)$existingRow['ID'], $payload);
        if ($result->isSuccess()) {
            $updated++;
        } else {
            $errors++;
            logLine('ERROR: обновление HL#' . $existingRow['ID'] . ' user_id=' . $userId . ': ' . implode('; ', $result->getErrorMessages()));
        }
    } else {
        $result = $dataClass::add($payload);
        if ($result->isSuccess()) {
            $created++;
        } else {
            $errors++;
            logLine('ERROR: создание записи user_id=' . $userId . ': ' . implode('; ', $result->getErrorMessages()));
        }
    }

    $processed++;
}

$elapsed = round(microtime(true) - $startedAt, 2);
logLine("=== Импорт завершен: processed={$processed}, created={$created}, updated={$updated}, skipped={$skipped}, errors={$errors}, elapsed={$elapsed}s ===");
echo "OK; processed={$processed}; created={$created}; updated={$updated}; skipped={$skipped}; errors={$errors}; elapsed={$elapsed}s\n";
