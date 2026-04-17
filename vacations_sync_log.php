<?php
/**
 * Vacations <-> GateDB_Test sync
 * Only ACTIVE users; 6-month window; only statuses [4,5,6,7,8]; diffed; batch MERGE; resume cursor
 * Version: v1.6.4-fix (2025-11-12)
 *
 * Исправления:
 *  - HL → SQL: UF_VACATION_STATE → Absence_State
 *  - SQL → HL: Absence_State → UF_VACATION_STATE
 *  - Поле UF_STATUS больше не используется для синхронизации состояния отпуска
 */
use Bitrix\Highloadblock as HL;
use Bitrix\Main\Loader;
use Bitrix\Main\Application;
use Bitrix\Main\Config\Option;
use Bitrix\Main\Type\Date;
use Bitrix\Main\Type\DateTime;
define('NOT_CHECK_PERMISSIONS', true);
define('NO_KEEP_STATISTIC', true);
define('BX_NO_ACCELERATOR_RESET', true);
// Устанавливаем DOCUMENT_ROOT вручную для CLI-режима
$_SERVER['DOCUMENT_ROOT'] = '/home/bitrix/www';
require($_SERVER['DOCUMENT_ROOT'].'/bitrix/modules/main/include/prolog_before.php');
Loader::includeModule('main');
Loader::includeModule('highloadblock');
const HLBLOCK_ID       = 83;
const MODULE_ID        = 'local.vacations.sync';
const OPT_SQLHL_CUR_R  = 'sqlhl_cursor_renew';
const OPT_SQLHL_CUR_I  = 'sqlhl_cursor_id';
const GATE_CONN_NAME   = 'gatedb';
const GATE_DB_DBO      = 'GateDB.dbo';
const GATE_TABLE       = 'StaffAbsences_1CZUP';
// Limits
const TIME_BUDGET_SEC          = 55;
const MAX_SQL_UPDATES_PER_RUN  = 6000;
const GATE_BATCH_SIZE          = 3000;
const HL_EMP_CHUNK_SIZE        = 1000;
const HL_SQL_MERGE_CHUNK       = 500;
const MONTH_WINDOW             = 6;
const ALLOWED_STATES           = [4,5,6,7,8];
const BASE_ABSENCE_ID_REGEX    = '/^\d+$/';
const DERIVED_VACATION_TYPE_ID = 3018104;
$startedAt = microtime(true);
// ---------- logging ----------
function logx(string $msg): void {
    static $fh = null;
    if ($fh === null) {
        $fn = '/home/bitrix/www/upload/logs/absences_exchange_test_'.date('Ymd').'.log';
        error_log("[DEBUG] Log filename: $fn");
        @mkdir(dirname($fn), 0775, true);
        if (!is_dir(dirname($fn))) {
            error_log("[ERROR] Directory still missing after mkdir: " . dirname($fn));
        }
        $fh = fopen($fn, 'ab');
        if ($fh === false) {
            error_log("[ERROR] fopen failed for: $fn");
            error_log("[ERROR] is_writable: " . (is_writable(dirname($fn)) ? 'yes' : 'no'));
            error_log("[ERROR] file_exists: " . (file_exists($fn) ? 'yes' : 'no'));
            error_log("[ERROR] touch test: " . (touch($fn) ? 'ok' : 'failed'));
            // fallback: не падать
            return;
        }
    }
    if (is_resource($fh)) {
        fwrite($fh, sprintf("[%s] %s\n", date('Y-m-d H:i:s'), $msg));
        fflush($fh);
    }
}
// ---------- helpers ----------
function compileHLDataClass(int $hlId): string {
    $hl = HL\HighloadBlockTable::getById($hlId)->fetch();
    $entity = HL\HighloadBlockTable::compileEntity($hl);
    return $entity->getDataClass();
}
/** Normalize GUID string to canonical form used in SQL comparisons */
function normalizeGuid(string $guid): string {
    $g = strtolower(trim($guid));
    $g = trim($g, "{}");
    return $g;
}
/** ACTIVE=Y + UF_1C_GUID not empty */
function getActiveUsersWithGuid(): array {
    $ids  = [];
    $guid = [];
    $rs = \CUser::GetList($by='id', $order='asc', ['ACTIVE'=>'Y'], ['FIELDS'=>['ID'], 'SELECT'=>['UF_1C_GUID']]);
    while ($u = $rs->Fetch()) {
        $uid = (int)$u['ID'];
        $g = normalizeGuid((string)($u['UF_1C_GUID'] ?? ''));
        if ($uid > 0 && $g !== '' && $g !== '0') {
            $ids[] = $uid;
            $guid[$uid] = $g;
        }
    }
    return [$ids, $guid, array_values(array_unique(array_filter($guid)))];
}
/** Build absence name */
function buildAbsenceName(array $hlRow): string {
    $start = $hlRow['UF_DATE_BEGIN'] instanceof DateTime
        ? $hlRow['UF_DATE_BEGIN']->format('d.m.Y')
        : date('d.m.Y', strtotime((string)$hlRow['UF_DATE_BEGIN']));
    $end = $hlRow['UF_DATE_END'] instanceof DateTime
        ? $hlRow['UF_DATE_END']->format('d.m.Y')
        : date('d.m.Y', strtotime((string)$hlRow['UF_DATE_END']));
    return "Ежегодный отпуск {$start}-{$end}";
}
/** BX date/time -> 'Y-m-d' or null */
function toSqlDate($bxDate): ?string {
    if (!$bxDate) return null;
    if ($bxDate instanceof Date || $bxDate instanceof DateTime) {
        return $bxDate->format('Y-m-d');
    }
    $ts = strtotime((string)$bxDate);
    return $ts ? date('Y-m-d', $ts) : null;
}
/** Resume cursor */
function loadSqlHlCursor(): array {
    $r = Option::get(MODULE_ID, OPT_SQLHL_CUR_R, '');
    $i = Option::get(MODULE_ID, OPT_SQLHL_CUR_I, '');
    if (!$r) $r = '1900-01-01 00:00:00';
    return [$r, (string)$i];
}
function saveSqlHlCursor(string $renew, string $id): void {
    Option::set(MODULE_ID, OPT_SQLHL_CUR_R, $renew);
    Option::set(MODULE_ID, OPT_SQLHL_CUR_I, $id);
}
/** Primary (portal) absence ID only, e.g. "12345". Derived IDs like "12345_a" are not primary. */
function isBaseAbsenceId(string $absenceId): bool {
    return (bool)preg_match(BASE_ABSENCE_ID_REGEX, $absenceId);
}
/** SQL datetime string -> normalized 'Y-m-d H:i:s' */
function normalizeSqlDateTime(?string $sqlDateTime): ?string {
    if (!$sqlDateTime) return null;
    $ts = strtotime($sqlDateTime);
    return $ts ? date('Y-m-d H:i:s', $ts) : null;
}
/** SQL date string -> BX Date */
function toBxDate(?string $sqlDate): ?Date {
    if (!$sqlDate) return null;
    $ts = strtotime($sqlDate);
    if (!$ts) return null;
    return new Date(date('Y-m-d', $ts), 'Y-m-d');
}
/** Date end = date begin + vacation days */
function calcBxDateEnd(Date $begin, int $days): Date {
    $php = new \DateTimeImmutable($begin->format('Y-m-d'));
    $php = $php->modify(sprintf('+%d day', max(0, $days)));
    return new Date($php->format('Y-m-d'), 'Y-m-d');
}
/** Sync SQL Absence_Renew_Date with real HL changed_at to avoid reprocessing */
function syncSqlRenewDate(\Bitrix\Main\DB\Connection $gateConn, $sqlHelper, string $absenceId, string $hlTime): void {
    $u = "UPDATE ".GATE_DB_DBO.".".GATE_TABLE."
          SET Absence_Renew_Date = N'".$sqlHelper->forSql($hlTime)."'
          WHERE Absence_ID = N'".$sqlHelper->forSql($absenceId)."'";
    $gateConn->queryExecute($u);
}
// ---------- setup ----------
$dataClass = compileHLDataClass(HLBLOCK_ID);
$gateConn  = Application::getConnection(GATE_CONN_NAME);
$sqlHelper = $gateConn->getSqlHelper();
// cutoff (6 months)
$cutoffYmd = '';
$cutoffBxDate = null;
try {
    $cutoffPhp = new \DateTimeImmutable('-'.MONTH_WINDOW.' months');
    $cutoffYmd = $cutoffPhp->format('Y-m-d');
    if ($cutoffYmd && preg_match('/^\d{4}-\d{2}-\d{2}$/', $cutoffYmd)) {
        $cutoffBxDate = new Date($cutoffYmd, 'Y-m-d');
    }
} catch (\Throwable $e) {
    $ts = strtotime('-'.MONTH_WINDOW.' months');
    if ($ts) {
        $cutoffYmd = date('Y-m-d', $ts);
        $cutoffBxDate = new Date($cutoffYmd, 'Y-m-d');
    }
}
logx("=== Test sync v1.6.2-fixed start ===");
logx("Cutoff date (>=): ".($cutoffYmd ?: '<none>'));
logx("Allowed UF_STATE: ".implode(',', ALLOWED_STATES));
// Users & GUIDs
[$activeUserIds, $userGuidMap, $activeGuidList] = getActiveUsersWithGuid();
$guidToUserId = [];
foreach ($userGuidMap as $uid => $guid) {
    $guidToUserId[normalizeGuid((string)$guid)] = (int)$uid;
}
// Карта GUID -> USER_ID для всех пользователей (в т.ч. неактивных),
// чтобы доп. записи из 1С не терялись на SQL-уровне фильтра только по ACTIVE=Y.
$allGuidToUserId = [];
$rsAllUsers = \CUser::GetList($by='id', $order='asc', [], ['FIELDS'=>['ID'], 'SELECT'=>['UF_1C_GUID']]);
while ($u = $rsAllUsers->Fetch()) {
    $uid = (int)$u['ID'];
    $guid = normalizeGuid((string)($u['UF_1C_GUID'] ?? ''));
    if ($uid > 0 && $guid !== '' && $guid !== '0' && !isset($allGuidToUserId[$guid])) {
        $allGuidToUserId[$guid] = $uid;
    }
}
logx("Active users with GUID: users=".count($activeUserIds).", guids=".count($activeGuidList));
if (!$activeUserIds || !$activeGuidList) {
    logx("Нет активных пользователей — прекращаем.");
    echo "OK v1.6.4-fix; no active users\n";
    return;
}
// ---------- HL -> SQL ----------
$hl2sqlCount = 0;
$selectHL = [
    'ID','UF_STATUS','UF_STATE','UF_VACATION_STATE','UF_VACATION_DAYS','UF_DATE_END',
    'UF_DATE_BEGIN','UF_EMPLOYEE','UF_CHANGED_AT'
];
foreach (array_chunk($activeUserIds, HL_EMP_CHUNK_SIZE) as $ci => $uidChunk) {
    if (microtime(true) - $startedAt > TIME_BUDGET_SEC) {
        logx("Time budget reached during HL->SQL (chunk ".($ci+1).")");
        break;
    }
    $filter = ['@UF_EMPLOYEE' => $uidChunk, '@UF_STATE' => ALLOWED_STATES];
    if ($cutoffBxDate instanceof Date) {
        $filter['>=UF_DATE_BEGIN'] = $cutoffBxDate;
    } else {
        logx("WARN: cutoff is invalid, skipping UF_DATE_BEGIN filter in HL->SQL");
    }
    $res = $dataClass::getList([
        'select' => $selectHL,
        'filter' => $filter,
        'order'  => ['UF_CHANGED_AT' => 'ASC', 'ID' => 'ASC'],
    ]);
    $hlRows = [];
    while ($r = $res->fetch()) {
        $empId = (int)$r['UF_EMPLOYEE'];
        $guid  = $userGuidMap[$empId] ?? null;
        if (!$guid) continue;
        $dateBegin = toSqlDate($r['UF_DATE_BEGIN']);
        if (!$dateBegin) continue;
        $hlChangedAt = $r['UF_CHANGED_AT'] instanceof DateTime
            ? $r['UF_CHANGED_AT']->format('Y-m-d H:i:s')
            : ($r['UF_CHANGED_AT'] ? date('Y-m-d H:i:s', strtotime((string)$r['UF_CHANGED_AT'])) : null);
        if (!$hlChangedAt) continue;
        $hlRows[] = [
            'ID'                => (int)$r['ID'],
            'GUID'              => $guid,
            'UF_CHANGED_AT'     => $hlChangedAt,
            'UF_STATE'          => (int)$r['UF_STATE'],
            'UF_STATUS'         => (int)$r['UF_STATUS'],
            'UF_VACATION_STATE' => (int)($r['UF_VACATION_STATE'] ?? 0),
            'UF_DATE_BEGIN'     => $dateBegin,
            'UF_VAC_DAYS'       => (int)$r['UF_VACATION_DAYS'],
            'NAME'              => buildAbsenceName($r),
        ];
    }
    if (!$hlRows) continue;
    // renew map из шлюза
    $idsForGate = array_map(fn($x) => "N'".$sqlHelper->forSql((string)$x['ID'])."'", $hlRows);
    $gateMap = [];
    foreach (array_chunk($idsForGate, 1000) as $idsChunk) {
        $q = "SELECT Absence_ID, Absence_Renew_Date FROM ".GATE_DB_DBO.".".GATE_TABLE." WHERE Absence_ID IN (".implode(',', $idsChunk).")";
        $rsGate = $gateConn->query($q);
        while ($g = $rsGate->fetch()) {
            $gateMap[(string)$g['Absence_ID']] = $g['Absence_Renew_Date']
                ? date('Y-m-d H:i:s', strtotime($g['Absence_Renew_Date']))
                : null;
        }
    }
    // кандидаты
    $candidates = [];
    foreach ($hlRows as $row) {
        $sqlRenew = $gateMap[(string)$row['ID']] ?? null;
        if ($sqlRenew === null || $row['UF_CHANGED_AT'] > $sqlRenew) {
            $candidates[] = $row;
        }
    }
    if (!$candidates) continue;
    // batch MERGE
    foreach (array_chunk($candidates, HL_SQL_MERGE_CHUNK) as $batch) {
        if (microtime(true) - $startedAt > TIME_BUDGET_SEC) {
            logx("Time budget reached in HL->SQL MERGE");
            break 2;
        }
        $vals = [];
        $logBatch = [];
        foreach ($batch as $b) {
            $logBatch[] = sprintf("id=%s статус=%s состояние=%s", (string)$b['ID'], (string)$b['UF_STATE'], (string)$b['UF_VACATION_STATE']);
            $vals[] = "("
                ."N'".$sqlHelper->forSql((string)$b['ID'])."',"
                ."N'".$sqlHelper->forSql($b['GUID'])."',"
                ."N'".$sqlHelper->forSql($b['NAME'])."',"
                .(int)$b['UF_STATE'].","
                .(int)$b['UF_VACATION_STATE'].","
                ."N'".$sqlHelper->forSql($b['UF_DATE_BEGIN'])."',"
                .(int)$b['UF_VAC_DAYS'].","
                ."N'".$sqlHelper->forSql($b['UF_CHANGED_AT'])."'"
            .")";
        }
        $sql = "
MERGE ".GATE_DB_DBO.".".GATE_TABLE." AS T
USING (VALUES ".implode(",", $vals).") AS S
(Absence_ID,Staff_ID,Absence_Name,Absence_Status,Absence_State,Absence_Date_Start,Absence_Day_Count,Src_Changed_At)
ON (T.Absence_ID = S.Absence_ID)
WHEN MATCHED AND (T.Absence_Renew_Date IS NULL OR T.Absence_Renew_Date < S.Src_Changed_At) THEN
  UPDATE SET
    T.Staff_ID = S.Staff_ID,
    T.Absence_Name = S.Absence_Name,
    T.Absence_Status = S.Absence_Status,
    T.Absence_Renew_Date = S.Src_Changed_At,
    T.Absence_Date_Start = S.Absence_Date_Start,
    T.Absence_Day_Count = S.Absence_Day_Count,
    T.Absence_State = S.Absence_State
WHEN NOT MATCHED THEN
  INSERT (Absence_ID,Staff_ID,Absence_Name,Absence_Status,Absence_Renew_Date,Absence_Date_Start,Absence_Day_Count,Absence_State,Source)
  VALUES (S.Absence_ID,S.Staff_ID,S.Absence_Name,S.Absence_Status,S.Src_Changed_At,S.Absence_Date_Start,S.Absence_Day_Count,S.Absence_State,N'ourtricolortv.nsc.ru');";
        logx("HL->SQL ОБНОВЛЕНИЕ ".count($batch)." rows: ".implode(", ", $logBatch));
        $gateConn->queryExecute($sql);
        $hl2sqlCount += count($batch);
    }
}
logx("HL->SQL done: {$hl2sqlCount}");
// ---------- SQL -> HL ----------
$sql2hlCount = 0;
list($cursorRenew, $cursorId) = loadSqlHlCursor();
$guidInList = implode(',', array_map(fn($g) => "N'".$sqlHelper->forSql($g)."'", $activeGuidList));
logx("SQL->HL resume cursor at: renew={$cursorRenew}, id='{$cursorId}'");
do {
    if (microtime(true) - $startedAt > TIME_BUDGET_SEC) {
        logx("Time budget reached during SQL->HL main loop");
        break;
    }
    $cutoffSqlClause = '';
    if ($cutoffYmd && preg_match('/^\d{4}-\d{2}-\d{2}$/', $cutoffYmd)) {
        $cutoffSqlClause = "AND Absence_Date_Start >= N'".$sqlHelper->forSql($cutoffYmd)."'";
    } else {
        logx("WARN: cutoff is invalid, skipping Absence_Date_Start filter in SQL->HL");
    }
    $q = "
SELECT TOP ".GATE_BATCH_SIZE."
  Absence_ID,AbsenceBases_ID,DocumentVacation_ID,Staff_ID,Absence_Name,Absence_Status,Absence_Renew_Date,Absence_State,Absence_Date_Start,Absence_Day_Count
FROM ".GATE_DB_DBO.".".GATE_TABLE."
WHERE (Staff_ID IN ($guidInList) OR PATINDEX('%[^0-9]%', Absence_ID) > 0)
  AND Absence_Status IN (".implode(',', ALLOWED_STATES).")
  $cutoffSqlClause
  AND (
    Absence_Renew_Date > '".$sqlHelper->forSql($cursorRenew)."'
    OR (Absence_Renew_Date = '".$sqlHelper->forSql($cursorRenew)."' AND Absence_ID > N'".$sqlHelper->forSql($cursorId)."')
  )
ORDER BY Absence_Renew_Date ASC, Absence_ID ASC";
    logx("SQL->HL fetch window: cursor={$cursorRenew}, id='{$cursorId}'");
    $rs = $gateConn->query($q);
    $rows = [];
    while ($r = $rs->fetch()) { $rows[] = $r; }
    if (!$rows) { logx("SQL->HL: no more rows after cursor"); break; }
    $firstRow = $rows[0];
    $lastRow = $rows[count($rows)-1];
    logx("SQL->HL fetched rows=".count($rows).", first=".((string)$firstRow['Absence_ID'])."/".((string)$firstRow['Absence_Renew_Date']).", last=".((string)$lastRow['Absence_ID'])."/".((string)$lastRow['Absence_Renew_Date']));
    // Предзагрузка существующих HL-элементов
    $hlIds = [];
    foreach ($rows as $row) {
        if (isBaseAbsenceId((string)$row['Absence_ID'])) $hlIds[] = (int)$row['Absence_ID'];
    }
    $derivedPrefixes = [];
    foreach ($rows as $row) {
        if (!isBaseAbsenceId((string)$row['Absence_ID'])) {
            $derivedPrefixes[] = (string)$row['Absence_ID'];
        }
    }
    $hlMap = [];
    if ($hlIds) {
        $filter = ['@ID' => $hlIds];
        if ($cutoffBxDate instanceof Date) {
            $filter['>=UF_DATE_BEGIN'] = $cutoffBxDate;
        }
        $res = $dataClass::getList([
            'select' => ['ID','UF_CHANGED_AT','UF_EMPLOYEE','UF_STATE','UF_DATE_BEGIN'],
            'filter' => $filter,
        ]);
        while ($x = $res->fetch()) { $hlMap[(int)$x['ID']] = $x; }
    }
    $hlDerivedMap = [];
    if ($derivedPrefixes) {
        $prefFilter = ['@UF_ID_PREFIX' => array_values(array_unique($derivedPrefixes))];
        if ($cutoffBxDate instanceof Date) {
            $prefFilter['>=UF_DATE_BEGIN'] = $cutoffBxDate;
        }
        $resDerived = $dataClass::getList([
            'select' => ['ID','UF_ID_PREFIX','UF_CHANGED_AT','UF_EMPLOYEE','UF_STATE','UF_VACATION_STATE'],
            'filter' => $prefFilter,
        ]);
        while ($x = $resDerived->fetch()) {
            $hlDerivedMap[(string)$x['UF_ID_PREFIX']] = $x;
        }
    }
    foreach ($rows as $row) {
        $cursorRenew = date('Y-m-d H:i:s', strtotime($row['Absence_Renew_Date'] ?: '1900-01-01'));
        $cursorId    = (string)$row['Absence_ID'];
        if (!isBaseAbsenceId((string)$row['Absence_ID'])) {
            $prefixId = (string)$row['Absence_ID'];
            $staffGuid = normalizeGuid((string)($row['Staff_ID'] ?? ''));
            $empId = (int)($allGuidToUserId[$staffGuid] ?? 0);
            if ($empId <= 0) {
                logx("SQL->HL ДОП skip prefix={$prefixId}: employee not found for Staff_ID=".((string)($row['Staff_ID'] ?? '')));
                continue;
            }
            $dateBegin = toBxDate((string)($row['Absence_Date_Start'] ?? ''));
            $vacDays = (int)($row['Absence_Day_Count'] ?? 0);
            if (!$dateBegin || $vacDays <= 0) {
                logx("SQL->HL ДОП skip prefix={$prefixId}: invalid date/days date_begin=".((string)($row['Absence_Date_Start'] ?? '')).", days={$vacDays}");
                continue;
            }
            $dateEnd = calcBxDateEnd($dateBegin, $vacDays);
            $sqlRenew = normalizeSqlDateTime((string)($row['Absence_Renew_Date'] ?? ''));
            $basisId = trim((string)($row['AbsenceBases_ID'] ?? ''));
            if ($basisId === '') {
                logx("SQL->HL ДОП skip prefix={$prefixId}: empty AbsenceBases_ID");
                continue;
            }
            $now = new DateTime();
            $commentTime = $now->format('d.m.Y H:i:s');
            $fields = [
                'UF_ID_PREFIX'         => $prefixId,
                'UF_ABSENCEBASES_ID'   => $basisId,
                'UF_EMPLOYEE'          => $empId,
                'UF_DATE_BEGIN'        => $dateBegin,
                'UF_VACATION_DAYS'     => $vacDays,
                'UF_DATE_END'          => $dateEnd,
                'UF_TYPE'              => DERIVED_VACATION_TYPE_ID,
                'UF_STATE'             => (int)$row['Absence_Status'],
                'UF_ABSENCE_RENEW_DATE'=> $now,
                'UF_VACATION_STATE'    => (int)$row['Absence_State'],
                'UF_VACATION_COMMENT'  => "Создан {$commentTime} из ЗУП на основании отпуска {$basisId}",
            ];
            $existing = $hlDerivedMap[$prefixId] ?? null;
            $hlChanged = null;
            if ($existing && $existing['UF_CHANGED_AT']) {
                $hlChanged = $existing['UF_CHANGED_AT'] instanceof DateTime
                    ? $existing['UF_CHANGED_AT']->format('Y-m-d H:i:s')
                    : date('Y-m-d H:i:s', strtotime((string)$existing['UF_CHANGED_AT']));
            }
            if ($existing && $sqlRenew && $hlChanged && $sqlRenew <= $hlChanged) {
                continue;
            }
            try {
                if ($existing) {
                    $upd = $dataClass::update((int)$existing['ID'], $fields);
                    if (!$upd->isSuccess()) {
                        logx("WARN SQL->HL ДОП update prefix={$prefixId}: ".implode('; ', $upd->getErrorMessages()));
                        continue;
                    }
                    logx(sprintf("SQL->HL ДОП ОБНОВЛЕНИЕ prefix=%s hl_id=%s basis=%s статус=%s состояние=%s",
                        $prefixId,
                        (string)$existing['ID'],
                        $basisId,
                        (string)$fields['UF_STATE'],
                        (string)$fields['UF_VACATION_STATE']
                    ));
                    $hlId = (int)$existing['ID'];
                } else {
                    $add = $dataClass::add($fields);
                    if (!$add->isSuccess()) {
                        logx("WARN SQL->HL ДОП add prefix={$prefixId}: ".implode('; ', $add->getErrorMessages()));
                        continue;
                    }
                    $hlId = (int)$add->getId();
                    $hlDerivedMap[$prefixId] = [
                        'ID' => $hlId,
                        'UF_ID_PREFIX' => $prefixId,
                    ];
                    logx(sprintf("SQL->HL ДОП СОЗДАНИЕ prefix=%s hl_id=%s basis=%s статус=%s состояние=%s",
                        $prefixId,
                        (string)$hlId,
                        $basisId,
                        (string)$fields['UF_STATE'],
                        (string)$fields['UF_VACATION_STATE']
                    ));
                }
                $res2 = $dataClass::getList([
                    'select' => ['UF_CHANGED_AT'],
                    'filter' => ['ID' => $hlId],
                    'limit'  => 1
                ]);
                $real = $res2->fetch();
                if ($real && $real['UF_CHANGED_AT']) {
                    $realTime = $real['UF_CHANGED_AT'] instanceof DateTime
                        ? $real['UF_CHANGED_AT']->format('Y-m-d H:i:s')
                        : date('Y-m-d H:i:s', strtotime((string)$real['UF_CHANGED_AT']));
                    try {
                        syncSqlRenewDate($gateConn, $sqlHelper, $prefixId, $realTime);
                    } catch (\Throwable $e) {
                        logx("WARN SQL->HL ДОП: can't update Absence_Renew_Date for prefix={$prefixId}: ".$e->getMessage());
                    }
                } else {
                    logx("WARN SQL->HL ДОП prefix={$prefixId}: UF_CHANGED_AT not read back after trigger");
                }
                $sql2hlCount++;
                if ($sql2hlCount >= MAX_SQL_UPDATES_PER_RUN) {
                    logx("SQL->HL reached per-run cap: ".MAX_SQL_UPDATES_PER_RUN);
                    break 2;
                }
            } catch (\Throwable $e) {
                logx("ERROR SQL->HL ДОП prefix={$prefixId}: ".$e->getMessage());
            }
            if (microtime(true) - $startedAt > TIME_BUDGET_SEC) {
                logx("Time budget reached inside SQL->HL rows loop");
                break 2;
            }
            continue;
        }
        $id = (int)$row['Absence_ID'];
        if (!isset($hlMap[$id])) continue;
        $empId = (int)$hlMap[$id]['UF_EMPLOYEE'];
        if ($empId <= 0 || !in_array($empId, $activeUserIds, true)) continue;
        $sqlRenew = normalizeSqlDateTime((string)($row['Absence_Renew_Date'] ?? ''));
        $hlCA = $hlMap[$id]['UF_CHANGED_AT'] instanceof DateTime
            ? $hlMap[$id]['UF_CHANGED_AT']->format('Y-m-d H:i:s')
            : ($hlMap[$id]['UF_CHANGED_AT'] ? date('Y-m-d H:i:s', strtotime((string)$hlMap[$id]['UF_CHANGED_AT'])) : null);
        if ($sqlRenew && (!$hlCA || $sqlRenew > $hlCA)) {
            // ИСПРАВЛЕНО: Absence_State → UF_VACATION_STATE
            $upd = [
                'UF_STATE'          => (int)$row['Absence_Status'],     // 4..8
                'UF_VACATION_STATE' => (int)$row['Absence_State'],     // 1909..1916 → UF_VACATION_STATE
            ];
            try {
                logx(sprintf("SQL->HL ОБНОВЛЕНИЕ id=%s статус=%s состояние=%s", (string)$id, (string)$upd['UF_STATE'], (string)$upd['UF_VACATION_STATE']));
                $r = $dataClass::update($id, $upd);
                if ($r->isSuccess()) {
                    // читаем реальный UF_CHANGED_AT после триггера
                    $res2 = $dataClass::getList([
                        'select' => ['UF_CHANGED_AT'],
                        'filter' => ['ID' => $id],
                        'limit'  => 1
                    ]);
                    $real = $res2->fetch();
                    if ($real && $real['UF_CHANGED_AT']) {
                        $realTime = $real['UF_CHANGED_AT'] instanceof DateTime
                            ? $real['UF_CHANGED_AT']->format('Y-m-d H:i:s')
                            : date('Y-m-d H:i:s', strtotime((string)$real['UF_CHANGED_AT']));
                        // пишем это же время в шлюз
                        try {
                            syncSqlRenewDate($gateConn, $sqlHelper, (string)$id, $realTime);
                        } catch (\Throwable $e) {
                            logx("WARN SQL->HL: can't update Absence_Renew_Date for HL#{$id}: ".$e->getMessage());
                        }
                    } else {
                        logx("WARN SQL->HL HL#{$id}: UF_CHANGED_AT not read back after trigger");
                    }
                    $sql2hlCount++;
                    if ($sql2hlCount >= MAX_SQL_UPDATES_PER_RUN) {
                        logx("SQL->HL reached per-run cap: ".MAX_SQL_UPDATES_PER_RUN);
                        break 2;
                    }
                } else {
                    logx("WARN SQL->HL HL#{$id}: ".implode('; ', $r->getErrorMessages()));
                }
            } catch (\Throwable $e) {
                logx("ERROR SQL->HL HL#{$id}: ".$e->getMessage());
            }
        }
        if (microtime(true) - $startedAt > TIME_BUDGET_SEC) {
            logx("Time budget reached inside SQL->HL rows loop");
            break 2;
        }
    }
} while (true);
// save cursor & finish
saveSqlHlCursor($cursorRenew, $cursorId);
$elapsed = round(microtime(true) - $startedAt, 3);
logx("=== Test sync done: HL->SQL={$hl2sqlCount}, SQL->HL={$sql2hlCount}, elapsed={$elapsed}s ===");
echo "OK v1.6.4-fix; cutoff=".($cutoffYmd?:'none')."; HL->SQL={$hl2sqlCount}; SQL->HL={$sql2hlCount}; elapsed={$elapsed}s";
