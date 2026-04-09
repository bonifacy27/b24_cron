<?php
/**
 * schedules_weekly_automation.php
 * Work schedules weekly automation
 * Версия: 1.3
 *
 * Доработки v1.2 (исключение ложных срабатываний):
 *  - Строго ограничили целевых пользователей:
 *      1) пользователь состоит в группе ID=46 (устойчиво к строковым ID групп)
 *      2) UF_WORK_FORMAT = "Комбинированный" (enum)
 *      3) UF_PLANNING_PERIOD = "еженедельно" (enum)
 *  - Enum ID получаем по VALUE (нормализованное сравнение: регистр/пробелы), с fallback на константы (1931/1932),
 *    чтобы скрипт был переносимее и не "стрелял" на порталах, где enum ID отличается.
 *  - Даже если CUser::GetList вернул лишних пользователей (бывает на некоторых порталах),
 *    выполняем вторичную строгую проверку на каждом пользователе и пропускаем несоответствующих.
 *
 * Для сотрудников с еженедельным планированием:
 *  - Вторник 10:00: уведомление
 *  - Четверг 10:00: напоминание
 *  - Четверг 19:00: автозаполнение (Офис + Автозаполнение) на следующую неделю (не выходные, не отпуск) + письмо
 *  - После автозаполнения пишем запись в HL-блок изменений графика (ENTITY_ID=97)
 *
 * HL планирования: ENTITY_ID=91
 * Статусы дня (HL92): Корректировка=1, Автозаполнение=4
 * Тип присутствия (HL93): Офис=1
 * HL изменений графика работы: ENTITY_ID=97
 *
 * Эмуляция времени:
 *   php schedules_weekly_automation.php "2026-02-03 10:00:00"
 */

use Bitrix\Main\Loader;
use Bitrix\Main\Type\DateTime;
use Bitrix\Main\Type\Date;
use Bitrix\Highloadblock as HL;

define("NO_KEEP_STATISTIC", true);
define("NOT_CHECK_PERMISSIONS", true);
define("BX_CRONTAB", true);
define("BX_NO_ACCELERATOR_RESET", true);

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

// ===================== НАСТРОЙКИ =====================
const HL_PLAN_ENTITY_ID = 91;
const HL_CHANGES_ENTITY_ID = 97;

const STATUS_KORREKTIROVKA_ID = 1;
const STATUS_AUTOZAPOLNENIE_ID = 4;

const PRESENCE_OFFICE_ID = 1;

// Ограничения целевой аудитории
const REQUIRED_GROUP_ID = 46;

// Fallback enum ID (если не удалось найти по VALUE)
const FALLBACK_WORK_FORMAT_COMBINED_ID = 1931;  // "Комбинированный"
const FALLBACK_PLANNING_PERIOD_WEEKLY_ID = 1932; // "еженедельно"

// --- ТЕСТ/ОТЛАДКА ---
$DRY_RUN = false;                 // true = ничего не меняем, только логируем/имитируем отправку писем
$TIME_TOLERANCE_MINUTES = 2;       // допуск по времени (в минутах) для попадания в окно 10:00 / 19:00
$FORCE_NOW = null;                // задаётся через CLI аргументом "Y-m-d H:i:s"
//$FORCE_NOW = "2026-02-05 19:00:00"; 
// Ограничение пользователями (для теста)
$TEST_ONLY_USER_ID = null;
$TEST_ONLY_USER_IDS = [];          // для всех
//$TEST_ONLY_USER_IDS = [3532];     // для теста,например [6202, 2648, 2923]
$TEST_MAIL_TO = null;              // для всех
//$TEST_MAIL_TO = 'lobachav@tricolor.ru'; // для теста
// =====================================================

/** ---------- ЛОГИ ---------- */
$logDir = $_SERVER["DOCUMENT_ROOT"] . "/upload/logs";
if (!is_dir($logDir)) {
	@mkdir($logDir, 0775, true);
}
$logFile = $logDir . "/work_schedule_weekly_automation_" . date('Ymd') . ".log";

function logLine(string $msg): void
{
	global $logFile;
	$ts = date('Y-m-d H:i:s');
	file_put_contents($logFile, "[$ts] $msg\n", FILE_APPEND);
}

/** ---------- STRING NORMALIZE ---------- */
function normStr($s): string
{
	$s = (string)$s;
	$s = trim($s);
	$s = mb_strtolower($s);
	$s = preg_replace('/\s+/u', ' ', $s);
	return $s;
}

/** ---------- GROUP CHECK (устойчиво к строковым ID групп) ---------- */
function isUserInGroup(int $userId, int $groupId): bool
{
	$groups = \CUser::GetUserGroup($userId); // часто: ["46","3",...]
	foreach ($groups as $g) {
		if ((int)$g === (int)$groupId) {
			return true;
		}
	}
	return false;
}

/** ---------- HL HELPERS ---------- */
function getHlDataClassByEntityId(int $entityId): string
{
	if (!Loader::includeModule('highloadblock')) {
		throw new \RuntimeException("Не подключен модуль highloadblock");
	}

	$hlblock = HL\HighloadBlockTable::getList([
		'filter' => ['=ID' => $entityId],
	])->fetch();

	if (!$hlblock) {
		throw new \RuntimeException("HL-блок не найден: ENTITY_ID={$entityId}");
	}

	$entity = HL\HighloadBlockTable::compileEntity($hlblock);
	return $entity->getDataClass();
}

/** ---------- USER FIELD ENUM HELPERS ---------- */
function getUserEnumIdExact(string $userFieldName, string $value): ?int
{
	static $cache = [];
	$key = $userFieldName . '|exact|' . $value;
	if (array_key_exists($key, $cache)) {
		return $cache[$key];
	}

	$enumId = null;
	$rs = \CUserFieldEnum::GetList(
		["SORT" => "ASC"],
		["USER_FIELD_NAME" => $userFieldName]
	);

	while ($row = $rs->Fetch()) {
		if ((string)$row['VALUE'] === (string)$value) {
			$enumId = (int)$row['ID'];
			break;
		}
	}

	$cache[$key] = $enumId;
	return $enumId;
}

function getUserEnumIdNormalized(string $userFieldName, string $value): ?int
{
	static $cache = [];
	$key = $userFieldName . '|norm|' . normStr($value);
	if (array_key_exists($key, $cache)) {
		return $cache[$key];
	}

	$target = normStr($value);
	$enumId = null;

	$rs = \CUserFieldEnum::GetList(
		["SORT" => "ASC"],
		["USER_FIELD_NAME" => $userFieldName]
	);

	while ($row = $rs->Fetch()) {
		if (normStr($row['VALUE']) === $target) {
			$enumId = (int)$row['ID'];
			break;
		}
	}

	$cache[$key] = $enumId;
	return $enumId;
}

/** ---------- USERS (строгая выборка + повторная проверка) ---------- */
function getTargetUsersStrict(int $workFormatEnumId, int $planningPeriodEnumId, int $requiredGroupId): array
{
	$users = [];

	// Пытаемся сузить выборку фильтром. Но даже если вернёт лишних — ниже строгая проверка всё отфильтрует.
	$rs = \CUser::GetList(
		($by = "ID"),
		($order = "ASC"),
		[
			"ACTIVE" => "Y",
			"GROUPS_ID" => [$requiredGroupId],
			"UF_WORK_FORMAT" => $workFormatEnumId,
			"UF_PLANNING_PERIOD" => $planningPeriodEnumId,
		],
		[
			"FIELDS" => ["ID", "NAME", "LAST_NAME", "SECOND_NAME", "EMAIL", "LOGIN", "ACTIVE"],
			"SELECT" => ["UF_WORK_FORMAT", "UF_PLANNING_PERIOD"]
		]
	);

	while ($u = $rs->Fetch()) {
		$userId = (int)$u['ID'];

		// строгая проверка группы (устойчиво к строковым ID)
		if (!isUserInGroup($userId, $requiredGroupId)) {
			continue;
		}

		// строгая проверка UF полей
		$rawWork = $u['UF_WORK_FORMAT'] ?? null;
		$rawPlan = $u['UF_PLANNING_PERIOD'] ?? null;

		if ((int)$rawWork !== (int)$workFormatEnumId) {
			continue;
		}
		if ((int)$rawPlan !== (int)$planningPeriodEnumId) {
			continue;
		}

		$users[] = $u;
	}

	return $users;
}

/** ---------- NEXT WEEK RANGE (Mon..Sun) ---------- */
function getNextWeekRange(Date $anyNowDate): array
{
	$dt = \DateTime::createFromFormat('d.m.Y', $anyNowDate->format('d.m.Y'));
	$dt->setTime(0, 0, 0);

	// Текущая неделя: понедельник
	$weekday = (int)$dt->format('N'); // 1=Mon..7=Sun
	$mondayThisWeek = (clone $dt)->modify('-' . ($weekday - 1) . ' days');

	// Следующая неделя
	$mondayNextWeek = (clone $mondayThisWeek)->modify('+7 days');
	$sundayNextWeek = (clone $mondayNextWeek)->modify('+6 days');

	$from = new Date($mondayNextWeek->format('d.m.Y'), 'd.m.Y');
	$to = new Date($sundayNextWeek->format('d.m.Y'), 'd.m.Y');

	return [$from, $to];
}

/** ---------- DIAGNOSTIC + TARGET DATES (next week) ---------- */
function getUserNextWeekDiagnostics(int $userId, Date $anyNowDate, string $planDataClass): array
{
	[$from, $to] = getNextWeekRange($anyNowDate);

	$all = $planDataClass::getList([
		'select' => ['ID','UF_DATE','UF_STATUS','UF_WEEKEND','UF_VACATION'],
		'filter' => [
			'=UF_USER' => $userId,
			'>=UF_DATE' => $from,
			'<=UF_DATE' => $to,
		],
		'order' => ['UF_DATE' => 'ASC'],
	])->fetchAll();

	$korTargetDates = [];

	foreach ($all as $r) {
		$status = (int)$r['UF_STATUS'];
		if ($status !== STATUS_KORREKTIROVKA_ID) {
			continue;
		}

		$isWeekend  = (string)$r['UF_WEEKEND'] === '1' || (int)$r['UF_WEEKEND'] === 1;
		$isVacation = (string)$r['UF_VACATION'] === '1' || (int)$r['UF_VACATION'] === 1;
		if ($isWeekend || $isVacation) {
			continue;
		}

		$d = ($r['UF_DATE'] instanceof Date) ? $r['UF_DATE']->format('d.m.Y') : (string)$r['UF_DATE'];
		$korTargetDates[] = $d;
	}

	return [
		'targetDates' => $korTargetDates,
		'range_from' => $from->format('d.m.Y'),
		'range_to' => $to->format('d.m.Y'),
		'rows' => count($all),
	];
}

/** ---------- DATE FORMATTING ---------- */
function formatDatesHumanWeekly(array $datesDdMmYyyy): string
{
	// Требование: вывод периодами, примеры:
	//  - 1.02 - 5.02
	//  - 1.03, 4-5.03
	$items = [];
	foreach ($datesDdMmYyyy as $d) {
		$dt = \DateTime::createFromFormat('d.m.Y', (string)$d);
		if ($dt) $items[] = $dt;
	}
	if (!$items) return '';

	usort($items, fn(\DateTime $a, \DateTime $b) => $a <=> $b);

	// unique by date
	$uniq = [];
	foreach ($items as $dt) {
		$uniq[$dt->format('Y-m-d')] = $dt;
	}
	$items = array_values($uniq);
	usort($items, fn(\DateTime $a, \DateTime $b) => $a <=> $b);

	$ranges = [];
	$start = $items[0];
	$prev = $items[0];

	for ($i = 1; $i < count($items); $i++) {
		$cur = $items[$i];
		$prevPlus = (clone $prev)->modify('+1 day');
		if ($cur->format('Y-m-d') === $prevPlus->format('Y-m-d')) {
			$prev = $cur;
			continue;
		}
		$ranges[] = [$start, $prev];
		$start = $cur;
		$prev = $cur;
	}
	$ranges[] = [$start, $prev];

	$parts = [];
	foreach ($ranges as [$s, $e]) {
		if ($s->format('Y-m-d') === $e->format('Y-m-d')) {
			$parts[] = (int)$s->format('j') . '.' . $s->format('m');
			continue;
		}

		// если месяц одинаковый: 4-5.03
		if ($s->format('Y-m') === $e->format('Y-m')) {
			$parts[] = (int)$s->format('j') . '-' . (int)$e->format('j') . '.' . $e->format('m');
		} else {
			// если пересекаем месяц: 30.01 - 2.02
			$parts[] = (int)$s->format('j') . '.' . $s->format('m') . ' - ' . (int)$e->format('j') . '.' . $e->format('m');
		}
	}

	return implode(', ', $parts);
}

function formatDatesRangesDdMm(array $datesDdMmYyyy): string
{
	// Для HL-лога: dd.mm диапазоны с ведущими нулями (как в месячном скрипте)
	$items = [];
	foreach ($datesDdMmYyyy as $d) {
		$dt = \DateTime::createFromFormat('d.m.Y', (string)$d);
		if ($dt) $items[] = $dt;
	}
	if (!$items) return '';

	usort($items, fn(\DateTime $a, \DateTime $b) => $a <=> $b);

	$days = [];
	foreach ($items as $dt) {
		$days[$dt->format('Y-m-d')] = $dt;
	}
	$items = array_values($days);
	usort($items, fn(\DateTime $a, \DateTime $b) => $a <=> $b);

	$ranges = [];
	$start = $items[0];
	$prev = $items[0];

	for ($i = 1; $i < count($items); $i++) {
		$cur = $items[$i];
		$prevPlus = (clone $prev)->modify('+1 day');
		if ($cur->format('Y-m-d') === $prevPlus->format('Y-m-d')) {
			$prev = $cur;
			continue;
		}
		$ranges[] = [$start, $prev];
		$start = $cur;
		$prev = $cur;
	}
	$ranges[] = [$start, $prev];

	$parts = [];
	foreach ($ranges as [$s, $e]) {
		$from = $s->format('d.m');
		$to = $e->format('d.m');
		$parts[] = ($s->format('Y-m-d') === $e->format('Y-m-d')) ? $from : ($from . '–' . $to);
	}

	return implode(', ', $parts);
}

function buildHlAutoOfficeMessage(array $datesDdMmYyyy): string
{
	$ddmmRanges = formatDatesRangesDdMm($datesDdMmYyyy);
	return 'Автозаполнено: Офис (' . $ddmmRanges . ');  ';
}

/** ---------- HL CHANGES LOG ---------- */
function addScheduleChangeLog(int $userId, string $message, string $eventType, bool $dryRun): bool
{
	$now = new DateTime();

	logLine("HL97 LOG: user_id={$userId}; dry_run=" . ($dryRun ? 'Y' : 'N') . "; event_type={$eventType}; message={$message}");

	if ($dryRun) {
		return true;
	}

	$changesDataClass = getHlDataClassByEntityId(HL_CHANGES_ENTITY_ID);

	$addRes = $changesDataClass::add([
		'UF_USER' => $userId,
		'UF_DATE' => $now,
		'UF_MESSAGE' => $message,
		'UF_UPDATED_AT' => $now,
		'UF_CREATED_AT' => $now,
		'UF_EVENT_TYPE' => $eventType,
	]);

	if (method_exists($addRes, 'isSuccess') && !$addRes->isSuccess()) {
		$errs = method_exists($addRes, 'getErrorMessages') ? implode('; ', $addRes->getErrorMessages()) : 'unknown error';
		logLine("HL97 LOG ERROR: user_id={$userId}; errors={$errs}");
		return false;
	}

	$logId = method_exists($addRes, 'getId') ? (int)$addRes->getId() : 0;
	logLine("HL97 LOG OK: id={$logId}; user_id={$userId}");
	return true;
}

/** ---------- NORMALIZE DATE ---------- */
function normalizeToYmd($value): ?string
{
	if ($value instanceof \Bitrix\Main\Type\Date) {
		return $value->format('Y-m-d');
	}

	$s = trim((string)$value);
	if ($s === '') return null;

	if (preg_match('~^\d{4}-\d{2}-\d{2}$~', $s)) return $s;

	$dt = \DateTime::createFromFormat('d.m.Y', $s);
	if ($dt) return $dt->format('Y-m-d');

	$dt = \DateTime::createFromFormat('d.m.Y H:i:s', $s);
	if ($dt) return $dt->format('Y-m-d');

	$dt = \DateTime::createFromFormat('Y-m-d H:i:s', $s);
	if ($dt) return $dt->format('Y-m-d');

	return null;
}

function normalizeToDmy($value): ?string
{
	if ($value instanceof \Bitrix\Main\Type\Date) {
		return $value->format('d.m.Y');
	}

	$s = trim((string)$value);
	if ($s === '') return null;

	if (preg_match('~^\d{2}\.\d{2}\.\d{4}$~', $s)) return $s;

	$dt = \DateTime::createFromFormat('Y-m-d', $s);
	if ($dt) return $dt->format('d.m.Y');

	$dt = \DateTime::createFromFormat('Y-m-d H:i:s', $s);
	if ($dt) return $dt->format('d.m.Y');

	$dt = \DateTime::createFromFormat('d.m.Y H:i:s', $s);
	if ($dt) return $dt->format('d.m.Y');

	return null;
}

/** ---------- UPDATE HL (ROBUST) ---------- */
function updateUserPlanToOfficeAuto(int $userId, string $planDataClass, array $datesDdMmYyyy, bool $dryRun): array
{
	if (!$datesDdMmYyyy) {
		return ['updated' => 0, 'ids' => [], 'dates' => []];
	}

	$needYmd = [];
	$needDmy = [];
	foreach ($datesDdMmYyyy as $d) {
		$y = normalizeToYmd($d);
		$m = normalizeToDmy($d);
		if ($y) $needYmd[$y] = true;
		if ($m) $needDmy[$m] = true;
	}

	$rows = $planDataClass::getList([
		'select' => ['ID', 'UF_DATE', 'UF_STATUS', 'UF_WEEKEND', 'UF_VACATION'],
		'filter' => [
			'=UF_USER' => $userId,
			'=UF_STATUS' => STATUS_KORREKTIROVKA_ID,
		],
		'order' => ['UF_DATE' => 'ASC'],
	])->fetchAll();

	$updatedIds = [];
	$updatedDates = [];
	$now = new DateTime();

	foreach ($rows as $r) {
		$isWeekend  = (string)$r['UF_WEEKEND'] === '1' || (int)$r['UF_WEEKEND'] === 1;
		$isVacation = (string)$r['UF_VACATION'] === '1' || (int)$r['UF_VACATION'] === 1;
		if ($isWeekend || $isVacation) continue;

		$ymd = normalizeToYmd($r['UF_DATE']);
		$dmy = normalizeToDmy($r['UF_DATE']);

		$match = false;
		if ($ymd && isset($needYmd[$ymd])) $match = true;
		if ($dmy && isset($needDmy[$dmy])) $match = true;
		if (!$match) continue;

		if ($dryRun) {
			$updatedIds[] = (int)$r['ID'];
			if ($dmy) $updatedDates[] = $dmy;
			continue;
		}

		$updateRes = $planDataClass::update((int)$r['ID'], [
			'UF_PRESENCE_TYPE' => PRESENCE_OFFICE_ID,
			'UF_STATUS' => STATUS_AUTOZAPOLNENIE_ID,
			'UF_APPROVED_AT' => $now,
			'UF_IS_APPROVED_AUTO' => 1,
		]);

		if (method_exists($updateRes, 'isSuccess') && !$updateRes->isSuccess()) {
			$errs = method_exists($updateRes, 'getErrorMessages') ? implode('; ', $updateRes->getErrorMessages()) : 'unknown error';
			logLine("AUTO UPDATE ERROR: row_id={$r['ID']}; ymd={$ymd}; dmy={$dmy}; errors={$errs}");
			continue;
		}

		$updatedIds[] = (int)$r['ID'];
		if ($dmy) $updatedDates[] = $dmy;
	}

	$updatedDates = array_values(array_unique($updatedDates));
	sort($updatedDates);
	return ['updated' => count($updatedIds), 'ids' => $updatedIds, 'dates' => $updatedDates];
}

/** ---------- EMAIL ---------- */
function sendMail(string $realTo, string $subject, string $body, bool $dryRun, ?string $forceTo = null): bool
{
	$to = $forceTo ?: $realTo;

	logLine("MAIL to={$to}; real_to={$realTo}; subject={$subject}; dry_run=" . ($dryRun ? 'Y' : 'N'));

	if ($dryRun) {
		return true;
	}

	$defaultFrom = \Bitrix\Main\Config\Option::get("main", "email_from", "no-reply@localhost");

	return \Bitrix\Main\Mail\Mail::send([
		'TO' => $to,
		'FROM' => $defaultFrom,
		'SUBJECT' => $subject,
		'BODY' => $body,
		'CONTENT_TYPE' => 'text/plain; charset=UTF-8',
		'HEADER' => [],
	]);
}

/** ---------- MAIN ---------- */
try {
	global $DRY_RUN, $TIME_TOLERANCE_MINUTES, $FORCE_NOW, $TEST_ONLY_USER_ID, $TEST_ONLY_USER_IDS, $TEST_MAIL_TO;

	if (PHP_SAPI === 'cli' && isset($argv[1]) && trim($argv[1]) !== '') {
		$FORCE_NOW = trim($argv[1]);
	}

	$now = $FORCE_NOW ? new DateTime($FORCE_NOW, 'Y-m-d H:i:s') : new DateTime();
	$nowDate = new Date($now->format('d.m.Y'), 'd.m.Y');
	$hm = $now->format('H:i');
	$weekdayN = (int)$now->format('N'); // 1=Mon..7=Sun

	logLine("=== START v1.3; DRY_RUN=" . ($DRY_RUN ? 'Y' : 'N') .
		"; FORCE_NOW=" . ($FORCE_NOW ?: 'NULL') .
		"; TEST_ONLY_USER_ID=" . ($TEST_ONLY_USER_ID !== null ? $TEST_ONLY_USER_ID : 'NULL') .
		"; TEST_ONLY_USER_IDS=" . (is_array($TEST_ONLY_USER_IDS) ? implode(',', $TEST_ONLY_USER_IDS) : 'NULL') .
		"; TEST_MAIL_TO=" . ($TEST_MAIL_TO ?: 'NULL') .
		"; NOW={$now->format('Y-m-d H:i:s')} ===");

	// Получаем enum ID (предпочтительно — по VALUE, нормализованно)
	$workFormatId = getUserEnumIdNormalized('UF_WORK_FORMAT', 'Комбинированный');
	if (!$workFormatId) {
		// fallback на точное (на всякий случай)
		$workFormatId = getUserEnumIdExact('UF_WORK_FORMAT', 'Комбинированный');
	}
	if (!$workFormatId) {
		$workFormatId = FALLBACK_WORK_FORMAT_COMBINED_ID;
		logLine("WARNING: UF_WORK_FORMAT 'Комбинированный' enumId not found by VALUE. Using fallback ID=" . $workFormatId);
	}

	$planningWeeklyId = getUserEnumIdNormalized('UF_PLANNING_PERIOD', 'еженедельно');
	if (!$planningWeeklyId) {
		$planningWeeklyId = getUserEnumIdExact('UF_PLANNING_PERIOD', 'еженедельно');
	}
	if (!$planningWeeklyId) {
		$planningWeeklyId = FALLBACK_PLANNING_PERIOD_WEEKLY_ID;
		logLine("WARNING: UF_PLANNING_PERIOD 'еженедельно' enumId not found by VALUE. Using fallback ID=" . $planningWeeklyId);
	}

	$planDataClass = getHlDataClassByEntityId(HL_PLAN_ENTITY_ID);

	$inTolerance = function(string $targetHm) use ($TIME_TOLERANCE_MINUTES, $now): bool {
		$cur = (int)$now->format('H') * 60 + (int)$now->format('i');
		[$th, $tm] = explode(':', $targetHm);
		$tar = (int)$th * 60 + (int)$tm;
		return abs($cur - $tar) <= $TIME_TOLERANCE_MINUTES;
	};

	// Определяем режим по дню недели и времени
	$runMode = null;
	if ($weekdayN === 2 && $inTolerance('10:00')) $runMode = 'MAIL_TUE_10';         // вторник
	elseif ($weekdayN === 4 && $inTolerance('10:00')) $runMode = 'MAIL_THU_10';     // четверг
	elseif ($weekdayN === 4 && $inTolerance('19:00')) $runMode = 'AUTO_THU_19';     // четверг

	if (!$runMode) {
		logLine("No actions for current moment. WeekdayN={$weekdayN}; CurrentHM={$hm}; Exit.");
		logLine("=== END ===");
		exit;
	}

	// Дедлайн — четверг текущей недели (конец дня)
	$deadlineDt = \DateTime::createFromFormat('Y-m-d H:i:s', $now->format('Y-m-d H:i:s'));
	$addDaysToThu = 4 - (int)$deadlineDt->format('N'); // Tue: +2, Thu:+0
	if ($addDaysToThu > 0) {
		$deadlineDt->modify('+' . $addDaysToThu . ' days');
	}
	$deadlineHuman = $deadlineDt->format('d.m.Y');

	logLine("RUN MODE: {$runMode}; deadlineHuman={$deadlineHuman}");
	logLine("TARGET FILTER: group=" . REQUIRED_GROUP_ID . "; workFormatEnumId={$workFormatId}; planningWeeklyEnumId={$planningWeeklyId}");

	// Строго выбираем пользователей
	$users = getTargetUsersStrict($workFormatId, $planningWeeklyId, REQUIRED_GROUP_ID);
	logLine("Target users found (weekly, strict): " . count($users));

	$processed = 0;
	$notified = 0;
	$autoUpdatedUsers = 0;

	foreach ($users as $u) {
		$processed++;
		$userId = (int)$u['ID'];
		$login = (string)($u['LOGIN'] ?? '');
		$email = trim((string)$u['EMAIL']);
		$fio = trim(($u['LAST_NAME'] ?? '') . ' ' . ($u['NAME'] ?? '') . ' ' . ($u['SECOND_NAME'] ?? ''));

		// ограничение одним пользователем (тест)
		if ($TEST_ONLY_USER_ID !== null && $userId !== (int)$TEST_ONLY_USER_ID) {
			continue;
		}
		// ограничение списком пользователей (тест)
		if (is_array($TEST_ONLY_USER_IDS) && count($TEST_ONLY_USER_IDS) > 0 && !in_array($userId, $TEST_ONLY_USER_IDS, true)) {
			continue;
		}

		// Доп. защитная строгая проверка (на случай неожиданных данных)
		if (!isUserInGroup($userId, REQUIRED_GROUP_ID)) {
			logLine("User#{$userId} {$fio}: SKIP (not in group " . REQUIRED_GROUP_ID . ")");
			continue;
		}
		$rawWork = $u['UF_WORK_FORMAT'] ?? null;
		$rawPlan = $u['UF_PLANNING_PERIOD'] ?? null;
		if ((int)$rawWork !== (int)$workFormatId) {
			logLine("User#{$userId} {$fio}: SKIP (UF_WORK_FORMAT mismatch; raw={$rawWork}, expected={$workFormatId})");
			continue;
		}
		if ((int)$rawPlan !== (int)$planningWeeklyId) {
			logLine("User#{$userId} {$fio}: SKIP (UF_PLANNING_PERIOD mismatch; raw={$rawPlan}, expected={$planningWeeklyId})");
			continue;
		}

		logLine("USER CHECK: ID={$userId}; LOGIN={$login}; EMAIL={$email}; FIO={$fio}; UF_WORK_FORMAT={$rawWork}; UF_PLANNING_PERIOD={$rawPlan}; GROUP46=Y");

		if ($email === '') {
			logLine("User#{$userId} {$fio}: skip, empty email");
			continue;
		}

		$res = getUserNextWeekDiagnostics($userId, $nowDate, $planDataClass);
		$targetDates = $res['targetDates'];

		logLine("User#{$userId} {$fio}: nextWeekRange {$res['range_from']}..{$res['range_to']}; rows={$res['rows']}; targetDates=" . count($targetDates));

		if (!$targetDates) {
			continue;
		}

		$datesHuman = formatDatesHumanWeekly($targetDates);
		logLine("User#{$userId} {$fio}: TARGET nextWeek dates for notify/auto = {$datesHuman}");

		if ($runMode === 'MAIL_TUE_10') {
			$subject = 'Планирование графика на следующую неделю';
			$body =
				"Коллега, добрый день!\n\n" .
				"Вам необходимо составить график работы на следующую неделю ({$datesHuman}) и отправить его на согласование руководителю до конца дня {$deadlineHuman}.\n" .
				"Если график в указанный срок не будет составлен и направлен на согласование, в него автоматически загрузятся офисные дни.\n\n" .
				"Ссылка на график: https://ourtricolortv.nsc.ru/schedules/my/\n";

			$ok = sendMail($email, $subject, $body, $DRY_RUN, $TEST_MAIL_TO);
			$notified += $ok ? 1 : 0;
		}
		elseif ($runMode === 'MAIL_THU_10') {
			$subject = 'Планирование графика на следующую неделю';
			$body =
				"Коллега, добрый день!\n\n" .
				"Напоминаем, Вам необходимо составить график работы на следующую неделю ({$datesHuman}) и отправить его на согласование руководителю до конца дня {$deadlineHuman}.\n" .
				"Если график в указанный срок не будет составлен и направлен на согласование, в него автоматически загрузятся офисные дни.\n\n" .
				"Ссылка на график: https://ourtricolortv.nsc.ru/schedules/my/\n";

			$ok = sendMail($email, $subject, $body, $DRY_RUN, $TEST_MAIL_TO);
			$notified += $ok ? 1 : 0;
		}
		elseif ($runMode === 'AUTO_THU_19') {
			$upd = updateUserPlanToOfficeAuto($userId, $planDataClass, $targetDates, $DRY_RUN);
			logLine("User#{$userId} {$fio}: AUTO updated={$upd['updated']}; ids=" . implode(',', $upd['ids']) . "; dates=" . implode(',', $upd['dates']));

			if ($upd['updated'] > 0) {
				$autoUpdatedUsers++;

				// Лог в HL97
				$eventType = 'cron: schedules_weekly_automation.php';
				$hlMsg = buildHlAutoOfficeMessage($upd['dates'] ?: $targetDates);
				addScheduleChangeLog($userId, $hlMsg, $eventType, $DRY_RUN);

				$subject = 'График работы на следующую неделю заполнен автоматически';
				$body =
					"Коллега, добрый день!\n\n" .
					"Вами не был составлен график работы на следующую неделю ({$datesHuman}), поэтому на этот период проставлены офисные дни. " .
					"Вы можете скорректировать график и отправить его на согласование руководителю. Обращаем внимание, что прошедшие периоды не подлежат корректировке.\n\n" .
					"Ссылка на график: https://ourtricolortv.nsc.ru/schedules/my/\n";

				$ok = sendMail($email, $subject, $body, $DRY_RUN, $TEST_MAIL_TO);
				$notified += $ok ? 1 : 0;
			}
		}
	}

	logLine("SUMMARY: processed={$processed}; notified={$notified}; autoUpdatedUsers={$autoUpdatedUsers}; mode={$runMode}");
	logLine("=== END ===");
}
catch (\Throwable $e) {
	logLine("ERROR: " . $e->getMessage());
	logLine($e->getTraceAsString());
}