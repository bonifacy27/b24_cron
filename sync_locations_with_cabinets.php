<?php
/**
 * sync_locations_with_cabinets.php
 *
 * Аккуратная синхронизация справочника местоположений (список/IBLOCK_ID=224)
 * со справочником кабинетов (HL-блок/ENTITY_ID=74).
 *
 * Что делает:
 *  - связывает элементы местоположений с кабинетами через числовое свойство списка CABINET_HL_ID (PROPERTY_3152);
 *  - переименовывает связанные местоположения, если NAME отличается от UF_NAME кабинета;
 *  - для ещё не связанных местоположений пытается найти кабинет по нормализованному названию
 *    (в том числе поддерживает старый формат "адрес|каб. N" == "адрес, каб. N");
 *  - добавляет недостающие местоположения из справочника кабинетов;
 *  - деактивирует местоположения, связанные с отсутствующим кабинетом, вместо физического удаления.
 *
 * Перед запуском создайте в списке местоположений пользовательское поле/свойство:
 *  - ID свойства: 3152
 *  - Символьный код: CABINET_HL_ID
 *  - Тип: число
 *  - Назначение: ID элемента HL-блока кабинетов (ENTITY_ID=74)
 *
 * Примеры запуска:
 *  php sync_locations_with_cabinets.php --dry-run
 *  php sync_locations_with_cabinets.php
 */

use Bitrix\Highloadblock as HL;
use Bitrix\Main\Loader;

$_SERVER['DOCUMENT_ROOT'] = '/home/bitrix/www';
$DOCUMENT_ROOT = $_SERVER['DOCUMENT_ROOT'];

define('NO_KEEP_STATISTIC', true);
define('NOT_CHECK_PERMISSIONS', true);
define('BX_NO_ACCELERATOR_RESET', true);
define('BX_CRONTAB', true);

require($_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/main/include/prolog_before.php');

const LOCATION_LIST_IBLOCK_ID = 224;
const CABINET_HL_BLOCK_ID = 74;
const LINK_PROPERTY_ID = 3152;
const LINK_PROPERTY_CODE = 'CABINET_HL_ID';

const ADMIN_USER_ID = 1;
const LOG_FILE = '/home/bitrix/www/upload/logs/sync_locations_with_cabinets.log';
const LOCK_FILE = '/home/bitrix/www/upload/logs/sync_locations_with_cabinets.lock';

// Физическое удаление опасно, т.к. справочник уже используется внешними системами.
// Поэтому "лишние" связанные элементы по умолчанию только деактивируются.
const DELETE_OBSOLETE_LOCATIONS = false;

$dryRun = in_array('--dry-run', $argv ?? [], true);

function logMessage(string $message): void
{
    $dir = dirname(LOG_FILE);
    if (!is_dir($dir)) {
        @mkdir($dir, 0775, true);
    }

    $line = '[' . date('Y-m-d H:i:s') . '] ' . $message . PHP_EOL;
    file_put_contents(LOG_FILE, $line, FILE_APPEND | LOCK_EX);
    echo $line;
}

function stopWithLog(string $message, int $code = 1): void
{
    logMessage($message);
    exit($code);
}

function createLock(): void
{
    if (file_exists(LOCK_FILE)) {
        $pid = (int)file_get_contents(LOCK_FILE);
        if ($pid > 0 && function_exists('posix_kill') && posix_kill($pid, 0)) {
            stopWithLog('Script already running. PID=' . $pid);
        }
        @unlink(LOCK_FILE);
    }

    file_put_contents(LOCK_FILE, (string)getmypid(), LOCK_EX);
    register_shutdown_function(static function (): void {
        if (file_exists(LOCK_FILE)) {
            @unlink(LOCK_FILE);
        }
    });
}

function ensureModulesLoaded(): void
{
    foreach (['iblock', 'highloadblock'] as $module) {
        if (!Loader::includeModule($module)) {
            throw new RuntimeException('Не подключен модуль ' . $module);
        }
    }
}

function ensureAdminAuthorized(): void
{
    global $USER;
    if (is_object($USER) && method_exists($USER, 'Authorize')) {
        $USER->Authorize(ADMIN_USER_ID);
    }
}

function getHlDataClass(int $hlBlockId): string
{
    $hlBlock = HL\HighloadBlockTable::getById($hlBlockId)->fetch();
    if (!$hlBlock) {
        throw new RuntimeException('HL-блок не найден: ID=' . $hlBlockId);
    }

    return HL\HighloadBlockTable::compileEntity($hlBlock)->getDataClass();
}

function getLinkProperty(): array
{
    $property = CIBlockProperty::GetList([], [
        'IBLOCK_ID' => LOCATION_LIST_IBLOCK_ID,
        'ID' => LINK_PROPERTY_ID,
    ])->Fetch();

    if (!$property) {
        throw new RuntimeException(
            'В списке местоположений IBLOCK_ID=' . LOCATION_LIST_IBLOCK_ID
            . ' не найдено свойство PROPERTY_' . LINK_PROPERTY_ID
            . ' для хранения ID элемента HL-блока кабинетов.'
        );
    }

    if ((string)($property['CODE'] ?? '') !== LINK_PROPERTY_CODE) {
        logMessage(
            'WARN link property ID=' . LINK_PROPERTY_ID
            . ' has CODE=' . (string)($property['CODE'] ?? '')
            . ', expected CODE=' . LINK_PROPERTY_CODE
        );
    }

    return $property;
}

function normalizeLocationName(string $value): string
{
    $value = trim($value);
    $value = str_replace('|', ',', $value);
    $value = preg_replace('/\s*,\s*/u', ', ', $value);
    $value = preg_replace('/\s+/u', ' ', $value);
    return mb_strtolower(trim($value), 'UTF-8');
}

function fetchCabinets(): array
{
    $dataClass = getHlDataClass(CABINET_HL_BLOCK_ID);
    $items = [];

    $rs = $dataClass::getList([
        'select' => ['ID', 'UF_NAME'],
        'order' => ['ID' => 'ASC'],
    ]);

    while ($row = $rs->fetch()) {
        $id = (int)$row['ID'];
        $name = trim((string)($row['UF_NAME'] ?? ''));
        if ($id <= 0 || $name === '') {
            continue;
        }
        $items[$id] = [
            'ID' => $id,
            'UF_NAME' => $name,
            'NORM_NAME' => normalizeLocationName($name),
        ];
    }

    return $items;
}

function fetchLocations(): array
{
    $items = [];
    $propertyField = 'PROPERTY_' . LINK_PROPERTY_ID;

    $rs = CIBlockElement::GetList(
        ['ID' => 'ASC'],
        ['IBLOCK_ID' => LOCATION_LIST_IBLOCK_ID, 'CHECK_PERMISSIONS' => 'N'],
        false,
        false,
        ['ID', 'IBLOCK_ID', 'NAME', 'ACTIVE', $propertyField]
    );

    while ($row = $rs->Fetch()) {
        $linkValue = $row[$propertyField . '_VALUE'] ?? null;
        $items[(int)$row['ID']] = [
            'ID' => (int)$row['ID'],
            'NAME' => (string)$row['NAME'],
            'ACTIVE' => (string)$row['ACTIVE'],
            'CABINET_ID' => (int)$linkValue,
            'NORM_NAME' => normalizeLocationName((string)$row['NAME']),
        ];
    }

    return $items;
}

function buildUniqueNameIndex(array $locations): array
{
    $counts = [];
    foreach ($locations as $location) {
        if ($location['CABINET_ID'] > 0) {
            continue;
        }
        $counts[$location['NORM_NAME']] = ($counts[$location['NORM_NAME']] ?? 0) + 1;
    }

    $index = [];
    foreach ($locations as $location) {
        if ($location['CABINET_ID'] === 0 && ($counts[$location['NORM_NAME']] ?? 0) === 1) {
            $index[$location['NORM_NAME']] = $location;
        }
    }

    return $index;
}

function updateLocation(int $locationId, array $fields, ?int $cabinetId, bool $dryRun): bool
{
    if ($dryRun) {
        logMessage('DRY-RUN update location ID=' . $locationId . ': ' . json_encode($fields, JSON_UNESCAPED_UNICODE) . ', cabinet=' . (string)$cabinetId);
        return true;
    }

    $element = new CIBlockElement();
    $ok = $element->Update($locationId, $fields);
    if (!$ok) {
        logMessage('ERROR update location ID=' . $locationId . ': ' . $element->LAST_ERROR);
        return false;
    }

    if ($cabinetId !== null) {
        CIBlockElement::SetPropertyValuesEx($locationId, LOCATION_LIST_IBLOCK_ID, [LINK_PROPERTY_ID => $cabinetId]);
    }

    return true;
}

function addLocation(array $cabinet, bool $dryRun): bool
{
    $fields = [
        'IBLOCK_ID' => LOCATION_LIST_IBLOCK_ID,
        'NAME' => $cabinet['UF_NAME'],
        'ACTIVE' => 'Y',
        'PROPERTY_VALUES' => [LINK_PROPERTY_ID => $cabinet['ID']],
    ];

    if ($dryRun) {
        logMessage('DRY-RUN add location: ' . json_encode($fields, JSON_UNESCAPED_UNICODE));
        return true;
    }

    $element = new CIBlockElement();
    $id = (int)$element->Add($fields);
    if ($id <= 0) {
        logMessage('ERROR add location for cabinet ID=' . $cabinet['ID'] . ': ' . $element->LAST_ERROR);
        return false;
    }

    logMessage('Added location ID=' . $id . ' for cabinet ID=' . $cabinet['ID']);
    return true;
}

function deleteOrDeactivateLocation(array $location, bool $dryRun): bool
{
    if (DELETE_OBSOLETE_LOCATIONS) {
        if ($dryRun) {
            logMessage('DRY-RUN delete obsolete location ID=' . $location['ID'] . ', NAME=' . $location['NAME']);
            return true;
        }
        return CIBlockElement::Delete($location['ID']);
    }

    if ($location['ACTIVE'] === 'N') {
        return true;
    }

    return updateLocation($location['ID'], ['ACTIVE' => 'N'], null, $dryRun);
}

try {
    createLock();
    ensureModulesLoaded();
    ensureAdminAuthorized();
    getLinkProperty();

    $cabinets = fetchCabinets();
    $locations = fetchLocations();
    $unlinkedLocationByName = buildUniqueNameIndex($locations);
    $usedLocationIds = [];
    $usedCabinetIds = [];

    $stats = ['linked' => 0, 'renamed' => 0, 'added' => 0, 'obsolete' => 0, 'errors' => 0];

    logMessage('Start sync. dryRun=' . ($dryRun ? 'Y' : 'N') . ', cabinets=' . count($cabinets) . ', locations=' . count($locations));

    foreach ($locations as $location) {
        if ($location['CABINET_ID'] <= 0) {
            continue;
        }

        $cabinet = $cabinets[$location['CABINET_ID']] ?? null;
        if (!$cabinet) {
            $stats['obsolete']++;
            if (!deleteOrDeactivateLocation($location, $dryRun)) {
                $stats['errors']++;
            }
            continue;
        }

        $usedLocationIds[$location['ID']] = true;
        $usedCabinetIds[$cabinet['ID']] = true;
        $fields = [];
        if ($location['NAME'] !== $cabinet['UF_NAME']) {
            $fields['NAME'] = $cabinet['UF_NAME'];
            $fields['ACTIVE'] = 'Y';
            $stats['renamed']++;
        } elseif ($location['ACTIVE'] !== 'Y') {
            $fields['ACTIVE'] = 'Y';
        }

        if ($fields && !updateLocation($location['ID'], $fields, null, $dryRun)) {
            $stats['errors']++;
        }
    }

    foreach ($cabinets as $cabinet) {
        if (isset($usedCabinetIds[$cabinet['ID']])) {
            continue;
        }

        $matchedLocation = $unlinkedLocationByName[$cabinet['NORM_NAME']] ?? null;
        if ($matchedLocation && empty($usedLocationIds[$matchedLocation['ID']])) {
            $fields = ['NAME' => $cabinet['UF_NAME'], 'ACTIVE' => 'Y'];
            if (updateLocation($matchedLocation['ID'], $fields, $cabinet['ID'], $dryRun)) {
                $usedLocationIds[$matchedLocation['ID']] = true;
                $usedCabinetIds[$cabinet['ID']] = true;
                $stats['linked']++;
                if ($matchedLocation['NAME'] !== $cabinet['UF_NAME']) {
                    $stats['renamed']++;
                }
            } else {
                $stats['errors']++;
            }
            continue;
        }

        if (addLocation($cabinet, $dryRun)) {
            $stats['added']++;
        } else {
            $stats['errors']++;
        }
    }

    logMessage('Finish sync: ' . json_encode($stats, JSON_UNESCAPED_UNICODE));
    exit($stats['errors'] > 0 ? 1 : 0);
} catch (Throwable $e) {
    stopWithLog('FATAL: ' . $e->getMessage());
}
