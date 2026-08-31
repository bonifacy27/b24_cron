<?php
/**
 * rewrite_current_year_vacations.php
 * v1.3
 *
 * Перезаписывает один отпуск в HL 83 за текущий год
 * только для сотрудников, у которых:
 * 1. В HL 84 есть остатки: UF_YEAR = текущий год и UF_ISSUED > 0
 * 2. В HL 83 за этот год НЕТ отпусков со статусом UF_STATE = 5
 *
 * Dry-run:
 * /local/tools/rewrite_current_year_vacations.php
 *
 * Apply:
 * /local/tools/rewrite_current_year_vacations.php?apply=Y
 * php /local/tools/rewrite_current_year_vacations.php --apply
 */

use Bitrix\Main\Loader;
use Bitrix\Main\Type\Date;
use Bitrix\Highloadblock\HighloadBlockTable;

$_SERVER['DOCUMENT_ROOT'] = '/home/bitrix/www';
require($_SERVER['DOCUMENT_ROOT'].'/bitrix/modules/main/include/prolog_before.php');

global $USER;

if (PHP_SAPI !== 'cli' && (!$USER || !$USER->IsAdmin())) {
    die('Access denied');
}

Loader::includeModule('highloadblock');

const HL_BALANCES_ID  = 84;
const HL_VACATIONS_ID = 83;

$year = (int)date('Y');
$cliOptions = PHP_SAPI === 'cli' ? getopt('', ['apply']) : [];
$apply = PHP_SAPI === 'cli'
    ? isset($cliOptions['apply'])
    : ($_GET['apply'] ?? '') === 'Y';

$yearStart = Date::createFromPhp(new DateTime($year . '-01-01'));
$yearEnd   = Date::createFromPhp(new DateTime($year . '-12-31'));

function getHlDataClass(int $hlblockId): string
{
    $hlblock = HighloadBlockTable::getById($hlblockId)->fetch();

    if (!$hlblock) {
        throw new RuntimeException('HL-блок не найден: ' . $hlblockId);
    }

    $entity = HighloadBlockTable::compileEntity($hlblock);
    return $entity->getDataClass();
}

function formatHlDate($value): string
{
    if ($value instanceof Date) {
        return $value->format('d.m.Y');
    }

    return (string)$value;
}

try {
    $balancesClass = getHlDataClass(HL_BALANCES_ID);
    $vacationsClass = getHlDataClass(HL_VACATIONS_ID);

    /**
     * 1. Получаем сотрудников из HL 84:
     * UF_YEAR = текущий год и UF_ISSUED > 0
     */
    $employeeIds = [];

    $rsBalances = $balancesClass::getList([
        'select' => [
            'ID',
            'UF_EMPLOYEE',
            'UF_YEAR',
            'UF_ISSUED',
        ],
        'filter' => [
            '=UF_YEAR' => $year,
            '>UF_ISSUED' => 0,
        ],
        'order' => [
            'UF_EMPLOYEE' => 'ASC',
        ],
    ]);

    while ($row = $rsBalances->fetch()) {
        $employeeId = (int)$row['UF_EMPLOYEE'];

        if ($employeeId > 0) {
            $employeeIds[$employeeId] = $employeeId;
        }
    }

    /**
     * 2. Ищем сотрудников, у которых за этот год уже есть отпуск UF_STATE = 5.
     * Таких сотрудников нужно исключить из перезаписи полностью.
     */
    $employeesWithState5 = [];

    if (!empty($employeeIds)) {
        foreach (array_chunk(array_values($employeeIds), 500) as $employeeChunk) {
            $rsState5Vacations = $vacationsClass::getList([
                'select' => [
                    'ID',
                    'UF_EMPLOYEE',
                    'UF_DATE_BEGIN',
                    'UF_DATE_END',
                    'UF_STATE',
                ],
                'filter' => [
                    '@UF_EMPLOYEE' => $employeeChunk,
                    '=UF_STATE' => 5,

                    // Отпуск относится к году, если пересекает период года:
                    // начало <= конец текущего года и конец >= начало текущего года
                    '<=UF_DATE_BEGIN' => $yearEnd,
                    '>=UF_DATE_END' => $yearStart,
                ],
                'order' => [
                    'UF_EMPLOYEE' => 'ASC',
                    'UF_DATE_BEGIN' => 'ASC',
                    'ID' => 'ASC',
                ],
            ]);

            while ($vacation = $rsState5Vacations->fetch()) {
                $employeeId = (int)$vacation['UF_EMPLOYEE'];

                if ($employeeId > 0) {
                    $employeesWithState5[$employeeId] = $employeeId;
                }
            }
        }
    }

    /**
     * 3. Оставляем только сотрудников без отпусков UF_STATE = 5 за год.
     */
    $employeeIdsToRewrite = array_diff_key($employeeIds, $employeesWithState5);

    /**
     * 4. По каждому оставшемуся сотруднику перезаписываем один отпуск за год.
     * Этого достаточно, чтобы модуль отпусков обновил остатки сотрудника.
     */
    $totalFound = 0;
    $totalUpdated = 0;
    $totalErrors = 0;
    $log = [];

    foreach ($employeeIdsToRewrite as $employeeId) {
        $rsVacations = $vacationsClass::getList([
            'select' => [
                'ID',
                'UF_EMPLOYEE',
                'UF_DATE_BEGIN',
                'UF_DATE_END',
            ],
            'filter' => [
                '=UF_EMPLOYEE' => $employeeId,

                // Отпуск относится к году, если пересекает период года:
                // начало <= конец текущего года и конец >= начало текущего года
                '<=UF_DATE_BEGIN' => $yearEnd,
                '>=UF_DATE_END' => $yearStart,
            ],
            'order' => [
                'UF_EMPLOYEE' => 'ASC',
                'UF_DATE_BEGIN' => 'ASC',
                'ID' => 'ASC',
            ],
            'limit' => 1,
        ]);

        while ($vacation = $rsVacations->fetch()) {
            $totalFound++;

            $vacationId = (int)$vacation['ID'];

            $fieldsToRewrite = [
                'UF_EMPLOYEE' => $vacation['UF_EMPLOYEE'],
                'UF_DATE_BEGIN' => $vacation['UF_DATE_BEGIN'],
                'UF_DATE_END' => $vacation['UF_DATE_END'],
            ];

            if ($apply) {
                $result = $vacationsClass::update($vacationId, $fieldsToRewrite);

                if ($result->isSuccess()) {
                    $totalUpdated++;
                    $status = 'UPDATED';
                } else {
                    $totalErrors++;
                    $status = 'ERROR: ' . implode('; ', $result->getErrorMessages());
                }
            } else {
                $status = 'DRY-RUN';
            }

            $log[] = [
                'ID' => $vacationId,
                'EMPLOYEE' => $employeeId,
                'BEGIN' => formatHlDate($vacation['UF_DATE_BEGIN']),
                'END' => formatHlDate($vacation['UF_DATE_END']),
                'STATUS' => $status,
            ];
        }
    }

    ?>
    <pre>
rewrite_current_year_vacations.php v1.3

Режим: <?= $apply ? 'APPLY' : 'DRY-RUN' ?>

HL остатков: <?= HL_BALANCES_ID ?>

HL отпусков: <?= HL_VACATIONS_ID ?>

Год: <?= $year ?>


Сотрудников с остатками UF_ISSUED > 0: <?= count($employeeIds) ?>

Исключено сотрудников с отпуском UF_STATE = 5 за год: <?= count($employeesWithState5) ?>

Сотрудников к перезаписи: <?= count($employeeIdsToRewrite) ?>

Выбрано отпусков к перезаписи (не более одного на сотрудника): <?= $totalFound ?>

Перезаписано: <?= $totalUpdated ?>

Ошибок: <?= $totalErrors ?>


<?php foreach ($log as $item): ?>
#<?= $item['ID'] ?> | employee=<?= $item['EMPLOYEE'] ?> | <?= $item['BEGIN'] ?> - <?= $item['END'] ?> | <?= $item['STATUS'] ?>

<?php endforeach; ?>
    </pre>
    <?php

} catch (Throwable $e) {
    ?>
    <pre>
ERROR:
<?= htmlspecialchars($e->getMessage()) ?>

<?= htmlspecialchars($e->getTraceAsString()) ?>
    </pre>
    <?php
}

require($_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/main/include/epilog_after.php');
