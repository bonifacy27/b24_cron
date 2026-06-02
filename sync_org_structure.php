<?php
/**
 * sync_org_structure.php
 * Version: 1.6
 *
 * Путь:
 * /home/bitrix/www/local/cron/sync_org_structure.php
 *
 * Логи:
 * /home/bitrix/www/upload/logs/sync_org_structure.log
 */

$_SERVER["DOCUMENT_ROOT"] = '/home/bitrix/www';
$DOCUMENT_ROOT = $_SERVER["DOCUMENT_ROOT"];

define("NO_KEEP_STATISTIC", true);
define("NOT_CHECK_PERMISSIONS", true);
define("BX_NO_ACCELERATOR_RESET", true);
define("BX_CRONTAB", true);

require($_SERVER["DOCUMENT_ROOT"] . "/bitrix/modules/main/include/prolog_before.php");

use Bitrix\Main\Loader;
use Bitrix\Main\Config\Option;
use Bitrix\Highloadblock\HighloadBlockTable;
use Bitrix\Main\Type\DateTime;

const SCRIPT_VERSION = '1.6';

const HL_BLOCK_ID = 99;

/**
 * Сколько сотрудников с заполненным UF_1C_GUID реально обрабатываем за запуск
 */
const LIMIT_USERS = 50;

/**
 * Сколько активных пользователей просматриваем за один внутренний проход,
 * чтобы пропускать пользователей без UF_1C_GUID
 */
const SELECT_PORTION = 500;

// TEST connect:   const SERVICE_URL_TEMPLATE = 'http://srv-off-1c03.nsc.ru/1c_Pay8_NSC_DEMO4/hs/Bitrix24Exchange/GetStaffInfo/%s/';
const SERVICE_URL_TEMPLATE = 'https://srv-off-1c01.nsc.ru/zup2/hs/Bitrix24Exchange/GetStaffInfo/%s/';


const SERVICE_LOGIN = 'test';
const SERVICE_PASSWORD = 'test';

const REQUEST_TIMEOUT = 15;

const LOG_FILE = '/home/bitrix/www/upload/logs/sync_org_structure.log';
const LOCK_FILE = '/home/bitrix/www/upload/logs/sync_org_structure.lock';

const OPTION_MODULE = 'local';
const OPTION_LAST_USER_ID = 'sync_org_structure_last_user_id';
const OPTION_LAST_RUN_DATE = 'sync_org_structure_last_date';

function logMessage($message)
{
    $file = LOG_FILE;
    $dir = dirname($file);

    if (!is_dir($dir)) {
        mkdir($dir, 0775, true);
    }

    file_put_contents(
        $file,
        '[' . date('Y-m-d H:i:s') . '] ' . $message . PHP_EOL,
        FILE_APPEND | LOCK_EX
    );
}

function stopWithLog($message)
{
    logMessage($message);
    echo $message . PHP_EOL;
    exit;
}

function createLock()
{
    $lockFile = LOCK_FILE;

    if (file_exists($lockFile)) {
        $pid = (int)file_get_contents($lockFile);

        if ($pid > 0 && function_exists('posix_kill') && posix_kill($pid, 0)) {
            stopWithLog('Script already running. PID=' . $pid);
        }

        unlink($lockFile);
    }

    file_put_contents($lockFile, getmypid());

    register_shutdown_function(function () use ($lockFile) {
        if (file_exists($lockFile)) {
            unlink($lockFile);
        }
    });
}

function getHlEntityDataClass($hlBlockId)
{
    $hlBlock = HighloadBlockTable::getById($hlBlockId)->fetch();

    if (!$hlBlock) {
        throw new Exception('HL-block not found. ID=' . $hlBlockId);
    }

    $entity = HighloadBlockTable::compileEntity($hlBlock);

    return $entity->getDataClass();
}

function normalizeName($value)
{
    $value = trim((string)$value);
    return preg_replace('/\s+/u', ' ', $value);
}

function makePathHash($path)
{
    return md5(mb_strtolower(trim($path), 'UTF-8'));
}

function requestStaffInfo($guid)
{
    $url = sprintf(SERVICE_URL_TEMPLATE, rawurlencode($guid));

    $ch = curl_init();

    curl_setopt_array($ch, [
        CURLOPT_URL => $url,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_CONNECTTIMEOUT => REQUEST_TIMEOUT,
        CURLOPT_SSL_VERIFYPEER => false,
        CURLOPT_SSL_VERIFYHOST => false,
        CURLOPT_TIMEOUT => REQUEST_TIMEOUT,
        CURLOPT_HTTPAUTH => CURLAUTH_BASIC,
        CURLOPT_USERPWD => SERVICE_LOGIN . ':' . SERVICE_PASSWORD,
        CURLOPT_HTTPHEADER => [
            'Accept: application/json',
        ],
    ]);

    $response = curl_exec($ch);
    $error = curl_error($ch);
    $httpCode = (int)curl_getinfo($ch, CURLINFO_HTTP_CODE);

    curl_close($ch);

    if ($response === false || $error) {
        throw new Exception('1C request error: ' . $error);
    }

    if ($httpCode < 200 || $httpCode >= 300) {
        throw new Exception(
            '1C HTTP error: ' . $httpCode . '. Response: ' . mb_substr((string)$response, 0, 300)
        );
    }

    $data = json_decode($response, true);

    if (!is_array($data)) {
        throw new Exception(
            'Invalid JSON from 1C. Response: ' . mb_substr((string)$response, 0, 300)
        );
    }

    if (empty($data['success'])) {
        throw new Exception(
            '1C returned success=false. Response: ' . mb_substr((string)$response, 0, 300)
        );
    }

    return $data;
}

function extractCeoChain(array $data)
{
    $levels = [];

    foreach ($data as $key => $value) {
        if (preg_match('/^ceo(\d+)$/i', $key, $matches)) {
            $level = (int)$matches[1];
            $name = normalizeName($value);
            $nameLower = mb_strtolower($name, 'UTF-8');

            if (
                $level > 0
                && $name !== ''
                && $nameLower !== 'null'
                && $nameLower !== 'nul'
                && $nameLower !== 'none'
            ) {
                $levels[$level] = $name;
            }
        }
    }

    ksort($levels);

    return $levels;
}

function findOrCreateNode($dataClass, $name, $level, $parentId, $fullPath)
{
    $pathHash = makePathHash($fullPath);

    $row = $dataClass::getList([
        'select' => ['ID'],
        'filter' => [
            '=UF_PATH_HASH' => $pathHash,
        ],
        'limit' => 1,
    ])->fetch();

    $fields = [
        'UF_NAME' => $name,
        'UF_LEVEL' => $level,
        'UF_FULL_PATH' => $fullPath,
        'UF_ACTIVE' => 1,
        'UF_LAST_SEEN_AT' => new DateTime(),
    ];

    if ($parentId > 0) {
        $fields['UF_PARENT_ID'] = $parentId;
    } else {
        $fields['UF_PARENT_ID'] = false;
    }

    if ($row) {
        $result = $dataClass::update($row['ID'], $fields);

        if (!$result->isSuccess()) {
            throw new Exception(
                'HL update error: ' . implode('; ', $result->getErrorMessages())
            );
        }

        return (int)$row['ID'];
    }

    $fields['UF_PATH_HASH'] = $pathHash;
    $fields['UF_XML_ID'] = $pathHash;
    $fields['UF_SORT'] = $level * 100;

    $result = $dataClass::add($fields);

    if (!$result->isSuccess()) {
        throw new Exception(
            'HL add error: ' . implode('; ', $result->getErrorMessages())
        );
    }

    return (int)$result->getId();
}

function syncOrgChain($dataClass, array $ceoChain)
{
    $parentId = 0;
    $pathParts = [];
    $lastNodeId = 0;
    $lastLevel = 0;

    foreach ($ceoChain as $level => $name) {
        $pathParts[] = $name;
        $fullPath = implode(' / ', $pathParts);

        $nodeId = findOrCreateNode(
            $dataClass,
            $name,
            $level,
            $parentId,
            $fullPath
        );

        $parentId = $nodeId;
        $lastNodeId = $nodeId;
        $lastLevel = $level;
    }

    return [
        'NODE_ID' => $lastNodeId,
        'PATH' => implode(' / ', $pathParts),
        'LEVEL' => $lastLevel,
    ];
}

/**
 * Важно:
 * CUser::GetList может некорректно фильтровать по !UF_1C_GUID.
 * Поэтому берем активных пользователей порциями и сами отбираем тех, у кого GUID заполнен.
 */
function getUsersForSync($limit, $lastUserId, &$newLastUserId)
{
    $users = [];
    $currentLastUserId = $lastUserId;
    $newLastUserId = $lastUserId;

    while (count($users) < $limit) {
        $rsUsers = CUser::GetList(
            $by = 'ID',
            $order = 'ASC',
            [
                'ACTIVE' => 'Y',
                '>ID' => $currentLastUserId,
            ],
            [
                'FIELDS' => [
                    'ID',
                    'LOGIN',
                    'NAME',
                    'LAST_NAME',
                ],
                'SELECT' => [
                    'UF_1C_GUID',
                ],
                'NAV_PARAMS' => [
                    'nTopCount' => SELECT_PORTION,
                ],
            ]
        );

        $portionFound = 0;
        $portionMaxUserId = $currentLastUserId;

        while ($user = $rsUsers->Fetch()) {
            $portionFound++;

            $userId = (int)$user['ID'];
            $portionMaxUserId = max($portionMaxUserId, $userId);

            if (trim((string)$user['UF_1C_GUID']) !== '') {
                $users[] = $user;

                if (count($users) >= $limit) {
                    break;
                }
            }
        }

        $newLastUserId = max($newLastUserId, $portionMaxUserId);

        if ($portionFound === 0) {
            break;
        }

        $currentLastUserId = $portionMaxUserId;

        if ($portionMaxUserId <= $lastUserId) {
            break;
        }
    }

    return $users;
}

function updateUserOrgData($userId, array $orgData, array $staffData)
{
    $user = new CUser();

    $fields = [
        'UF_1C_ORG_NODE' => $orgData['NODE_ID'],
        'UF_1C_ORG_PATH' => $orgData['PATH'],
        'UF_1C_ORG_LEVEL' => $orgData['LEVEL'],
        'UF_1C_MANAGER_GUID' => normalizeName($staffData['managerGuid'] ?? ''),
        'UF_1C_SYNC_AT' => new DateTime(),
    ];

    $result = $user->Update($userId, $fields);

    if (!$result) {
        global $APPLICATION;

        $exception = $APPLICATION->GetException();

        $error = $exception
            ? $exception->GetString()
            : $user->LAST_ERROR;

        throw new Exception('User update error: ' . $error);
    }
}

try {
    createLock();

    logMessage('========================================');
    logMessage('Start sync_org_structure.php v' . SCRIPT_VERSION);

    if (!Loader::includeModule('highloadblock')) {
        throw new Exception('Module highloadblock not loaded');
    }

    $currentDate = date('Y-m-d');

    $lastRunDate = Option::get(
        OPTION_MODULE,
        OPTION_LAST_RUN_DATE,
        ''
    );

    if ($lastRunDate !== $currentDate) {
        Option::set(OPTION_MODULE, OPTION_LAST_USER_ID, 0);
        Option::set(OPTION_MODULE, OPTION_LAST_RUN_DATE, $currentDate);

        logMessage('New day detected. Progress reset.');
    }

    $dataClass = getHlEntityDataClass(HL_BLOCK_ID);

    $lastUserId = (int)Option::get(
        OPTION_MODULE,
        OPTION_LAST_USER_ID,
        0
    );

    logMessage('Last user ID: ' . $lastUserId);
    logMessage('Limit users: ' . LIMIT_USERS);
    logMessage('Select portion: ' . SELECT_PORTION);

    $newLastUserId = $lastUserId;

    $users = getUsersForSync(
        LIMIT_USERS,
        $lastUserId,
        $newLastUserId
    );

    if (empty($users)) {
        if ($newLastUserId > $lastUserId) {
            Option::set(
                OPTION_MODULE,
                OPTION_LAST_USER_ID,
                $newLastUserId
            );

            logMessage(
                'No users with UF_1C_GUID in scanned portion. Progress saved. Last user ID=' .
                $newLastUserId
            );
        } else {
            logMessage(
                'Users not found after ID=' .
                $lastUserId .
                '. Daily sync is complete.'
            );
        }

        echo 'No users with UF_1C_GUID. LastUserId=' . $newLastUserId . PHP_EOL;
        exit;
    }

    logMessage('Users with UF_1C_GUID found: ' . count($users));

    $successCount = 0;
    $skipCount = 0;
    $processedCount = 0;
    $maxProcessedUserId = $lastUserId;

    foreach ($users as $user) {
        $userId = (int)$user['ID'];

        $maxProcessedUserId = max(
            $maxProcessedUserId,
            $userId
        );

        $processedCount++;

        $guid = trim((string)$user['UF_1C_GUID']);

        $userName = trim(
            $user['LAST_NAME'] . ' ' . $user['NAME']
        );

        logMessage(
            'User ID=' .
            $userId .
            ' GUID=' .
            $guid .
            ' ' .
            $userName
        );

        try {
            $staffData = requestStaffInfo($guid);

            $ceoChain = extractCeoChain($staffData);

            if (empty($ceoChain)) {
                throw new Exception('CEO chain is empty');
            }

            logMessage(
                'CEO chain: ' .
                implode(' / ', $ceoChain)
            );

            $orgData = syncOrgChain(
                $dataClass,
                $ceoChain
            );

            if (empty($orgData['NODE_ID'])) {
                throw new Exception('Final org node not created');
            }

            updateUserOrgData(
                $userId,
                $orgData,
                $staffData
            );

            logMessage(
                'User updated. NODE_ID=' .
                $orgData['NODE_ID'] .
                ', LEVEL=' .
                $orgData['LEVEL']
            );

            $successCount++;
        } catch (Throwable $e) {
            $skipCount++;

            logMessage(
                'SKIP user ID=' .
                $userId .
                ': ' .
                $e->getMessage()
            );
        }

        usleep(300000);
    }

    /**
     * Сохраняем прогресс.
     * Берем максимум:
     * - последний реально обработанный пользователь с GUID
     * - последний просмотренный пользователь в порции
     */
    $progressUserId = max($maxProcessedUserId, $newLastUserId);

    Option::set(
        OPTION_MODULE,
        OPTION_LAST_USER_ID,
        $progressUserId
    );

    logMessage(
        'Progress saved. Last user ID=' .
        $progressUserId
    );

    logMessage(
        'Finish sync. Processed=' .
        $processedCount .
        ', Success=' .
        $successCount .
        ', Skipped=' .
        $skipCount
    );

    logMessage('========================================');

    echo
        'Done. Processed=' .
        $processedCount .
        ', Success=' .
        $successCount .
        ', Skipped=' .
        $skipCount .
        ', LastUserId=' .
        $progressUserId .
        PHP_EOL;

} catch (Throwable $e) {
    logMessage(
        'FATAL ERROR: ' .
        $e->getMessage()
    );

    echo
        'FATAL ERROR: ' .
        $e->getMessage() .
        PHP_EOL;
}
