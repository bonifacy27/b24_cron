<?php
/**
 * Installs and reads a SQL Server audit for vacation status changes.
 *
 * Install once (requires CREATE TABLE / CREATE TRIGGER permissions):
 *   php -f vacations_sql_status_audit.php -- --install
 *
 * Report (read-only, last 14 days by default):
 *   php -f vacations_sql_status_audit.php -- --days=30 --limit=500
 *
 * The important transitions are marked as TARGET_5_TO_6 and ROLLBACK_6_TO_5.
 * Actor=vacations_sync_log.php is supplied through SESSION_CONTEXT by the sync.
 */

use Bitrix\Main\Application;
use Bitrix\Main\Loader;

define('NOT_CHECK_PERMISSIONS', true);
define('NO_KEEP_STATISTIC', true);
define('BX_NO_ACCELERATOR_RESET', true);

$_SERVER['DOCUMENT_ROOT'] = '/home/bitrix/www';
require $_SERVER['DOCUMENT_ROOT'].'/bitrix/modules/main/include/prolog_before.php';

Loader::includeModule('main');

const AUDIT_CONNECTION = 'gatedb';
const AUDIT_DATABASE = 'GateDB';
const AUDIT_SCHEMA = 'dbo';
const AUDIT_SOURCE_TABLE = 'StaffAbsences_1CZUP';
const AUDIT_TABLE = 'StaffAbsences_1CZUP_StatusAudit';
const AUDIT_TRIGGER = 'trg_StaffAbsences_1CZUP_StatusAudit';

$isCli = PHP_SAPI === 'cli';
if (!$isCli) {
    global $USER;
    if (!$USER || !$USER->IsAdmin()) {
        http_response_code(403);
        die('Access denied');
    }
}

function optionValue(string $name, ?string $default = null): ?string
{
    global $argv;
    if (PHP_SAPI === 'cli') {
        foreach (array_slice($argv ?? [], 1) as $argument) {
            if ($argument === '--'.$name) {
                return 'Y';
            }
            if (strpos($argument, '--'.$name.'=') === 0) {
                return substr($argument, strlen($name) + 3);
            }
        }
        return $default;
    }

    return isset($_GET[$name]) ? (string)$_GET[$name] : $default;
}

function qualified(string $object): string
{
    return '['.AUDIT_DATABASE.'].['.AUDIT_SCHEMA.'].['.$object.']';
}

function installAudit(\Bitrix\Main\DB\Connection $connection): void
{
    $audit = qualified(AUDIT_TABLE);
    $source = '['.AUDIT_SCHEMA.'].['.AUDIT_SOURCE_TABLE.']';
    $trigger = '['.AUDIT_SCHEMA.'].['.AUDIT_TRIGGER.']';

    $connection->queryExecute("
IF OBJECT_ID(N'".AUDIT_DATABASE.".".AUDIT_SCHEMA.".".AUDIT_TABLE."', N'U') IS NULL
BEGIN
    CREATE TABLE {$audit} (
        Audit_ID bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
        Changed_At datetime2(3) NOT NULL CONSTRAINT DF_StaffAbsences_StatusAudit_ChangedAt DEFAULT SYSUTCDATETIME(),
        Absence_ID nvarchar(255) NOT NULL,
        Old_Status int NULL,
        New_Status int NULL,
        Old_State int NULL,
        New_State int NULL,
        Old_Renew_Date datetime NULL,
        New_Renew_Date datetime NULL,
        Writer nvarchar(128) NULL,
        Login_Name nvarchar(128) NOT NULL,
        Original_Login_Name nvarchar(128) NOT NULL,
        Host_Name nvarchar(128) NULL,
        Application_Name nvarchar(128) NULL,
        Session_ID int NOT NULL
    );
    CREATE INDEX IX_StaffAbsences_StatusAudit_ChangedAt
        ON {$audit} (Changed_At DESC, Audit_ID DESC);
    CREATE INDEX IX_StaffAbsences_StatusAudit_Absence
        ON {$audit} (Absence_ID, Changed_At DESC);
END");

    $triggerSql = "CREATE OR ALTER TRIGGER {$trigger}
ON {$source}
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT UPDATE(Absence_Status) RETURN;

    INSERT INTO [".AUDIT_SCHEMA."].[".AUDIT_TABLE."]
        (Absence_ID, Old_Status, New_Status, Old_State, New_State,
         Old_Renew_Date, New_Renew_Date, Writer, Login_Name,
         Original_Login_Name, Host_Name, Application_Name, Session_ID)
    SELECT CONVERT(nvarchar(255), i.Absence_ID), d.Absence_Status, i.Absence_Status,
           d.Absence_State, i.Absence_State, d.Absence_Renew_Date, i.Absence_Renew_Date,
           CONVERT(nvarchar(128), SESSION_CONTEXT(N'vacation_writer')),
           SUSER_SNAME(), ORIGINAL_LOGIN(), HOST_NAME(), APP_NAME(), @@SPID
    FROM inserted i
    INNER JOIN deleted d ON d.Absence_ID = i.Absence_ID
    WHERE (i.Absence_Status <> d.Absence_Status)
       OR (i.Absence_Status IS NULL AND d.Absence_Status IS NOT NULL)
       OR (i.Absence_Status IS NOT NULL AND d.Absence_Status IS NULL);
END";

    $escapedTriggerSql = str_replace("'", "''", $triggerSql);
    $connection->queryExecute(
        "EXEC [".AUDIT_DATABASE."].sys.sp_executesql N'{$escapedTriggerSql}'"
    );
}

function printReport(\Bitrix\Main\DB\Connection $connection, int $days, int $limit): void
{
    $audit = qualified(AUDIT_TABLE);
    $summary = $connection->query("
SELECT
    SUM(CASE WHEN Old_Status = 5 AND New_Status = 6 THEN 1 ELSE 0 END) AS Zup5To6,
    SUM(CASE WHEN Old_Status = 6 AND New_Status = 5 THEN 1 ELSE 0 END) AS Rollback6To5,
    COUNT(*) AS Total
FROM {$audit}
WHERE Changed_At >= DATEADD(day, -{$days}, SYSUTCDATETIME())")->fetch();

    echo "SQL vacation status audit (UTC), last {$days} day(s)\n";
    echo '5 -> 6: '.(int)$summary['Zup5To6']
        .'; 6 -> 5: '.(int)$summary['Rollback6To5']
        .'; all status changes: '.(int)$summary['Total']."\n\n";
    echo "TIME\tABSENCE_ID\tTRANSITION\tACTOR\tLOGIN\tHOST\tAPPLICATION\tRENEW_DATE\n";

    $rows = $connection->query("
SELECT TOP {$limit} Changed_At, Absence_ID, Old_Status, New_Status,
       COALESCE(Writer, N'<external>') AS Actor, Original_Login_Name,
       Host_Name, Application_Name, New_Renew_Date
FROM {$audit}
WHERE Changed_At >= DATEADD(day, -{$days}, SYSUTCDATETIME())
ORDER BY Changed_At DESC, Audit_ID DESC");
    while ($row = $rows->fetch()) {
        $transition = $row['Old_Status'].' -> '.$row['New_Status'];
        if ((int)$row['Old_Status'] === 5 && (int)$row['New_Status'] === 6) {
            $transition .= ' [TARGET_5_TO_6]';
        } elseif ((int)$row['Old_Status'] === 6 && (int)$row['New_Status'] === 5) {
            $transition .= ' [ROLLBACK_6_TO_5]';
        }
        $values = [
            $row['Changed_At'], $row['Absence_ID'], $transition, $row['Actor'],
            $row['Original_Login_Name'], $row['Host_Name'], $row['Application_Name'],
            $row['New_Renew_Date'],
        ];
        echo implode("\t", array_map(static function ($value): string {
            return str_replace(["\r", "\n", "\t"], ' ', (string)$value);
        }, $values))."\n";
    }
}

$days = max(1, min(365, (int)optionValue('days', '14')));
$limit = max(1, min(10000, (int)optionValue('limit', '500')));
$install = optionValue('install', 'N') === 'Y';
$connection = Application::getConnection(AUDIT_CONNECTION);

try {
    if ($install) {
        installAudit($connection);
        echo "Audit table and trigger are installed.\n\n";
    }
    printReport($connection, $days, $limit);
} catch (\Throwable $error) {
    if (PHP_SAPI === 'cli') {
        fwrite(STDERR, 'ERROR: '.$error->getMessage()."\n");
    } else {
        http_response_code(500);
        echo 'ERROR: '.htmlspecialchars($error->getMessage(), ENT_QUOTES | ENT_SUBSTITUTE, 'UTF-8');
    }
    exit(1);
}
