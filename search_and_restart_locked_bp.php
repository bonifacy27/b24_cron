<?php
/**
 * Cron-скрипт: поиск зависших бизнес-процессов и отправка push-уведомлений
 * Запускать 1 раз в 3 часа через cron в /etc/cron.d/bx_dbourtricolor
 */

// Устанавливаем DOCUMENT_ROOT вручную для CLI-режима
$_SERVER['DOCUMENT_ROOT'] = '/home/bitrix/www';

// Настройки Битрикс для фонового режима
define('NO_KEEP_STATISTIC', true);
define('NOT_CHECK_PERMISSIONS', true);
define('BX_CRONTAB', true);
define('BX_NO_ACCELERATOR_RESET', true);

// Подключаем ядро Битрикс
require_once $_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/main/include/prolog_before.php';

CModule::IncludeModule('bizproc');

$logFile = $_SERVER['DOCUMENT_ROOT'] . '/local/logs/locked_bp_workflow.log';
$connection = Bitrix\Main\Application::getConnection('default');
$sql = "SELECT ID FROM b_bp_workflow_instance WHERE WORKFLOW_TEMPLATE_ID<>790 AND OWNER_ID IS NOT NULL";

$recordset = $connection->query($sql);
while ($record = $recordset->fetch()) {
    $workflowId = $record['ID'];
    $workflowState = CBPStateService::GetWorkflowState($workflowId);
    $templateName = $workflowState['TEMPLATE_NAME'];

    $text = '⚠️ Завис бизнес-процесс "' . $templateName . '" (ID: ' . $workflowId . '). Перезапуск не выполнен.';
    message_to_pushover($text);
    logMessage($text, $logFile);
}

// Функция логирования
function logMessage($message, $file) {
    $timestamp = date('Y-m-d H:i:s');
    file_put_contents($file, "[{$timestamp}] {$message}\n", FILE_APPEND | LOCK_EX);
}

// Завершаем корректно
require_once $_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/main/include/epilog_after.php';
