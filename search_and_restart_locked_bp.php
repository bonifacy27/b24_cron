<?php
/**
 * Cron-скрипт: поиск и перезапуск зависших бизнес-процессов
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

use Bitrix\Main\Application;
use Bitrix\Main\Diag\Debug;
CModule::IncludeModule('bizproc');
$logFile = $_SERVER['DOCUMENT_ROOT'] . '/local/logs/locked_bp_workflow.log';

$connection = Bitrix\Main\Application::getConnection('default');
$sqlHelper = $connection->getSqlHelper();

$telegram_id = '924569493';
$method='sendMessage';

$sql = "SELECT ID FROM b_bp_workflow_instance WHERE WORKFLOW_TEMPLATE_ID<>790 AND OWNER_ID IS NOT NULL";

$recordset = $connection->query($sql);
while ($record = $recordset->fetch())
{
	
$workflow_id = $record['ID']; //ID инстанс

$arState = CBPStateService::GetWorkflowState($workflow_id);  // забираем параметры инстанс

$BP_template_id = $arState['TEMPLATE_ID']; // ID шаблона БП
$BP_parameters = $arState['DOCUMENT_ID'];  // параметры запуска БП - id документа, id Iblock
$BP_template_name = $arState['TEMPLATE_NAME']; // название шаблона БП

$text='Завис процесс '.$BP_template_name;
message_to_telegram_cron($text,$cfile,$method,$telegram_id);
logMessage($text, $logFile);

/////////////////////
// Останавливаем зависший процесс

CBPDocument::TerminateWorkflow(
		$workflow_id,
		$arState["DOCUMENT_ID"],
		$arErrorsTmp
	);
	if (count($arErrorsTmp) > 0)
	{
		foreach ($arErrorsTmp as $e)
			$errorMessage .= $e["message"].". ";
	}


/////////////////////
// Запускаем заново тот же процесс по тому же документу

$arErrorsTmp = array();
$wfId = CBPDocument::StartWorkflow(
	       $BP_template_id,
	       $BP_parameters,
	       array(),
	       $arErrorsTmp
	 );

if (count($arErrorsTmp) > 0)
{
	foreach ($arErrorsTmp as $e)
		$errorMessage .= "[".$e["code"]."] ".$e["message"]."
";
}


}


// Функция логирования
function logMessage($message, $file) {
    $timestamp = date('Y-m-d H:i:s');
    file_put_contents($file, "[{$timestamp}] {$message}\n", FILE_APPEND | LOCK_EX);
}

function message_to_telegram_cron($text,$cfile,$method,$chatid)
{
    $ch = curl_init();
    curl_setopt_array(
        $ch,
        array(
            CURLOPT_URL => 'https://api.telegram.org/bot1829906430:AAHimzkz7PFYOa8ha26Yfrxtn4QSRPKUG7w/'.$method,
            CURLOPT_POST => TRUE,
            CURLOPT_RETURNTRANSFER => TRUE,
            CURLOPT_TIMEOUT => 10,
            CURLOPT_POSTFIELDS => array(
                'chat_id' => $chatid,
                'text' => $text,
				'photo' => $cfile,
            ),
        )
    );
    curl_exec($ch);
	curl_close($ch);
}



// Завершаем корректно
require_once $_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/main/include/epilog_after.php';

