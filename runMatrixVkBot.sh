#!/bin/bash
if [ -z "$3" ]
then
  echo "Необходимо три параметра: скрипт, который запускаем, файл статуса, в который пишем отчёт 'SUCCESS' или 'ERROR' и третий файл дампа, в котоый пишем вывод команды и сохраняем его в случае ошибки!"
  exit 1
fi
exec_file="$1"
status_file="$2"
error_dump_file="$3"

# Проверка на повторный одновременный запуск:
if [ ! -z "`ps aux|grep proccess_telegram_db.py|grep -v grep|grep -v runMatrixVkBot.sh`" ]
then
  echo "already executing - skip start script"
  exit 0
fi

$exec_file &> ${error_dump_file}
if [ $? == 0 ] 
then 
  echo "SUCCESS exec ${exec_file}" > ${status_file}
else 
  echo "ERROR exec ${exec_file}" > ${status_file}
  cp "${error_dump_file}" "${error_dump_file}_error_save_`date +%Y.%m.%d-%T`"
fi

