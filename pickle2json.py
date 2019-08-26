#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# A simple chat client for matrix.
# This sample will allow you to connect to a room, and send/recieve messages.
# Args: host:port username password room
# Error Codes:
# 1 - Unknown problem has occured
# 2 - Could not find the server.
# 3 - Bad URL Format.
# 4 - Bad username/password.
# 11 - Wrong room format.
# 12 - Couldn't find room.

import sys
import logging
import time
import datetime
import json
import os
import pickle
import traceback
import vk
#import ujson
import config as conf

def get_exception_traceback_descr(e):
  tb_str = traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__)
  result=""
  for msg in tb_str:
    result+=msg
  return result

def save_json(data):
  global log
  log.debug("=start function=")
  log.debug("save to data_file:%s"%conf.data_file)
  try:
    #data_file=open(conf.data_file,"wb")
    data_file=open(conf.data_file,"w")
  except Exception as e:
    log.error(get_exception_traceback_descr(e))
    log.error("open(%s) for writing"%conf.data_file)
    return False
    
  try:
    data_file.write(json.dumps(data, indent=4, sort_keys=True,ensure_ascii=False))
    #pickle.dump(data,data_file)
    data_file.close()
  except Exception as e:
    log.error(get_exception_traceback_descr(e))
    log.error("json.dump to '%s'"%conf.data_file)
    print(json.dumps(event, indent=4, sort_keys=True,ensure_ascii=False))
    sys.exit(1)
    return False
  return True

def load_pickle():
  global log
  log.debug("=start function=")
  tmp_data_file=conf.data_file
  if os.path.exists(tmp_data_file):
    log.debug("Загружаем файл промежуточных данных: '%s'" % tmp_data_file)
    data_file = open(tmp_data_file,'rb')
    try:
      data=pickle.load(data_file)
      data_file.close()
      log.debug("Загрузили файл промежуточных данных: '%s'" % tmp_data_file)
      if not "users" in data:
        log.warning("Битый файл сессии - сброс")
        return None
    except Exception as e:
      log.error(get_exception_traceback_descr(e))
      log.warning("Битый файл сессии - сброс")
      return None
  else:
    log.warning("Файл промежуточных данных не существует")
    return None
  #debug_dump_json_to_file("debug_data_as_json.json",data)
  return data

def main():
  data=load_pickle()
  for user in data["users"]:
    item=data["users"][user]
    if "vk" in item and "session" in item["vk"]:
      print(item["vk"]["session"])
      del item["vk"]["session"]
#  print(json.dumps(data, indent=4, sort_keys=True,ensure_ascii=False))
  save_json(data)

if __name__ == '__main__':
  log= logging.getLogger("pickle2json")
  if conf.debug:
    log.setLevel(logging.DEBUG)
  else:
    log.setLevel(logging.INFO)

  # create the logging file handler
  fh = logging.FileHandler(conf.log_path)
  formatter = logging.Formatter('%(asctime)s - %(name)s - %(filename)s:%(lineno)d - %(funcName)s() %(levelname)s - %(message)s')
  fh.setFormatter(formatter)

  if conf.debug:
    # логирование в консоль:
    #stdout = logging.FileHandler("/dev/stdout")
    stdout = logging.StreamHandler(sys.stdout)
    stdout.setFormatter(formatter)
    log.addHandler(stdout)

  # add handler to logger object
  log.addHandler(fh)

  log.info("Program started")
  main()
  log.info("Program exit!")
