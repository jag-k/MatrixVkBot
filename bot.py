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
import re
import threading
import redis
import requests
import traceback
import ujson
import vk
import wget
from PIL import Image
from credentials import token, vk_app_id
from vk_messages import VkMessage, VkPolling

from matrix_client.client import MatrixClient
from matrix_client.api import MatrixRequestError
from requests.exceptions import MissingSchema
import config as conf

client = None
log = None
data={}
lock = None

vk_threads = {}

vk_dialogs = {}

VK_API_VERSION = '3.0'

currentchat = {}

link = 'https://oauth.vk.com/authorize?client_id={}&' \
       'display=page&redirect_uri=https://oauth.vk.com/blank.html&scope=friends,messages,offline,docs,photos,video' \
       '&response_type=token&v={}'.format(vk_app_id, VK_API_VERSION)


def process_command(user,room,cmd):
  global client
  global log
  global data
  answer=None
  session_data=None

  if user not in data["users"]:
    data["users"][user]={}
  if room not in data["users"][user]:
    data["users"][user][room]={}
    data["users"][user][room]["state"]="listen_command"

  session_data=data["users"][user][room]

  cur_state=data["users"][user][room]["state"]

  if cur_state == "listen_command":
    if re.search('^!*\?$', cmd.lower()) is not None or \
      re.search('^!*h$', cmd.lower()) is not None or \
      re.search('^!*помощь', cmd.lower()) is not None or \
      re.search('^!*справка', cmd.lower()) is not None or \
      re.search('^!*help', cmd.lower()) is not None:
      answer="""!login - авторизоваться в ВК
!logout - выйти из ВК
!search - поиск диалогов в ВК
      """ 
      return send_message(room,answer)

    # login
    elif re.search('^!login .*', cmd.lower()) is not None:
      return login_command(user,room,cmd)
  elif cur_state == "wait_vk_id":
    if re.search('^!stop$', cmd.lower()) is not None or \
        re.search('^!отмена$', cmd.lower()) is not None or \
        re.search('^!cancel$', cmd.lower()) is not None:
      data[user][room]["state"]="listen_command"
      send_message(room,'Отменил ожидание кода VK. Перешёл в начальный режим. Жду команд.')
    elif:
      # FIXME тут парсинг ссылки наверное должен быть
      m = re.search('https://oauth\.vk\.com/blank\.html#access_token=[a-z0-9]*&expires_in=[0-9]*&user_id=[0-9]*',cmd)
      if m:
        code = extract_unique_code(m.group(0))
        try:
          user = verifycode(code)
          send_message(room,'Вход выполнен в аккаунт {} {}!'.format(user['first_name'], user['last_name']))
          data[user][room]["vk_id"]=code
          # сохраняем на диск:
          save_data(data)
        except:
          send_message(room, 'Неверная ссылка, попробуйте ещё раз!')
    
  return True

def extract_unique_code(text):
    # Extracts the unique_code from the sent /start command.
    try:
        return text[45:].split('&')[0]
    except:
        return None


def verifycode(code):
    session = vk.Session(access_token=code)
    api = vk.API(session, v=VK_API_VERSION)
    return dict(api.account.getProfileInfo(fields=[]))


def info_extractor(info):
    info = info[-1].url[8:-1].split('.')
    return info

def login_command(user,room,cmd):
  global lock
  global data
  session_data=data[user][room]
  if "vk_id" not in session_data or session_data["vk_id"]=None:
    send_message(room,'Нажмите по ссылке ниже. Откройте её и согласитесь. После скопируйте текст из адресной строки и отправьте эту ссылку мне сюда')
    send_message(room,link)
    data[user][room]["state"]="wait_vk_id"
  else:
    send_message(room,'Вход уже выполнен!\n/logout для выхода.')

def check_thread(uid):
    for th in threading.enumerate():
        if th.getName() == 'vk' + str(uid):
            return False
    return True

def replace_shields(text):
    text = text.replace('&lt;', '<')
    text = text.replace('&gt;', '>')
    text = text.replace('&amp;', '&')
    text = text.replace('&copy;', '©')
    text = text.replace('&reg;', '®')
    text = text.replace('&laquo;', '«')
    text = text.replace('&raquo;', '«')
    text = text.replace('&deg;', '°')
    text = text.replace('&trade;', '™')
    text = text.replace('&plusmn;', '±')
    return text














def save_data(data):
  global log
  log.debug("save to data_file:%s"%conf.data_file)
  try:
    data_file=open(conf.data_file,"wb")
  except:
    log.error("open(%s) for writing"%conf.data_file)
    return False
    
  try:
    pickle.dump(data,data_file)
    data_file.close()
  except:
    log.error("pickle.dump to '%s'"%conf.data_file)
    return False
  return True

def load_data():
  global log
  tmp_data_file=conf.data_file
  reset=False
  if os.path.exists(tmp_data_file):
    log.debug("Загружаем файл промежуточных данных: '%s'" % tmp_data_file)
    data_file = open(tmp_data_file,'rb')
    try:
      data=pickle.load(data_file)
      data_file.close()
      log.debug("Загрузили файл промежуточных данных: '%s'" % tmp_data_file)
    except:
      log.warning("Битый файл сессии - сброс")
      reset=True
    if not "users" in data:
      log.warning("Битый файл сессии - сброс")
      reset=True
  else:
    log.warning("Файл промежуточных данных не существует")
    reset=True
  if reset:
    log.warning("Сброс промежуточных данных")
    data={}
    data["users"]={}
    save_data(data)
  return data


def send_html(room_id,html):
  global client
  global log

  room=None
  try:
    room = client.join_room(room_id)
  except MatrixRequestError as e:
    print(e)
    if e.code == 400:
      log.error("Room ID/Alias in the wrong format")
      return False
    else:
      log.error("Couldn't find room.")
      return False
  try:
    room.send_html(html)
  except:
    log.error("Unknown error at send message '%s' to room '%s'"%(html,room_id))
    return False
  return True

def send_message(room_id,message):
  global client
  global log

  #FIXME отладка парсера
  #print("message=%s"%message)
  #return True

  room=None
  try:
    room = client.join_room(room_id)
  except MatrixRequestError as e:
    print(e)
    if e.code == 400:
      log.error("Room ID/Alias in the wrong format")
      return False
    else:
      log.error("Couldn't find room.")
      return False
  try:
    room.send_text(message)
  except:
    log.error("Unknown error at send message '%s' to room '%s'"%(message,room_id))
    return False
  return True

# Called when a message is recieved.
def on_message(event):
    global client
    global log
    global lock
    print(json.dumps(event, indent=4, sort_keys=True,ensure_ascii=False))
    if event['type'] == "m.room.member":
        if event['membership'] == "join":
            print("{0} joined".format(event['content']['displayname']))
    elif event['type'] == "m.room.message":
        if event['content']['msgtype'] == "m.text":
            print("{0}: {1}".format(event['sender'], event['content']['body']))
            log.debug("try lock before process_command()")
            with lock:
              log.debug("success lock before process_command()")
              if process_command(event['sender'], event['room_id'],event['content']['body']) == False:
                log.error("error process command: '%s'"%event['content']['body'])
                return False
    else:
      print(event['type'])
    return True

def on_event(event):
    print("event:")
    print(event)
    print(json.dumps(event, indent=4, sort_keys=True,ensure_ascii=False))

def on_invite(room, event):
    global client
    global log

    if conf.debug:
      print("invite:")
      print("room_data:")
      print(room)
      print("event_data:")
      print(event)
      print(json.dumps(event, indent=4, sort_keys=True,ensure_ascii=False))

    # Просматриваем сообщения:
    for event_item in event['events']:
      if event_item['type'] == "m.room.join_rules":
        if event_item['content']['join_rule'] == "invite":
          # Приглашение вступить в комнату:
          room = client.join_room(room)
          room.send_text("Спасибо за приглашение! Недеюсь быть Вам полезным. :-)")
          room.send_text("Для справки по доступным командам - неберите: '!help' (или '!?', или '!h')")
          log.info("New user: '%s'"%event_item["sender"])

def exception_handler(e):
  global client
  global log
  log.error("main listener thread except. He must retrying...")
  print(e)
  log.info("wait 30 second before retrying...")
  time.sleep(30)

def main():
    global client
    global data
    global log
    global lock

    lock = threading.RLock()

    log.debug("try lock before main load_data()")
    with lock:
      log.debug("success lock before main load_data()")
      data=load_data()

    log.info("try init matrix-client")
    client = MatrixClient(conf.server)
    log.info("success init matrix-client")

    try:
        log.info("try login matrix-client")
        client.login_with_password(username=conf.username, password=conf.password)
        log.info("success login matrix-client")
    except MatrixRequestError as e:
        print(e)
        log.debug(e)
        if e.code == 403:
            log.error("Bad username or password.")
            sys.exit(4)
        else:
            log.error("Check your sever details are correct.")
            sys.exit(2)
    except MissingSchema as e:
        log.error("Bad URL format.")
        print(e)
        log.debug(e)
        sys.exit(3)

    log.info("try init listeners")
    client.add_listener(on_message)
    client.add_ephemeral_listener(on_event)
    client.add_invite_listener(on_invite)
    client.start_listener_thread(exception_handler=exception_handler)
    log.info("success init listeners")

    x=0
    log.info("enter main loop")
    while True:
      print("step %d"%x)
      x+=1
      time.sleep(10)
    log.info("exit main loop")


if __name__ == '__main__':
  log= logging.getLogger("MatrixVkBot")
  if conf.debug:
    log.setLevel(logging.DEBUG)
  else:
    log.setLevel(logging.INFO)

  # create the logging file handler
  fh = logging.FileHandler(conf.log_path)
  formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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
