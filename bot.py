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
import requests
import traceback
import vk
import ujson
import wget
from PIL import Image

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
VK_POLLING_VERSION = '3.0'

currentchat = {}



def process_command(user,room,cmd):
  global client
  global log
  global data
  answer=None
  session_data_room=None
  session_data_vk=None
  session_data_user=None

  if re.search('^@%s:.*'%conf.username, user.lower()) is not None:
    # отправленное нами же сообщение - пропускаем:
    log.debug("skip our message")
    return True

  if user not in data["users"]:
    data["users"][user]={}
  if "rooms" not in data["users"][user]:
    data["users"][user]["rooms"]={}
  if "vk" not in data["users"][user]:
    data["users"][user]["vk"]={}
  if room not in data["users"][user]["rooms"]:
    data["users"][user]["rooms"][room]={}
    data["users"][user]["rooms"][room]["state"]="listen_command"

  session_data_room=data["users"][user]["rooms"][room]
  session_data_vk=data["users"][user]["vk"]
  session_data_user=data["users"][user]

  cur_state=data["users"][user]["rooms"][room]["state"]

  log.debug("user=%s send command=%s"%(user,cmd))
  log.debug("cur_state=%s"%cur_state)

  # комната в режиме диалога - это созданная ботом комната. Она не принимает иных команд. Все команды принимает только комната с ботом
  if cur_state == "dialog":
    dialog=session_data_room["cur_dialog"]
    if vk_send_text(session_data_vk["vk_id"],dialog["id"],cmd,dialog["group"]) == False:
      log.error("error vk_send_text() for user %s"%user)
      send_message(room,"/me не смог отправить сообщение в ВК - ошибка АПИ")
      return False
    return True

  # Комната управления:
  # в любом состоянии отмена - всё отменяет:
  if re.search('^!stop$', cmd.lower()) is not None or \
      re.search('^!стоп$', cmd.lower()) is not None or \
      re.search('^!отмена$', cmd.lower()) is not None or \
      re.search('^!cancel$', cmd.lower()) is not None:
    data["users"][user]["rooms"][room]["state"]="listen_command"
    send_message(room,'Отменил текущий режим (%s) и перешёл в начальный режим ожидания команд. Жду команд.'%session_data_room["state"])
    return True
  elif re.search('^!стат$', cmd.lower()) is not None or \
      re.search('^!состояние$', cmd.lower()) is not None or \
      re.search('^!чат$', cmd.lower()) is not None or \
      re.search('^!chat$', cmd.lower()) is not None or \
      re.search('^!room$', cmd.lower()) is not None or \
      re.search('^!stat$', cmd.lower()) is not None:
    send_message(room,"Текущее состояние: %s"%session_data_room["state"])
    if session_data_room["state"]=="dialog":
      send_message(room,'Текущая комната: "%s"'%session_data_room["cur_dialog"]["title"])
    return True

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
    elif re.search('^!login$', cmd.lower()) is not None:
      return login_command(user,room,cmd)
    # dialogs
    elif re.search('^!dialogs$', cmd.lower()) is not None or \
      re.search('^!диалоги$', cmd.lower()) is not None or \
      re.search('^!чаты$', cmd.lower()) is not None or \
      re.search('^!комнаты$', cmd.lower()) is not None or \
      re.search('^!chats$', cmd.lower()) is not None or \
      re.search('^!rooms$', cmd.lower()) is not None or \
      re.search('^!d$', cmd.lower()) is not None:
      return dialogs_command(user,room,cmd)

  elif cur_state == "wait_vk_id":
    # парсинг ссылки
    m = re.search('https://oauth\.vk\.com/blank\.html#access_token=[a-z0-9]*&expires_in=[0-9]*&user_id=[0-9]*',cmd)
    if m:
      code = extract_unique_code(m.group(0))
      try:
        vk_user = verifycode(code)
      except:
        send_message(room, 'Неверная ссылка, попробуйте ещё раз!')
        log.warning("error auth url from user=%s"%user)
        return False
      send_message(room,'Вход выполнен в аккаунт {} {}!'.format(vk_user['first_name'], vk_user['last_name']))
      data["users"][user]["vk"]["vk_id"]=code
      data["users"][user]["rooms"][room]["state"]="listen_command"
      # сохраняем на диск:
      save_data(data)

  elif cur_state == "wait_vk_app_id":
    # парсинг ссылки
    try:
      vk_app_id = int(cmd)
    except:
      log.warning("error get VK app id from user=%s"%user)
      send_message(room, 'Я ожидаю от вас "ID приложения" по ссылке https://vk.com/apps?act=manage в настройках созданного Вами приложения. Код должен быть обычным числом. Или же отмените ожидание с помощью команд: !стоп, !stop, !отмена, !cancel')
      return False
    data["users"][user]["vk"]["vk_app_id"]=vk_app_id
    data["users"][user]["rooms"][room]["state"]="listen_command"
    # сохраняем на диск:
    save_data(data)
    # заново запускаем обработчик логина с уже обновлёнными данными:
    return login_command(user,room,cmd)

  elif cur_state == "wait_dialog_index":
    try:
      index=int(cmd)
    except:
      send_message(room,"пожалуйста, введите номер диалога или команды !stop, !отмена, !cancel")
      return True
    if index not in session_data_room["dialogs_list"]:
      send_message(room,"Неверный номер диалога, введите верный номер диалога или команды !stop, !отмена, !cancel")
      return True
    cur_dialog=session_data_room["dialogs_list"][index]
    found_room=find_bridge_room(user,cur_dialog["id"])
    if found_room != None:
      # Такая комната уже существует!
      log.info("room already exist for user '%s' for vk-dialog with vk-id '%d' ('%s')"%(user,cur_dialog["id"],cur_dialog["title"]))
      send_message(room,"У Вас уже есть комната (%s), связанная с этим пользователме - не создаю повторную. Позже будет добавлен функционал по чистке такх комнат."%found_room)
      send_message(room,"Перешёл в режим команд")
      data["users"][user]["rooms"][room]["state"]="listen_command"
      return False
    room_id=create_room(user,cur_dialog["title"] + " (VK)")
    if room_id==None:
      log.error("error create_room() for user '%s' for vk-dialog with vk-id '%d' ('%s')"%(user,cur_dialog["id"],cur_dialog["title"]))
      send_message(room,"Не смог создать дополнительную комнату в матрице: '%s' связанную с одноимённым диалогом в ВК"%cur_dialog["title"])
      send_message(room,"Перешёл в режим команд")
      data["users"][user]["rooms"][room]["state"]="listen_command"
      return False
    send_message(room,"Создал новую комнату матрицы с именем: '%s (VK)' связанную с одноимённым диалогом в ВК"%cur_dialog["title"])
    data["users"][user]["rooms"][room_id]={}
    data["users"][user]["rooms"][room_id]["cur_dialog"]=cur_dialog
    data["users"][user]["rooms"][room_id]["state"]="dialog"
    # сохраняем на диск:
    save_data(data)
    send_message(room,"Перешёл в режим команд")
    data["users"][user]["rooms"][room]["state"]="listen_command"

  return True

def find_bridge_room(user,vk_room_id):
  for room in data["users"][user]["rooms"]:
    if data["users"][user]["rooms"][room]["state"]=="dialog" and \
       data["users"][user]["rooms"][room]["cur_dialog"]["id"]==vk_room_id:
      log.info("found bridge for user '%s' with vk_id '%d'"%(user,vk_room_id))
      return room;
  return None

def get_new_vk_messages(user):
  global data
  global lock
  if "vk" not in data["users"][user]:
    return None
  if "vk_id" not in data["users"][user]["vk"]:
    return None
  session = get_session(data["users"][user]["vk"]["vk_id"])
  
  #log.debug("ts=%d, pts=%d"%(data["users"][user]["vk"]["ts"], data["users"][user]["vk"]["pts"]))

  api = vk.API(session, v=VK_POLLING_VERSION)
  try:
    ts_pts = ujson.dumps({"ts": data["users"][user]["vk"]["ts"], "pts": data["users"][user]["vk"]["pts"]})
    new = api.execute(code='return API.messages.getLongPollHistory({});'.format(ts_pts))
  except vk.api.VkAPIError:
    timeout = 3
    log.warning('Retrying getLongPollHistory in {} seconds'.format(timeout))
    time.sleep(timeout)
    with lock:
      data["users"][user]["vk"]["ts"], data["users"][user]["vk"]["pts"] = get_tses(session)
    ts_pts = ujson.dumps({"ts": data["users"][user]["vk"]["ts"], "pts": data["users"][user]["vk"]["pts"]})
    new = api.execute(code='return API.messages.getLongPollHistory({});'.format(ts_pts))

  #print("New data from VK:")
  #print(json.dumps(new, indent=4, sort_keys=True,ensure_ascii=False))

  msgs = new['messages']
  with lock:
    data["users"][user]["vk"]["pts"] = new["new_pts"]
  count = msgs[0]

  res = None
  if count == 0:
    pass
  else:
    res={}
    res["messages"] = msgs[1:]
    res["profiles"] = new["profiles"]
  return res


def extract_unique_code(text):
    # Extracts the unique_code from the sent /start command.
    try:
        return text[45:].split('&')[0]
    except:
        return None

def get_session(token):
    return vk.Session(access_token=token)

def get_tses(session):
    api = vk.API(session, v=VK_POLLING_VERSION)
    ts = api.messages.getLongPollServer(need_pts=1)
    return ts['ts'], ts['pts']

def verifycode(code):
    session = vk.Session(access_token=code)
    api = vk.API(session, v=VK_API_VERSION)
    return dict(api.account.getProfileInfo(fields=[]))


def info_extractor(info):
    info = info[-1].url[8:-1].split('.')
    return info

def vk_send_text(vk_id, chat_id, message, group=False, forward_messages=None):
  global log
  try:
    session = get_session(vk_id)
    api = vk.API(session, v=VK_API_VERSION)
    if group:
      api.messages.send(chat_id=chat_id, message=message, forward_messages=forward_messages)
    else:
      api.messages.send(user_id=chat_id, message=message, forward_messages=forward_messages)
  except:
    log.error("vk_send_text API or network error")
    return False
  return True

def dialogs_command(user,room,cmd):
  global log
  global lock
  global data
  log.debug("dialogs_command()")
  session_data_room=data["users"][user]["rooms"][room]
  session_data_vk=data["users"][user]["vk"]
  if "vk_id" not in session_data_vk or session_data_vk["vk_id"]==None:
    send_message(room,'Вы не вошли в ВК - используйте !login для входа')
    return True
  vk_id=session_data_vk["vk_id"]
  dialogs=get_dialogs(vk_id)
  if dialogs == None:
    send_message(room,'Не смог получить спиоок бесед из ВК - попробуйте позже :-(')
    log.error("get_dialogs() for user=%s"%user)
    return False

  # Формируем список диалогов:
  send_message(room,"Выберите диалог:")
  message=""
  index=1
  dialogs_list={}
  for item in dialogs:
    dialogs_list[index]=item
    message+="%d. "%index
    message+=item["title"]
    message+="\n"
    index+=1
  send_message(room,message)
  data["users"][user]["rooms"][room]["state"]="wait_dialog_index"
  data["users"][user]["rooms"][room]["dialogs_list"]=dialogs_list
  return True

def get_dialogs(vk_id):
  global log
  # Формируем структуры:
  order = []
  users_ids = []
  group_ids = []
  positive_group_ids = []
  try:
    api = vk.API(get_session(vk_id), v=VK_API_VERSION)
    dialogs = api.messages.getDialogs(count=200)
  except:
    log.error("get dialogs from VK API")
    return None
  for chat in dialogs[1:]:
    if 'chat_id' in chat:
      chat['title'] = replace_shields(chat['title'])
      order.append({'title': chat['title'], 'id': chat['chat_id'], 'group': True})
    elif chat['uid'] > 0:
      order.append({'title': None, 'id': chat['uid'], 'group': False})
      users_ids.append(chat['uid'])
    elif chat['uid'] < 0:
      order.append({'title': None, 'id': chat['uid'],'group': False})
      group_ids.append(chat['uid'])

  for g in group_ids:
    positive_group_ids.append(str(g)[1:])

  if users_ids:
    users = api.users.get(user_ids=users_ids, fields=['first_name', 'last_name', 'uid'])
  else:
    users = []

  if positive_group_ids:
    groups = api.groups.getById(group_ids=positive_group_ids, fields=[])
  else:
    groups = []

  for output in order:
    if output['title'] == ' ... ' or not output['title']:
      if output['id'] > 0:
        for x in users:
          if x['uid'] == output['id']:
            output['title'] = '{} {}'.format(x['first_name'], x['last_name'])
            break
      else:
        for f in groups:
          if str(f['gid']) == str(output['id'])[1:]:
            output['title'] = '{}'.format(f['name'])
            break
  return order

def login_command(user,room,cmd):
  global lock
  global data
  log.debug("login_command()")
  session_data_vk=data["users"][user]["vk"]
  if "vk_app_id" not in session_data_vk or session_data_vk["vk_app_id"]==None:
    send_message(room,'Пройдите по ссылке https://vk.com/editapp?act=create и создаqnt своё Standalone-приложение, затем во вкладке Настройки переведите Состояние в "Приложение включено" и "видно всем", не забудьте сохранить изменения!')
    send_message(room,'После этого скопируйте "ID приложения" в настройках у созданного перед этим приложения по ссылке https://vk.com/apps?act=manage  и пришлите мне сюда в чат. Я жду :-)')
    data["users"][user]["rooms"][room]["state"]="wait_vk_app_id"
  elif "vk_id" not in session_data_vk or session_data_vk["vk_id"]==None:
    send_message(room,'Нажмите по ссылке ниже. Откройте её и согласитесь. После скопируйте текст из адресной строки и отправьте эту ссылку мне сюда')
    link = 'https://oauth.vk.com/authorize?client_id={}&' \
           'display=page&redirect_uri=https://oauth.vk.com/blank.html&scope=friends,messages,offline,docs,photos,video' \
           '&response_type=token&v={}'.format(session_data_vk["vk_app_id"], VK_API_VERSION)
    send_message(room,link)
    data["users"][user]["rooms"][room]["state"]="wait_vk_id"
  else:
    send_message(room,'Вход уже выполнен!\n/logout для выхода.')




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

def create_room(matrix_uid, room_name):
  global log
  global client

  # сначала спрашиваем у сервера, есть ли такой пользователь (чтобы не создавать просто так комнату):
  try:
    response = client.api.get_display_name(matrix_uid)
  except MatrixRequestError as e:
    log.error("Couldn't get user display name - may be no such user on server? username = '%s'"%matrix_uid)
    log.error("skip create room for user '%s' - need admin!"%matrix_uid)
    return None
  log.debug("Success get display name '%s' for user '%s' - user exist. Try create room for this is user"%(response,matrix_uid))

  try:
    room=client.create_room(is_public=False, invitees=None)
  except MatrixRequestError as e:
    print(e)
    if e.code == 400:
      log.error("Room ID/Alias in the wrong format")
      return None
    else:
      log.error("Couldn't create room.")
      return None
  log.debug("New room created. room_id='%s'"%room.room_id)

  # приглашаем пользователя в комнату:
  try:
    response = client.api.invite_user(room.room_id,matrix_uid)
  except MatrixRequestError as e:
    print(e)
    log.error("Can not invite user '%s' to room '%s'"%(matrix_uid,room.room_id))
    try:
      # Нужно выйти из комнаты:
      log.info("Leave from room: '%s'"%(room.room_id))
      response = client.api.leave_room(room.room_id)
    except:
      log.error("error leave room: '%s'"%(room.room_id))
      return None
    try:
      # И забыть её:
      log.info("Forgot room: '%s'"%(room.room_id))
      response = client.api.forget_room(room.room_id)
    except:
      log.error("error leave room: '%s'"%(room.room_id))
      return None
    return None
  log.debug("success invite user '%s' to room '%s'"%(matrix_uid,room.room_id))

  try:
    room.set_room_name(room_name)
  except:
    log.error("error set_room_name room_id='%s' to '%s'"%(room.room_id, room_name))
  return room.room_id;

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
    global lock
    global data

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
          user=event_item["sender"]
          log.info("New user: '%s'"%user)
          # Прописываем системную группу для пользователя (группа, в которую будут сыпаться системные сообщения от бота и где он будет слушать команды):
          with lock:
            if "users" not in data:
              data["users"]={}
            if user not in data["users"]:
              data["users"][user]={}
            if "matrix_bot_data" not in data["users"][user]:
              data["users"][user]["matrix_bot_data"]={}
            if "control_room" not in data["users"][user]["matrix_bot_data"]:
              data["users"][user]["matrix_bot_data"]["control_room"]=room.room_id
            save_data(data)



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
        token = client.login(username=conf.username, password=conf.password,device_id=conf.device_id)
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
      # Запускаем незапущенные потоки - при старте бота или же если подключился новый пользователь:
      num=start_vk_polls()
      if num > 0:
        log.info("start_vk_polls() start %d new poller proccess for receive VK messages"%num)
      time.sleep(5)
    log.info("exit main loop")

def check_thread_exist(vk_id):
    for th in threading.enumerate():
        if th.getName() == 'vk' + str(vk_id):
            return True
    return False

# запуск потоков получения сообщений:
def start_vk_polls():
  global data
  global lock
  global log

  started=0
  
  with lock:
    for user in data["users"]:
      if "vk" in data["users"][user] and "vk_id" in data["users"][user]["vk"]:
        vk_data=data["users"][user]["vk"]
        vk_id=data["users"][user]["vk"]["vk_id"]
        if check_thread_exist(vk_id) == False:
          t = threading.Thread(name='vk' + str(vk_id), target=vk_receiver_thread, args=(user,))
          t.setDaemon(True)
          t.start()
          started+=1
  return started

def get_name_from_url(url):
  return re.sub('.*/', '', url)

def send_file_to_matrix(room,sender_name,attachment):
  src=attachment["doc"]['url']
  size=attachment["doc"]['size']
  
  image_data=get_data_from_url(src)
  if image_data==None:
    log.error("get image from url: %s"%src)
    return False

  # определение типа:
  ext=attachment["doc"]["ext"]
  mimetype="text/plain"
  if ext == "txt":
    mimetype="text/plain"
  elif ext == "doc": 
    mimetype="application/msword"
  elif ext == "xls": 
    mimetype="application/vnd.ms-excel"
  elif ext == "odt": 
    mimetype="application/vnd.oasis.opendocument.text"
  elif ext == "ods": 
    mimetype="application/vnd.oasis.opendocument.spreadsheet"
    
  mxc_url=upload_file(image_data,mimetype)
  if mxc_url == None:
    log.error("uload file to matrix server")
    return False
  log.debug("send file 1")
  if "title" in attachment["doc"]:
    file_name=attachment["doc"]["title"]
  else:
    file_name=get_name_from_url(src)

  if sender_name!=None:
    file_name=sender_name+' прислал файл: '+file_name

  if matrix_send_file(room,mxc_url,file_name,mimetype,size) == False:
    log.error("send file to room")
    return False

def send_geo_to_matrix(room,sender_name,geo):
  coordinates=geo["coordinates"]
  lat=coordinates.split(' ')[0]
  lon=coordinates.split(' ')[1]
  place_name=geo["place"]["title"]
  #src="http://staticmap.openstreetmap.de/staticmap.php?center=40.714728,-73.998672&zoom=14&size=865x512&maptype=mapnik"
  geo_url="https://opentopomap.org/#marker=13/%(lat)s/%(lon)s"%{"lat":lat,"lon":lon}

  if sender_name!=None:
    text = sender_name + ' прислал местоположение (%s, %s):\n'%(lat,lon) + geo_url
  else:
    text = 'местоположение (%s, %s):\n'%(lat,lon) + geo_url
  send_message(room,text)
  return True
  
  # FIXME добавить превью карты:
  image_data=get_data_from_url(src)
  if image_data==None:
    log.error("get image from url: %s"%src)
    return False

  # FIXME добавить определение типа:
  mimetype="image/jpeg"
  size=len(image_data)
    
  mxc_url=upload_file(image_data,mimetype)
  if mxc_url == None:
    log.error("uload file to matrix server")
    return False
  log.debug("send file 1")
  if "title" in attachment["photo"]:
    file_name=attachment["photo"]["title"]
  else:
    file_name=get_name_from_url(src)


  if matrix_send_image(room,mxc_url,file_name,height,width,mimetype,size) == False:
    log.error("send file to room")
    return False

def send_photo_to_matrix(room,sender_name,attachment):
  src=attachment["photo"]['src_small']
  if "src" in attachment["photo"]:
    src=attachment["photo"]["src"]
  if "src_big" in attachment["photo"]:
    src=attachment["photo"]["src_big"]
  if "src_xbig" in attachment["photo"]:
    src=attachment["photo"]["src_xbig"]
  if "src_xxbig" in attachment["photo"]:
    src=attachment["photo"]["src_xxbig"]
  width=attachment["photo"]["width"]
  height=attachment["photo"]["height"]
  
  image_data=get_data_from_url(src)
  if image_data==None:
    log.error("get image from url: %s"%src)
    return False

  # FIXME добавить определение типа:
  mimetype="image/jpeg"
  size=len(image_data)
    
  mxc_url=upload_file(image_data,mimetype)
  if mxc_url == None:
    log.error("uload file to matrix server")
    return False
  log.debug("send file 1")
  if "title" in attachment["photo"]:
    file_name=attachment["photo"]["title"]
  else:
    file_name=get_name_from_url(src)

  if sender_name!=None:
    file_name=sender_name+' прислал изображение: '+file_name

  if matrix_send_image(room,mxc_url,file_name,height,width,mimetype,size) == False:
    log.error("send file to room")
    return False

def send_video_to_matrix(room,sender_name,attachment):
  src=attachment["video"]['image']
  
  image_data=get_data_from_url(src)
  if image_data==None:
    log.error("get image from url: %s"%src)
    return False

  # FIXME добавить определение типа:
  mimetype="image/jpeg"
  size=len(image_data)
    
  mxc_url=upload_file(image_data,mimetype)
  if mxc_url == None:
    log.error("uload file to matrix server")
    return False
  log.debug("send file 1")
  if "title" in attachment["video"]:
    file_name=attachment["video"]["title"]+"(превью видео).jpg"
  else:
    file_name=get_name_from_url(src)

  if sender_name!=None:
    file_name=sender_name+' прислал изображение: '+file_name

  if matrix_send_image(room,mxc_url,file_name,height=0,width=0,mimetype=mimetype,size=size) == False:
    log.error("send file to room")
    return False
  video_url="https://vk.com/video%(owner_id)s_%(vid)s"%{"owner_id":attachment["video"]["owner_id"],"vid":attachment["video"]["vid"]}
  return send_message(room,"Ссылка на просмотр потокового видео: %s"%video_url)

def send_audio_to_matrix(room,sender_name,attachment):
  src=attachment["audio"]['url']
  size=0
  duration=attachment["audio"]["duration"]
  file_name=attachment["audio"]["title"]+" ("+attachment["audio"]["title"]+").mp3"
  # FIXME добавить определение типа:
  mimetype="audio/mpeg"
  
  audio_data=get_data_from_url(src)
  if audio_data==None:
    log.error("get image from url: %s"%src)
    return False
    
  mxc_url=upload_file(audio_data,mimetype)
  if mxc_url == None:
    log.error("uload file to matrix server")
    return False
  log.debug("send file 1")

  if sender_name!=None:
    file_name=sender_name+' прислал песню: '+file_name

  if matrix_send_audio(room,mxc_url,file_name,mimetype,size,duration) == False:
    log.error("send file to room")
    return False

def send_voice_to_matrix(room,sender_name,attachment):
  src=attachment["doc"]['url']
  size=attachment["doc"]["size"]
  file_name=attachment["doc"]["title"]
  # FIXME добавить определение типа:
  mimetype="audio/ogg"
  
  audio_data=get_data_from_url(src)
  if audio_data==None:
    log.error("get voice from url: %s"%src)
    return False
    
  mxc_url=upload_file(audio_data,mimetype)
  if mxc_url == None:
    log.error("uload file to matrix server")
    return False
  log.debug("send file 1")

  if sender_name!=None:
    file_name=sender_name+' прислал изображение: '+file_name

  if matrix_send_audio(room,mxc_url,file_name,mimetype,size,duration=0) == False:
    log.error("send file to room")
    return False

def send_attachments(room,sender_name,attachments):
  for attachment in attachments:
    # Отправляем фото:
    if attachment["type"]=="photo":
      if send_photo_to_matrix(room,sender_name,attachment)==False:
        log.error("send_photo_to_matrix()")
        return False
    # Отправляем фото:
    elif attachment["type"]=="audio":
      if send_audio_to_matrix(room,sender_name,attachment)==False:
        log.error("send_audio_to_matrix()")
    # Отправляем видео:
    elif attachment["type"]=="video":
      if send_video_to_matrix(room,sender_name,attachment)==False:
        log.error("send_video_to_matrix()")
    # документы:
    elif attachment["type"]=="doc":
      if "ext" in attachment["doc"] and attachment["doc"]["ext"]=="ogg":
        # голосовое сообщение:
        if send_voice_to_matrix(room,sender_name,attachment)==False:
          log.error("send_voice_to_matrix()")
          return False
      else:
        # иные прикреплённые документы:
        if send_file_to_matrix(room,sender_name,attachment)==False:
          log.error("send_file_to_matrix()")

    else:
      log.error("unknown attachment type - skip. attachment type=%s"%attachment["type"])
  return True

def get_data_from_url(url):
  global log
  try:
    response = requests.get(url, stream=True)
    data = response.content      # a `bytes` object
  except:
    log.error("fetch data from url: %s"%url)
    return None
  return data


def matrix_send_audio(room_id,url,name,mimetype="audio/mpeg",size=0,duration=0):
  global log
  global client
  ret=None
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
  audioinfo={}
  audioinfo["mimetype"]=mimetype
  audioinfo["size"]=size
  audioinfo["duration"]=duration
  try:
    log.debug("send file 2")
    #ret=room.send_image(url,name,imageinfo)
    ret=room.send_audio(url,name,audioinfo=audioinfo)
    log.debug("send file 3")
  except MatrixRequestError as e:
    print(e)
    if e.code == 400:
      log.error("ERROR send audio with mxurl=%s"%url)
      return False
    else:
      log.error("Couldn't send audio (unknown error) with mxurl=%s"%url)
      return False
  return True

def matrix_send_image(room_id,url,name,height,width,mimetype,size):
  global log
  global client
  ret=None
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
  imageinfo={}
  imageinfo["mimetype"]=mimetype
  imageinfo["size"]=size
  imageinfo["h"]=height
  imageinfo["w"]=width
  try:
    log.debug("send file 2")
    ret=room.send_image(url,name,imageinfo=imageinfo)
    #ret=room.send_image(url,name)
    log.debug("send file 3")
  except MatrixRequestError as e:
    print(e)
    if e.code == 400:
      log.error("ERROR send image with mxurl=%s"%url)
      return False
    else:
      log.error("Couldn't send image (unknown error) with mxurl=%s"%url)
      return False
  return True

def matrix_send_file(room_id,url,name,mimetype,size):
  global log
  global client
  ret=None
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
  fileinfo={}
  fileinfo["mimetype"]=mimetype
  fileinfo["size"]=size
  try:
    log.debug("send file 2")
    ret=room.send_file(url,name,fileinfo=fileinfo)
    log.debug("send file 3")
  except MatrixRequestError as e:
    print(e)
    if e.code == 400:
      log.error("ERROR send file with mxurl=%s"%url)
      return False
    else:
      log.error("Couldn't send file (unknown error) with mxurl=%s"%url)
      return False
  return True

def upload_file(content,content_type,filename=None):
  global log
  global client
  log.debug("upload file 1")
  ret=None
  try:
    log.debug("upload file 2")
    ret=client.upload(content,content_type)
    log.debug("upload file 3")
  except MatrixRequestError as e:
    print(e)
    if e.code == 400:
      log.error("ERROR upload file")
      return None
    else:
      log.error("Couldn't upload file (unknown error)")
      return None
  return ret

def proccess_vk_message(bot_control_room,room,sender_name,m):
  global data
  global lock
  global log
  send_status=False
  text=""
  if len(m["body"])>0:
    if 'fwd_messages' in m:
      # это ответ на сообщение - добавляем текст сообщения, на который дан ответ:
      for fwd in m['fwd_messages']:
        fwd_uid=fwd['uid']
        fwd_text=fwd['body']
        # TODO получить ФИО авторов перенаправляемых сообщений
        text+="> <%(fwd_user)s> %(fwd_text)s\n\n" % {"fwd_user":fwd_uid, "fwd_text":fwd_text}
      text+=m["body"]
    else:
      if sender_name!=None:
        text="<strong>%s</strong>: %s"%(sender_name,m["body"])
      else:
        text=m["body"]
    send_html(room,text)
    send_status=True
  # отправка вложений:
  if "attachments" in m:
    if send_attachments(room,sender_name,m["attachments"])==False:
      send_message(room,'Ошибка: не смог отправить вложения из исходного сообщения ВК - см. логи')
      send_message(bot_control_room,'Ошибка: не смог отправить вложения из исходного сообщения ВК - вложения были от: %s'%sender_name)
    else:
      send_status=True
  # отправка местоположения:
  if "geo" in m:
    if send_geo_to_matrix(room,sender_name,m["geo"])==False:
      send_message(room,'Ошибка: не смог отправить местоположение из исходного сообщения ВК - см. логи')
      send_message(bot_control_room,'Ошибка: не смог отправить местоположение из исходного сообщения ВК - вложения были от: %s'%sender_name)
    else:
      send_status=True
  return send_status


def vk_receiver_thread(user):
  global data
  global lock
  global log
  log.info("start new vk_receiver_thread() for user='%s'"%user)
  # Обновляем временные метки:
  session = get_session(data["users"][user]["vk"]["vk_id"])
  with lock:
    data["users"][user]["vk"]["ts"], data["users"][user]["vk"]["pts"] = get_tses(session)
    bot_control_room=data["users"][user]["matrix_bot_data"]["control_room"]

  while True:
    res=get_new_vk_messages(user)
    if res != None:
      for m in res["messages"]:
        if m["out"]==1:
          log.debug("receive our message - skip")
        else:
          # FIXME
          print("Receive message from VK:")
          print(json.dumps(m, indent=4, sort_keys=True,ensure_ascii=False))
          send_status=False
          for room in data["users"][user]["rooms"]:
            if "cur_dialog" in data["users"][user]["rooms"][room]:
              sender_name=None
              vk_room_id=m["uid"]
              # проверяем, групповой ли это чат:
              if "chat_id" in m:
                # групповой чат:
                vk_room_id = m["chat_id"]

              if data["users"][user]["rooms"][room]["cur_dialog"]["id"] == vk_room_id:
                # проверяем, групповой ли это чат:
                if "chat_id" in m:
                  # Если это групповой чат - нужно добавить имя отправителя, т.к. их там может быть много:
                  # Ищем отправителя в профилях полученного сообщения:
                  for profile in res["profiles"]:
                    if profile["uid"]==m["uid"]:
                      sender_name="%s %s"%(profile["first_name"],profile["last_name"])
                send_status=proccess_vk_message(bot_control_room,room,sender_name,m)

          if send_status==False:
            # Не нашли созданной комнаты, чтобы отправить туда сообщение.
            # Нужно самим создать комнату и отправить туда сообщение.

            # Нужно найти имя диалога:
            dialogs=get_dialogs(data["users"][user]["vk"]["vk_id"])
            if dialogs == None:
              log.error("get_dialogs for user '%s'"%user)
              send_message(bot_control_room,'Не смог получить спиоок бесед из ВК - поэтому не смог создать новую комнату в связи с пришедшикомнату попробуйте позже :-(')

            cur_dialog=None
            for item in dialogs:
              print("item:")
              print(item)
              if "chat_id" in m:
                # значит ищем среди групп:
                if item["group"] == True and item["id"] == m["chat_id"]:
                  room_name=item["title"]
                  cur_dialog=item
              else:
                # значит ищем среди чатов:
                if item["group"] == False and item["id"] == m["uid"]:
                  cur_dialog=item
            if cur_dialog == None:
              log.error("can not found VK sender in dialogs - logic error - skip")
              send_message(bot_control_room,"Не смог найти диалог для вновь-поступившего сообщения. vk_uid отправителя='%d'"%m["uid"])
              send_message(bot_control_room,"Сообщение было: '%s'"%m["body"])
              continue
            
            room_id=create_room(user,cur_dialog["title"] + " (VK)")
            if room_id==None:
              log.error("error create_room() for user '%s' for vk-dialog with vk-id '%d' ('%s')"%(user,cur_dialog["id"],cur_dialog["title"]))
              send_message(bot_control_room,"Не смог создать дополнительную комнату в матрице: '%s' связанную с одноимённым диалогом в ВК"%cur_dialog["title"])
              continue
            send_message(bot_control_room,"Создал новую комнату матрицы с именем: '%s (VK)' связанную с одноимённым диалогом в ВК"%cur_dialog["title"])
            with lock:
              data["users"][user]["rooms"][room_id]={}
              data["users"][user]["rooms"][room_id]["cur_dialog"]=cur_dialog
              data["users"][user]["rooms"][room_id]["state"]="dialog"
              # сохраняем на диск:
              save_data(data)
            # отправляем текст во вновь созданную комнату:
            sender_name=None
            if "chat_id" in m:
              # Групповой чат - добавляем имя отправителя:
              # Ищем отправителя в профилях полученного сообщения:
              for profile in res["profiles"]:
                if profile["uid"]==m["uid"]:
                  sender_name="<strong>%s %s:</strong> "%(profile["first_name"],profile["last_name"])

            send_status=proccess_vk_message(bot_control_room,room,sender_name,m)

      # FIXME 
      time.sleep(2)

  return True

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
