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
import random
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

VK_API_VERSION = '5.95'
VK_POLLING_VERSION = '5.95'

currentchat = {}

def process_command(user,room,cmd,formated_message=None,format_type=None,reply_to_id=None,file_url=None,file_type=None):
  global client
  global log
  global data
  log.debug("=start function=")
  answer=None
  session_data_room=None
  session_data_vk=None
  session_data_user=None

  if reply_to_id!=None and format_type=="org.matrix.custom.html" and formated_message!=None:
    # разбираем, чтобы получить исходное сообщение и ответ
    source_message=re.sub('<mx-reply><blockquote>.*<\/a><br>','', formated_message)
    source_message=re.sub('</blockquote></mx-reply>.*','', source_message)
    source_cmd=re.sub(r'.*</blockquote></mx-reply>','', formated_message.replace('\n',''))
    log.debug("source=%s"%source_message)
    log.debug("cmd=%s"%source_cmd)
    cmd="> %s\n%s"%(source_message,source_cmd)

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
    bot_control_room=data["users"][user]["matrix_bot_data"]["control_room"]
    dialog=session_data_room["cur_dialog"]
    if file_type!=None and file_url!=None:
      # отправка файла:
      if re.search("^image",file_type)!=None:
        # Отправка изображения из матрицы:
        photo_data=get_file(file_url)
        if photo_data==None:
          log.error("error get file by mxurl=%s"%file_url)
          bot_system_message(user,'Ошибка: не смог получить вложение из матрицы по mxurl=%s'%file_url)
          send_message(room,'Ошибка: не смог получить вложение из матрицы по mxurl=%s'%file_url)
          return False
        if vk_send_photo(session_data_vk["vk_id"],dialog["id"],cmd,photo_data,dialog["type"]) == False:
          log.error("error vk_send_photo() for user %s"%user)
          send_message(room,"не смог отправить фото в ВК - ошибка АПИ")
          return False
      elif re.search("^video",file_type)!=None:
        # Отправка видео из матрицы:
        video_data=get_file(file_url)
        if video_data==None:
          log.error("error get file by mxurl=%s"%file_url)
          bot_system_message(user,'Ошибка: не смог получить вложение из матрицы по mxurl=%s'%file_url)
          send_message(room,'Ошибка: не смог получить вложение из матрицы по mxurl=%s'%file_url)
          return False
        if vk_send_video(session_data_vk["vk_id"],dialog["id"],cmd,video_data,dialog["type"]) == False:
          log.error("error vk_send_video() for user %s"%user)
          send_message(room,"не смог отправить видео в ВК - ошибка АПИ")
          return False
      else:
        # Отправка простого файла из матрицы:
        doc_data=get_file(file_url)
        if doc_data==None:
          log.error("error get file by mxurl=%s"%file_url)
          bot_system_message(user,'Ошибка: не смог получить вложение из матрицы по mxurl=%s'%file_url)
          send_message(room,'Ошибка: не смог получить вложение из матрицы по mxurl=%s'%file_url)
          return False
        if vk_send_doc(session_data_vk["vk_id"],dialog["id"],cmd,doc_data,dialog["type"]) == False:
          log.error("error vk_send_doc() for user %s"%user)
          send_message(room,"не смог отправить файл в ВК - ошибка АПИ")
          return False
    else:
      # отправка текста:
      if vk_send_text(session_data_vk["vk_id"],dialog["id"],cmd,dialog["type"]) == False:
        log.error("error vk_send_text() for user %s"%user)
        send_message(room,"не смог отправить сообщение в ВК - ошибка АПИ")
        return False
    # Сохраняем последнюю введённую пользователем команду:
    log.debug("set last message as: %s"%cmd)
    data["users"][user]["rooms"][room]["last_matrix_owner_message"]=cmd
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
!logout - выйти из ВК (пока не реализовано)
!search - поиск диалогов в ВК (пока не реализовано)
!dialogs - список всех ваших диалогов в ВК. В ответном сообщении Вам потребуется ввести номер диалога, чтобы начать общение в этом диалоге через матрицу.
!rooms - список соответствий диалогов ВК и ваших комнат
!delete room_id - удалить соответствение диалога ВК и комнаты MATRIX. Диалог в ВК останется и если придёт новое сообщение в нём - то бот заново создаст у вас комнту и соответстие. И вы получите сообщение из ВК.
!stat - текущее состояние комнаты
      """ 
      return send_message(room,answer)

    # login
    elif re.search('^!login$', cmd.lower()) is not None:
      return login_command(user,room,cmd)
    # dialogs
    elif re.search('^!dialogs$', cmd.lower()) is not None or \
      re.search('^!диалоги$', cmd.lower()) is not None or \
      re.search('^!d$', cmd.lower()) is not None:
      return dialogs_command(user,room,cmd)

    elif re.search('^!rooms$', cmd.lower()) is not None or \
      re.search('^!комнаты$', cmd.lower()) is not None:
      return rooms_command(user,room,cmd)

    elif re.search('^!delete .*', cmd.lower()) is not None:
      return delete_room_association(user,room,cmd)

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
    data["users"][user]["rooms"][room_id]["last_matrix_owner_message"]=""
    data["users"][user]["rooms"][room_id]["cur_dialog"]=cur_dialog
    data["users"][user]["rooms"][room_id]["state"]="dialog"
    # сохраняем на диск:
    save_data(data)
    send_message(room,"Перешёл в режим команд")
    data["users"][user]["rooms"][room]["state"]="listen_command"

  return True

def find_bridge_room(user,vk_room_id):
  global log
  log.debug("=start function=")
  for room in data["users"][user]["rooms"]:
    if data["users"][user]["rooms"][room]["state"]=="dialog" and \
       data["users"][user]["rooms"][room]["cur_dialog"]["id"]==vk_room_id:
      log.info("found bridge for user '%s' with vk_id '%d'"%(user,vk_room_id))
      return room;
  return None

def get_new_vk_messages(user):
  global data
  global lock
  global log
  log.debug("=start function=")
  if "vk" not in data["users"][user]:
    return None
  if "vk_id" not in data["users"][user]["vk"]:
    return None
  session = get_session(data["users"][user]["vk"]["vk_id"])
  
  #log.debug("ts=%d, pts=%d"%(data["users"][user]["vk"]["ts"], data["users"][user]["vk"]["pts"]))

  api = vk.API(session, v=VK_POLLING_VERSION)
  try:
    ts_pts = ujson.dumps({"ts": data["users"][user]["vk"]["ts"], "pts": data["users"][user]["vk"]["pts"]})
    #ts_pts = ujson.dumps({"ts": data["users"][user]["vk"]["ts"], "pts": data["users"][user]["vk"]["pts"],"wait":25})
    new = api.execute(code='return API.messages.getLongPollHistory({});'.format(ts_pts))
  except vk.api.VkAPIError:
    timeout = 3
    log.warning('Retrying getLongPollHistory in {} seconds'.format(timeout))
    time.sleep(timeout)
    with lock:
      data["users"][user]["vk"]["ts"], data["users"][user]["vk"]["pts"] = get_tses(session)
    ts_pts = ujson.dumps({"ts": data["users"][user]["vk"]["ts"], "pts": data["users"][user]["vk"]["pts"]})
    new = api.execute(code='return API.messages.getLongPollHistory({});'.format(ts_pts))

  log.debug("New data from VK:")
  log.debug(json.dumps(new, indent=4, sort_keys=True,ensure_ascii=False))

  msgs = new['messages']
  with lock:
    data["users"][user]["vk"]["pts"] = new["new_pts"]
  count = msgs["count"]

  res = None
  if count == 0:
    pass
  else:
    res={}
    res["messages"] = msgs["items"]
    res["profiles"] = new["profiles"]
  return res


def extract_unique_code(text):
  global log
  log.debug("=start function=")
  # Extracts the unique_code from the sent /start command.
  try:
      return text[45:].split('&')[0]
  except:
      return None

def get_session(token):
  global log
  log.debug("=start function=")
  return vk.Session(access_token=token)

def get_tses(session):
  global log
  log.debug("=start function=")
  api = vk.API(session, v=VK_POLLING_VERSION)
  ts = api.messages.getLongPollServer(need_pts=1)
  print("ts=",ts)
#    sys.exit()
  return ts['ts'], ts['pts']

def verifycode(code):
  global log
  log.debug("=start function=")
  session = vk.Session(access_token=code)
  api = vk.API(session, v=VK_API_VERSION)
  return dict(api.account.getProfileInfo(fields=[]))


def info_extractor(info):
  global log
  log.debug("=start function=")
  info = info[-1].url[8:-1].split('.')
  return info

def vk_send_text(vk_id, chat_id, message, chat_type="user", forward_messages=None):
  global log
  log.debug("=start function=")
  try:
    random_id=random.randint(0,4294967296)
    session = get_session(vk_id)
    api = vk.API(session, v=VK_API_VERSION)
    if chat_type!="user":
      api.messages.send(peer_id=chat_id, random_id=random_id,  message=message, forward_messages=forward_messages)
    else:
      api.messages.send(user_id=chat_id, random_id=random_id, message=message, forward_messages=forward_messages)
  except:
    log.error("vk_send_text API or network error")
    return False
  return True

def vk_send_video(vk_id, chat_id, name, video_data, chat_type="user"):
  global log
  log.debug("=start function=")
  random_id=random.randint(0,4294967296)
  try:
    session = get_session(vk_id)
    api = vk.API(session, v=VK_API_VERSION)
    # получаем адрес загрузки:
    save_response=api.video.save(name=name)
    log.debug("api.video.save return:")
    log.debug(save_response)
    url = save_response['upload_url']
    files = {'video_file': (name,video_data,'multipart/form-data')}
    r = requests.post(url, files=files)
    log.debug("requests.post return: %s"%r.text)
    ret=json.loads(r.text)
    attachment_str="video%d_%d"%(ret['owner_id'],ret['video_id'])
    if chat_type!="user":
      ret=api.messages.send(chat_id=chat_id, random_id=random_id, message=name,attachment=(attachment_str))
      log.debug("api.messages.send return:")
      log.debug(ret)
    else:
      ret=api.messages.send(user_id=chat_id, random_id=random_id, message=name,attachment=(attachment_str))
      log.debug("api.messages.send return:")
      log.debug(ret)
  except:
    log.error("vk_send_video API or network error")
    return False
  return True

def vk_send_doc(vk_id, chat_id, name, doc_data, chat_type="user"):
  global log
  log.debug("=start function=")
  random_id=random.randint(0,4294967296)
  try:
    session = get_session(vk_id)
    api = vk.API(session, v=VK_API_VERSION)
    # получаем адрес загрузки:
    response=api.docs.getMessagesUploadServer()
    log.debug("api.docs.getMessagesUploadServer return:")
    log.debug(response)
    # 
    url = response['upload_url']
    files = {'file': (name,doc_data,'multipart/form-data')}
    r = requests.post(url, files=files)
    log.debug("requests.post return: %s"%r.text)
    ret=json.loads(r.text)
    response=api.docs.save(file=ret['file'],title=name)
    log.debug("api.docs.save return:")
    log.debug(response)
    attachment_str="doc%d_%d"%(response['doc']['owner_id'],response['doc']['id'])
    if chat_type!="user":
      ret=api.messages.send(chat_id=chat_id,random_id=random_id, message=name,attachment=(attachment_str))
      log.debug("api..messages.send return:")
      log.debug(ret)
    else:
      ret=api.messages.send(user_id=chat_id,random_id=random_id, message=name,attachment=(attachment_str))
      log.debug("api..messages.send return:")
      log.debug(ret)
  except:
    log.error("vk_send_doc API or network error")
    return False
  return True

def vk_send_photo(vk_id, chat_id, name, photo_data, chat_type="user"):
  global log
  log.debug("=start function=")
  random_id=random.randint(0,4294967296)

  try:
    session = get_session(vk_id)
    api = vk.API(session, v=VK_API_VERSION)
    # получаем адрес загрузки:
    response=api.photos.getMessagesUploadServer()
    log.debug("api.photos.getMessagesUploadServer return:")
    log.debug(response)
    # 
    url = response['upload_url']
    files = {'photo': ('photo.png',photo_data,'multipart/form-data')}
    r = requests.post(url, files=files)
    log.debug("requests.post return: %s"%r.text)
    ret=json.loads(r.text)
    response=api.photos.saveMessagesPhoto(photo=ret['photo'],server=ret['server'],hash=ret['hash'])
    log.debug("api.photos.saveMessagesPhoto return:")
    log.debug(response)
    attachment="photo%(owner_id)d_%(media_id)d"%{"owner_id":response[0]["owner_id"],"media_id":response[0]["id"]}
    log.debug("attachment=%s"%attachment)
    if chat_type!="user":
      ret=api.messages.send(chat_id=chat_id, random_id=random_id, message=name,attachment=attachment)
      log.debug("api..messages.send return:")
      log.debug(ret)
    else:
      ret=api.messages.send(user_id=chat_id, random_id=random_id, message=name,attachment=attachment)
      log.debug("api..messages.send return:")
      log.debug(ret)
  except:
    log.error("vk_send_photo API or network error")
    return False
  return True

def delete_room_association(user,room,cmd):
  global log
  log.debug("=start function=")
  global lock
  global data
  global client

  room_id=cmd.replace("!delete ","").strip()
  if room_id in data["users"][user]["rooms"]:
    # удаляем запись об этой комнате из данных:
    log.info("Remove room: '%s' from data of user '%s'"%(room_id,user))
    vk_dialog_title=""
    if "cur_dialog" in data["users"][user]["rooms"][room_id] and \
      "title" in data["users"][user]["rooms"][room_id]["cur_dialog"]:
      vk_dialog_title=data["users"][user]["rooms"][room_id]["cur_dialog"]["title"]
    del data["users"][user]["rooms"][room_id]
    log.info("save state data on disk")
    save_data(data)
    bot_system_message(user,"Успешно удалил соответствие: %s - %s"%(vk_dialog_title,room_id))
    try:
      # удаляем всех остальных из комнаты:
      log.info("kick all from room: '%s'"%(room_id))
      #print("members: ",client.rooms[room_id]._members)
      my_full_id=client.user_id
      for item in client.rooms[room_id]._members:
        if item.user_id!=my_full_id:
          log.debug("kick user_id: %s"%item.user_id)
          client.rooms[room_id].kick_user(item.user_id,"пользователь удалил эту ассоциацию диалога ВК и комнаты MATRIX")
    except:
      log.error("error kick users from room: '%s'"%(room_id))
      bot_system_message(user,"Ошибка при попытке выгнать пользователей из комнаты: %s"%room_id)
    bot_system_message(user,"Успешно выгнал всех из комнаты: %s"%room_id)
    try:
      log.info("try leave from room: '%s'"%(room_id))
      response = client.api.leave_room(room_id)
    except:
      log.error("error leave room: '%s'"%(room_id))
      bot_system_message(user,"Ошибка выхода из комнаты: %s"%room_id)
      return False
    bot_system_message(user,"Успешно вышел из комнаты: %s"%room_id)
    try:
      # Нужно выйти из комнаты:
      log.info("Leave from room: '%s'"%(room_id))
      response = client.api.leave_room(room_id)
    except:
      log.error("error leave room: '%s'"%(room_id))
      bot_system_message(user,"Ошибка выхода из комнаты: %s"%room_id)
      return False
    bot_system_message(user,"Успешно вышел из комнаты: %s"%room_id)
    try:
      # И забыть её:
      log.info("Forgot room: '%s'"%(room_id))
      response = client.api.forget_room(room_id)
    except:
      log.error("error forgot room: '%s'"%(room_id))
      bot_system_message(user,"Не смог 'забыть' (удалить из архива) комнату: %s"%room_id)
      return False
    bot_system_message(user,"Успешно забыл комнату: %s"%room_id)
    log.info("success delete room association for room_id: %s"%room_id)
  else:
    bot_system_message(user,"Не нашёл соответствия с идентификатором комнаты: %s"%room_id)
    bot_cancel_command(room,user)
    return False
  return True

def rooms_command(user,room,cmd):
  global log
  log.debug("=start function=")
  global lock
  global data
  message="=== Список текущих соответствий диалогов ВК и комнат MATRIX: ===\n\n"
  index=1
  try:
    for room_id in data["users"][user]["rooms"]:
      item=data["users"][user]["rooms"][room_id]
      if "cur_dialog" in item:
        message+="%d"%index
        message+=". " + item["cur_dialog"]["title"] + " - " + room_id + "\n"
        index+=1
      else:
        log.debug("no cur_dialog for room: %s"%room_id)
    bot_system_message(user,message)
  except:
    log.error("create and send list of current rooms")
    bot_system_message(user,"Ошибка формирования списка комнат")
    return False
  return True

def dialogs_command(user,room,cmd):
  global log
  log.debug("=start function=")
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
    bot_system_message(user,'Не смог получить спиоок бесед из ВК - попробуйте позже :-(')
    log.error("get_dialogs() for user=%s"%user)
    bot_cancel_command(room,user)
    return False

  # Формируем список диалогов:
  send_message(room,"Выберите диалог:")
  try:
    message=""
    index=1
    dialogs_list={}
    for item_id in dialogs["chats"]:
      item=dialogs["chats"][item_id]
      dialogs_list[index]=item
      message+="%d. "%index
      log.debug(item)
      message+=item["title_ext"]
      message+="\n"
      index+=1
    send_message(room,message)
    data["users"][user]["rooms"][room]["state"]="wait_dialog_index"
    data["users"][user]["rooms"][room]["dialogs_list"]=dialogs_list
  except:
    log.error("create message with dialogs")
    bot_system_message(user,'Не смог сформировать список спиоок бесед из ВК - попробуйте позже  или обратитесь к разработчику :-(')
    bot_cancel_command(room,user)
    return False
  return True

def bot_system_message(user,message):
  global log
  global lock
  global data
  log.debug("=start function=")
  log.info(message)
  bot_control_room=data["users"][user]["matrix_bot_data"]["control_room"]
  return send_message(bot_control_room,message)

def bot_cancel_command(room,user):
  global log
  global lock
  global data
  log.debug("=start function=")
  log.warning("=====  call bot fault  =====")
  bot_control_room=data["users"][user]["matrix_bot_data"]["control_room"]
  session_data_room=data["users"][user]["rooms"][room]
  data["users"][user]["rooms"][room]["state"]="listen_command"
  send_message(bot_control_room,'Отменил текущий режим (%s) и перешёл в начальный режим ожидания команд. Жду команд.'%session_data_room["state"])
  return True

def debug_dump_json_to_file(filename, data):
  global log
  log.debug("=start function=")
  json_text=json.dumps(data, indent=4, sort_keys=True,ensure_ascii=False)
  f=open(filename,"w+")
  f.write(json_text)
  f.close()
  return True

def get_dialogs(vk_id):
  global log
  log.debug("=start function=")
  out={}
  # Формируем структуры:
  try:
    api = vk.API(get_session(vk_id), v=VK_API_VERSION)
    #dialogs = api.messages.getDialogs(count=200)
    dialogs = api.messages.getConversations(count=200,extended=1,fields="id,first_name,last_name,name,type")
    #debug_dump_json_to_file("dialogs_from_api.json",dialogs)
    out["groups"]={}
    for item in dialogs["groups"]:
      out["groups"][item["id"]]=item
    out["users"]={}
    for item in dialogs["profiles"]:
      out["users"][item["id"]]=item
    out["chats"]={}

    # Чаты ( это не то же самое, что группы O_o):
    for item in dialogs["items"]:
      # приводим к единообразию:
      if item["conversation"]["peer"]["type"]=="chat":
        elem={}
        elem["type"]="chat"
        elem["id"]=item["conversation"]["peer"]["id"]
        elem["users_count"]=item["conversation"]["chat_settings"]["members_count"]
        elem["title"]=item["conversation"]["chat_settings"]["title"]
        elem["state"]=item["conversation"]["chat_settings"]["state"]
        elem["title_ext"]=elem["title"]+" (групповой чат)"
        out["chats"][elem["id"]]=elem

      if item["conversation"]["peer"]["type"]=="group":
        elem={}
        elem["type"]="group"
        elem["id"]=item["conversation"]["peer"]["id"]
        elem["group_id"]=item["conversation"]["peer"]["local_id"]
        elem["title"]=out["groups"][elem["group_id"]]["name"]
        elem["title_ext"]=elem["title"]+" (сообщество)"
        out["chats"][elem["id"]]=elem

      if item["conversation"]["peer"]["type"]=="user":
        elem={}
        elem["type"]="user"
        elem["id"]=item["conversation"]["peer"]["id"]
        elem["user_id"]=item["conversation"]["peer"]["local_id"]
        elem["title"]=out["users"][elem["user_id"]]["first_name"]
        if out["users"][elem["user_id"]]["last_name"]!="":
          elem["title"]+=" "+out["users"][elem["user_id"]]["last_name"]
        elem["title_ext"]=elem["title"]
        out["chats"][elem["id"]]=elem

  except:
    log.error("get dialogs from VK API")
    return None
  #log.debug(json.dumps(out, indent=4, sort_keys=True,ensure_ascii=False))
  return out
  
def close_dialog(user,room_id):
  global log
  global client
  global lock
  global data
  log.debug("=start function=")
  log.debug("close_dialog()")
  log.debug("Try remove room: '%s' from data of user '%s'"%(room_id,user))
  if user in data["users"]:
    if "rooms" in data["users"][user]:
      if room_id in data["users"][user]["rooms"]:
        # удаляем запись об этой комнате из данных:
        log.info("Remove room: '%s' from data of user '%s'"%(room_id,user))
        del data["users"][user]["rooms"][room_id]
        log.info("save state data on disk")
        save_data(data)
        try:
          # Нужно выйти из комнаты:
          log.info("Leave from room: '%s'"%(room_id))
          response = client.api.leave_room(room_id)
        except:
          log.error("error leave room: '%s'"%(room_id))
          return None
        try:
          # И забыть её:
          log.info("Forgot room: '%s'"%(room_id))
          response = client.api.forget_room(room_id)
        except:
          log.error("error forgot room: '%s'"%(room_id))
          return None
        return None
        log.info("success close dialog for user invite user '%s' and room '%s'"%(user,room_id))
        return True
      else:
        log.warning("unknown room '%s' for user '%s'"%(room_id,user))
  else:
    log.warning("unknown user: '%s'"%(user))

  log.info("do not close dialog for user user '%s' and room '%s'"%(user,room_id))
  return False
              

def login_command(user,room,cmd):
  global log
  global lock
  global data
  log.debug("=start function=")
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
  global log
  log.debug("=start function=")
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
  log.debug("=start function=")
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
  log.debug("=start function=")
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
  #print(json.dumps(data, indent=4, sort_keys=True,ensure_ascii=False))
  #sys.exit()
  return data

def create_room(matrix_uid, room_name):
  global log
  global louser_id
  global client
  log.debug("=start function=")

  log.debug("create_room()")

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
  log.debug("=start function=")

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
    bot_system_message(user,'Не смог отправить сообщение в комнату: %s'%room_id)
    return False
  return True

def get_file(mxurl):
  global client
  global log
  log.debug("=start function=")
  log.debug("get_file 1")
  ret=None
  # получаем глобальную ссылку на файл:
  try:
    log.debug("get_file file 2")
    full_url=client.api.get_download_url(mxurl)
    log.debug("get_file file 3")
  except MatrixRequestError as e:
    log.error(e)
    if e.code == 400:
      log.error("ERROR download file")
      return None
    else:
      log.error("Couldn't download file (unknown error)")
      return None
  # скачиваем файл по ссылке:
  try:
    response = requests.get(full_url, stream=True)
    data = response.content      # a `bytes` object
  except:
    log.error("fetch file data from url: %s"%full_url)
    return None
  return data

def send_message(room_id,message):
  global client
  global log
  log.debug("=start function=")

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

def send_notice(room_id,message):
  global client
  global log
  log.debug("=start function=")
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
    room.send_notice(message)
  except:
    log.error("Unknown error at send notice message '%s' to room '%s'"%(message,room_id))
    return False
  return True


# Called when a message is recieved.
def on_message(event):
  global client
  global log
  global lock
  log.debug("=start function=")
  formatted_body=None
  format_type=None
  reply_to_id=None
  file_url=None
  file_type=None

  print("new MATRIX message:")
  print(json.dumps(event, indent=4, sort_keys=True,ensure_ascii=False))
  if event['type'] == "m.room.member":
      # join:
      if event['content']['membership'] == "join":
          log.info("{0} joined".format(event['content']['displayname']))
      # leave:
      elif event['content']['membership'] == "leave":
          log.info("{0} leave".format(event['sender']))
          # close room:
          with lock:
            log.debug("success lock before process_command()")
            if close_dialog(event['sender'],event['room_id']) == False:
              log.warning("close_dialog()==False")
      return True
  elif event['type'] == "m.room.message":
      if event['content']['msgtype'] == "m.text":
          reply_to_id=None
          if "m.relates_to" in  event['content']:
            # это ответ на сообщение:
            try:
              reply_to_id=event['content']['m.relates_to']['m.in_reply_to']['event_id']
            except:
              log.error("bad formated event reply - skip")
              log.error(event)
              return False
          formatted_body=None
          format_type=None
          if "formatted_body" in event['content'] and "format" in event['content']:
            formatted_body=event['content']['formatted_body']
            format_type=event['content']['format']

      elif event['content']['msgtype'] == "m.video":
        try:
          file_type=event['content']['info']['mimetype']
          file_url=event['content']['url']
        except:
          log.error("bad formated event with file data - skip")
          log.error(event)
          return False
      elif event['content']['msgtype'] == "m.image":
        try:
          file_url=event['content']['url']
          if "imageinfo" in event['content']['info']:
            file_type=event['content']['info']['imageinfo']['mimetype']
          else:
            file_type=event['content']['info']['mimetype']
        except:
          log.error("bad formated event with file data - skip")
          log.error(event)
          return False
      elif event['content']['msgtype'] == "m.file":
        try:
          file_url=event['content']['url']
          if "fileinfo" in event['content']['info']:
            file_type=event['content']['info']['fileinfo']['mimetype']
          else:
            file_type=event['content']['info']['mimetype']
        except:
          log.error("bad formated event with file data - skip")
          log.error(event)
          return False

      log.debug("{0}: {1}".format(event['sender'], event['content']["body"].encode('utf8')))
      log.debug("try lock before process_command()")
      with lock:
        log.debug("success lock before process_command()")
        if process_command(\
            event['sender'],\
            event['room_id'],\
            event['content']["body"],\
            formated_message=formatted_body,\
            format_type=format_type,\
            reply_to_id=reply_to_id,\
            file_url=file_url,\
            file_type=file_type\
          ) == False:
          log.error("error process command: '%s'"%event['content']["body"])
          return False

  else:
    print(event['type'])
  return True

def on_event(event):
  global log
  log.debug("=start function=")
  print("event:")
  print(event)
  print(json.dumps(event, indent=4, sort_keys=True,ensure_ascii=False))

def on_invite(room, event):
  global client
  global log
  global lock
  global data
  log.debug("=start function=")

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
  log.debug("=start function=")
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
    time.sleep(1)
  log.info("exit main loop")

def check_thread_exist(vk_id):
  global log
  log.debug("=start function=")
  for th in threading.enumerate():
      if th.getName() == 'vk' + str(vk_id):
          return True
  return False

# запуск потоков получения сообщений:
def start_vk_polls():
  global data
  global lock
  global log
  log.debug("=start function=")

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
  global log
  log.debug("=start function=")
  return re.sub('.*/', '', url)

def send_file_to_matrix(room,sender_name,attachment):
  global log
  log.debug("=start function=")
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
  global log
  log.debug("=start function=")
  if geo["type"]=='point':
    coordinates=geo["coordinates"]
    lat=coordinates["latitude"]
    lon=coordinates["longitude"]
    place_name=geo["place"]["title"]

    
    geo_url="https://opentopomap.org/#marker=13/%(lat)s/%(lon)s"%{"lat":lat,"lon":lon}

    if sender_name!=None:
      text = sender_name + ' прислал местоположение (%s, %s):\n'%(lat,lon) + geo_url
    else:
      text = 'местоположение (%s, %s):\n'%(lat,lon) + geo_url
    send_message(room,text)
    return True
    
    # TODO добавить превью карты (на данный момент этот сайт не работает):
    preview_src="https://staticmap.openstreetmap.de/staticmap.php?center=%(lat)f,%(lon)f&zoom=14&size=400x300&maptype=mapnik"%{"lat":lat,"lon":lon}
    log.debug("try get static map preview from url: %s"%preview_src)
    image_data=get_data_from_url(preview_src,referer="osm.org")
    if image_data==None:
      log.error("get image from url: %s"%src)
      return False

    # TODO добавить определение типа:
    mimetype="image/png"
    size=len(image_data)
      
    mxc_url=upload_file(image_data,mimetype)
    if mxc_url == None:
      log.error("uload file to matrix server")
      return False
    log.debug("send file 1")
    file_name=place_name+".png"

    size=len(image_data)
    if matrix_send_image(room,mxc_url,file_name,mimetype,size=size) == False:
      log.error("send file to room")
      return False
  else:
    bot_system_message(user,"получен неизвестный тип гео-данных - прпоускаю")
    send_message(room,"получен неизвестный тип гео-данных - прпоускаю")
    return False

def send_photo_to_matrix(room,sender_name,attachment):
  global log
  log.debug("=start function=")
  if "sizes" not in attachment["photo"]:
    log.error("parse photo attachment - not found tag 'sizes'")
    log.error(attachment["photo"])
    return False
  # находим самый большой размер фото:
  width=0
  height=0
  src=None
  for item in attachment["photo"]["sizes"]:
    if item["width"] > width:
      width=item["width"]
      height=item["height"]
      src=item["url"]
      continue
    if item["height"] > height:
      width=item["width"]
      height=item["height"]
      src=item["url"]

  if src == None:
    log.error("get src for photo")
    log.error(attachment["photo"])
    return False
  
  image_data=get_data_from_url(src)
  if image_data==None:
    log.error("get image from url: %s"%src)
    return False

  # TODO добавить определение типа:
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

  if matrix_send_image(room,mxc_url,file_name,mimetype,height,width,size) == False:
    log.error("send file to room")
    return False

def send_video_to_matrix(room,sender_name,attachment):
  global log
  log.debug("=start function=")
  src=attachment["video"]['first_frame_320']
  
  image_data=get_data_from_url(src)
  if image_data==None:
    log.error("get image from url: %s"%src)
    return False

  # TODO добавить определение типа:
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

  if matrix_send_image(room,mxc_url,file_name,mimetype=mimetype,height=0,width=0,size=size) == False:
    log.error("send file to room")
    return False
  video_url="https://vk.com/video%(owner_id)s_%(vid)s"%{"owner_id":attachment["video"]["owner_id"],"vid":attachment["video"]["id"]}
  return send_message(room,"Ссылка на просмотр потокового видео: %s"%video_url)

def send_audio_to_matrix(room,sender_name,attachment):
  global log
  log.debug("=start function=")
  src=attachment["audio"]['url']
  size=0
  duration=attachment["audio"]["duration"]
  file_name=attachment["audio"]["title"]+" ("+attachment["audio"]["title"]+").mp3"
  # TODO добавить определение типа:
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
  global log
  log.debug("=start function=")
  src=attachment["doc"]['url']
  size=attachment["doc"]["size"]
  file_name=attachment["doc"]["title"]
  # TODO добавить определение типа:
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
  global log
  log.debug("=start function=")
  success_status=True
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
    # сообщение со стены:
    elif attachment["type"]=="wall":
      text=""
      if sender_name!=None:
        text+="<p><strong>%(sender_name)s</strong>:</p>\n"%{"sender_name":sender_name}
      text+="<blockquote>\n<p>Запись на стене:</p>\n<p>%(wall_text)s</p>\n" % {"wall_text":attachment["wall"]["text"]}
      # если на стене были вложения, то добавляем их как ссылки:
      if "attachments" in attachment["wall"]:
        for attachment in attachment["wall"]["attachments"]:
          url=None
          if attachment['type']=="photo":
            url=attachment["photo"]["src"]
          elif attachment['type']=="video":
            url="https://vk.com/video%(owner_id)s_%(vid)s"%{"owner_id":attachment["video"]["owner_id"],"vid":attachment["video"]["vid"]}
          elif attachment['type']=="audio":
            url=attachment["audio"]['url']
          elif attachment['type']=="doc":
            url=attachment["doc"]['url']
          if url!=None:
            text+="<p>вложение: %(url)s</p>\n" % {"url":url}
      text+="</blockquote>\n"
      if send_html(room,text)==False:
        log.error("send_html()")
        bot_system_message(user,"Не смог отправить сообщение в комнату: '%s', сообщение было: %s"%(room,text))
        success_status=False
    else:
      log.error("unknown attachment type - skip. attachment type=%s"%attachment["type"])
  return success_status

def get_data_from_url(url,referer=None):
  global log
  log.debug("=start function=")
  try:
    if referer!=None:
      response = requests.get(url, stream=True,headers=dict(referer = referer))
    else:
      response = requests.get(url, stream=True)
    data = response.content      # a `bytes` object
  except:
    log.error("fetch data from url: %s"%url)
    return None
  return data


def matrix_send_audio(room_id,url,name,mimetype="audio/mpeg",size=0,duration=0):
  global log
  global client
  log.debug("=start function=")
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

def matrix_send_image(room_id,url,name,mimetype,height=None,width=None,size=None):
  global log
  global client
  log.debug("=start function=")
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
  if size!=None:
    imageinfo["size"]=size
  if height!=None:
    imageinfo["h"]=height
  if width!=None:
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
  log.debug("=start function=")
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
  log.debug("=start function=")
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

def check_equal_messages(vk_body,matrix_body):
  global log
  log.debug("=start function=")
  log.debug("check_equal_messages(%s,%s)"%(vk_body,matrix_body))
  if vk_body==matrix_body:
    return True
  # заменяем разные символы:
  src=vk_body.replace('&gt;','>')\
        .replace('&lt;','<')\
        .replace('<br>','\n')
  log.debug("check_equal_messages() after replace: vk_body: %s"%src)
  log.debug("check_equal_messages() after replace: matrix_body: %s"%matrix_body)
  if src==matrix_body:
    log.debug("check_equal_messages() equal!")
    return True
  else:
    log.debug("check_equal_messages() NOT equal!")
  return False

def proccess_vk_message(bot_control_room,room,user,sender_name,m):
  global data
  global lock
  global log
  log.debug("=start function=")
  send_status=False
  owner_message=False
  with lock:
    last_matrix_owner_message=data["users"][user]["rooms"][room]["last_matrix_owner_message"]
  text=""
  # Сообщение от нашей учётки в ВК:
  if m["out"]==1:
    log.debug("receive our message")
    owner_message=True
    if check_equal_messages(m["text"],last_matrix_owner_message):
      # текст такой же, какой мы отправляли последний раз в эту комнату из матрицы - не отображаем его:
      log.debug("receive from vk our text, sended from matrix - skip it")
      return True
    else:
      # Это наше сообщение, но отправлено из другого клиента. Шлём просто текст, но через m.notice, чтобы не дилинькал клиент:
      text="Ваша реплика:\n" + m["text"]
      return send_notice(room,text)

  if 'fwd_messages' in m:
    if sender_name!=None:
      text+="<p><strong>%(sender_name)s</strong>:</p>\n"%{"sender_name":sender_name}
    # это ответ на сообщение - добавляем текст сообщения, на который дан ответ:
    for fwd in m['fwd_messages']:
      fwd_uid=fwd["peer_id"]
      fwd_text=fwd["text"]
      # TODO получить ФИО авторов перенаправляемых сообщений
      text+="<blockquote>\n<p>В ответ на реплику от <strong>%(fwd_user)s</strong>:</p><p>%(fwd_text)s</p>\n" % {"fwd_user":fwd_uid, "fwd_text":fwd_text}
      # если это ответ на вложения, то добавляем их как ссылки:
      if "attachments" in fwd:
        for attachment in fwd["attachments"]:
          url=None
          if attachment['type']=="photo":
            url=attachment["photo"]["src"]
          elif attachment['type']=="video":
            url="https://vk.com/video%(owner_id)s_%(vid)s"%{"owner_id":attachment["video"]["owner_id"],"vid":attachment["video"]["vid"]}
          elif attachment['type']=="audio":
            url=attachment["audio"]['url']
          elif attachment['type']=="doc":
            url=attachment["doc"]['url']
          if url!=None:
            text+="<p>вложение: %(url)s</p>\n" % {"url":url}
      text+="</blockquote>\n"
    text+="<p>%s</p>\n" % m["text"]
  else:
    if sender_name!=None:
      text="<strong>%s</strong>: %s"%(sender_name,m["text"])
    else:
      text=m["text"]

  if len(text)>0:
    if send_html(room,text) == True:
      send_status=True
    else:
      bot_system_message(user,"Ошибка: не смог отправить сообщение из ВК в комнату: '%s' сообщение были от: %s"%(room,sender_name))
      bot_system_message(user,"Содержимое сообщения: '%s'"%text)
      send_status=False
  # отправка вложений:
  if "attachments" in m:
    if send_attachments(room,sender_name,m["attachments"])==False:
      send_message(room,'Ошибка: не смог отправить вложения из исходного сообщения ВК - см. логи')
      bot_system_message(user,'Ошибка: не смог отправить вложения из исходного сообщения ВК - вложения были от: %s'%sender_name)
    else:
      send_status=True
  # отправка местоположения:
  if "geo" in m:
    if send_geo_to_matrix(room,sender_name,m["geo"])==False:
      send_message(room,'Ошибка: не смог отправить местоположение из исходного сообщения ВК - см. логи')
      bot_system_message(user,'Ошибка: не смог отправить местоположение из исходного сообщения ВК - вложения были от: %s'%sender_name)
    else:
      send_status=True
  if send_status==False:
    bot_system_message(user,'Ошибка: не смог отправить сообщение в матрицу из ВК в комнату %s'%room)
    log.warning("сообщение было:")
    log.warning(m)

  return send_status


def vk_receiver_thread(user):
  global data
  global lock
  global log
  log.debug("=start function=")
  log.info("start new vk_receiver_thread() for user='%s'"%user)
  # Обновляем временные метки:
  session = get_session(data["users"][user]["vk"]["vk_id"])
  last_matrix_owner_message=None
  with lock:
    data["users"][user]["vk"]["ts"], data["users"][user]["vk"]["pts"] = get_tses(session)
    bot_control_room=data["users"][user]["matrix_bot_data"]["control_room"]

  while True:
    res=get_new_vk_messages(user)
    if res != None:
      for m in res["messages"]:
        log.debug("Receive message from VK:")
        log.debug(json.dumps(m, indent=4, sort_keys=True,ensure_ascii=False))
        found_room=False
        for room in data["users"][user]["rooms"]:
          if "cur_dialog" in data["users"][user]["rooms"][room]:
            sender_name=None
            vk_room_id=m["peer_id"]
            # проверяем, групповой ли это чат:
            if "chat_id" in m:
              # групповой чат:
              vk_room_id = m["chat_id"]

            if data["users"][user]["rooms"][room]["cur_dialog"]["id"] == vk_room_id:
              # нашли комнату:
              found_room=True
              # проверяем, групповой ли это чат:
              if "chat_id" in m:
                # Если это групповой чат - нужно добавить имя отправителя, т.к. их там может быть много:
                # Ищем отправителя в профилях полученного сообщения:
                for profile in res["profiles"]:
                  if profile["peer_id"]==m["peer_id"]:
                    sender_name="%s %s"%(profile["first_name"],profile["last_name"])
              if proccess_vk_message(bot_control_room,room,user,sender_name,m) == False:
                log.warning("proccess_vk_message(room=%s) return false"%(room))

        if found_room==False:
          # Не нашли созданной комнаты, чтобы отправить туда сообщение.
          # Нужно самим создать комнату и отправить туда сообщение.

          # Нужно найти имя диалога:
          dialogs=get_dialogs(data["users"][user]["vk"]["vk_id"])
          if dialogs == None:
            log.error("get_dialogs for user '%s'"%user)
            bot_system_message(user,'Не смог получить спиоок бесед из ВК - поэтому не смог создать новую комнату в связи с пришедшикомнату попробуйте позже :-(')

          cur_dialog=None
          if "chat_id" in m:
            # значит ищем среди групп:
            if m["chat_id"] in dialogs["chats"]:
              cur_dialog=dialogs["chats"][m["chat_id"]]
          else:
            # значит ищем среди чатов:
            if m["peer_id"] in dialogs["chats"]:
              cur_dialog=dialogs["chats"][m["peer_id"]]

          if cur_dialog == None:
            log.error("can not found VK sender in dialogs - logic error - skip")
            bot_system_message(user,"Не смог найти диалог для вновь-поступившего сообщения. vk_uid отправителя='%d'"%m["peer_id"])
            bot_system_message(user,"Сообщение было: '%s'"%m["text"])
            continue
          
          room_id=create_room(user,cur_dialog["title_ext"] + " (VK)")
          if room_id==None:
            log.error("error create_room() for user '%s' for vk-dialog with vk-id '%d' ('%s')"%(user,cur_dialog["id"],cur_dialog["title"]))
            bot_system_message(user,"Не смог создать дополнительную комнату в матрице: '%s' связанную с одноимённым диалогом в ВК"%cur_dialog["title"])
            continue
          bot_system_message(user,"Создал новую комнату матрицы с именем: '%s (VK)' связанную с одноимённым диалогом в ВК"%cur_dialog["title"])
          with lock:
            data["users"][user]["rooms"][room_id]={}
            data["users"][user]["rooms"][room_id]["cur_dialog"]=cur_dialog
            data["users"][user]["rooms"][room_id]["state"]="dialog"
            data["users"][user]["rooms"][room_id]["last_matrix_owner_message"]=""
            # сохраняем на диск:
            save_data(data)
          # отправляем текст во вновь созданную комнату:
          sender_name=None
          if "chat_id" in m:
            # Групповой чат - добавляем имя отправителя:
            # Ищем отправителя в профилях полученного сообщения:
            for profile in res["profiles"]:
              if profile["peer_id"]==m["peer_id"]:
                sender_name="<strong>%s %s:</strong> "%(profile["first_name"],profile["last_name"])

          if proccess_vk_message(bot_control_room,room_id,user,sender_name,m) == False:
            log.warning("proccess_vk_message(room=%s) return false"%room_id)

    # FIXME 
    log.info("sleep main loop 1")
    time.sleep(5)

  return True

if __name__ == '__main__':
  log= logging.getLogger("MatrixVkBot")
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
