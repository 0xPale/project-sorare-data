import websocket
import json
import time
import asyncio
from datetime import datetime
from variables import outputFolder

w_socket = 'wss://ws.sorare.com/cable'
identifier = json.dumps({"channel": "GraphqlChannel"})
'''
subscription_query = {
  "query": "subscription marketUpdated { publicMarketWasUpdated { card_slug: slug, transfer: userOwnersWithRate { transfer_date: from, transfer_type: transferType, transfer_priceETH: price, transfer_priceFiat: priceInFiat { usd } } } }",
  "variables": {},
  "operationName": "marketUpdated",
  "action": "execute"
}
'''

subscription_query = {
  "query": "subscription marketUpdated { publicMarketWasUpdated { card_slug: slug, transfer: ownerWithRates { transfer_date: from, transfer_type: transferType, transfer_priceETH: price, transfer_priceFiat: priceInFiat { usd } } } }",
  "variables": {},
  "operationName": "marketUpdated",
  "action": "execute"
}

def on_open(ws):
  subscribe_command = {"command": "subscribe", "identifier": identifier}
  ws.send(json.dumps(subscribe_command).encode())

  asyncio.sleep(1)

  message_command = {
    "command": "message",
    "identifier": identifier,
    "data": json.dumps(subscription_query)
  }
  ws.send(json.dumps(message_command).encode())
  print('WebSocket Opened')

def on_message(ws, data):
  message = json.loads(data)
  message_raw = data
  type = message.get('type')
  if type == 'welcome':
    print(type)
    #pass
  elif type == 'ping':
    print(type)
    #pass
  elif message.get('message') is not None:
    #print(message['message']['result']['data']['publicMarketWasUpdated'])
    #print(message_raw)
    with open(outputFolder + 'subscription/marketUpdate_dump_' + datetime.now().strftime("%Y%m%d_%H") + '.json', "a") as output_file:
      output_file.write(str(message_raw) + "\n")

def on_error(ws, error):
  print('Error:', error)

def on_close(ws, close_status_code, close_message):
  print('WebSocket Closed:', close_message, close_status_code)
  asyncio.sleep(1)
  long_connection()

def long_connection():
  ws = websocket.WebSocketApp(
    w_socket,
    on_message=on_message,
    on_close=on_close,
    on_error=on_error,
    on_open=on_open
  )
  ws.run_forever()

if __name__ == '__main__':
  long_connection()