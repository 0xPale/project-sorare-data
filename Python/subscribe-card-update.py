import websocket
import json
import time
from datetime import datetime
import getpass
from variables import outputFolderLocal, outputFolderCloud

if getpass.getuser() == "benjamin":
    outputFolder = outputFolderLocal
else:
    outputFolder = outputFolderCloud

# This is the WebSocket URL for the Sorare API.
w_socket = 'wss://ws.sorare.com/cable'
# This is the identifier for the subscription. It is a JSON object that contains the channel name.
identifier = json.dumps({"channel": "GraphqlChannel"})

# This is the subscription query. It is a JSON object that contains the query and the variables.
subscription_query = {
  "query": "subscription marketUpdated {\
    publicMarketWasUpdated {\
      card_slug: slug\
      card_name: name\
      player {\
        slug\
      }\
      card_rarity: rarity\
      card_season: season {\
        startYear\
      }\
      transfer: ownerWithRates {\
        sorareAccount: account {\
          manager: owner {\
            ... on User {\
              nickname\
              slug\
            }\
          }\
        }\
        transfer_date: from\
        transfer_type: transferType\
        transfer_priceETH: price\
        transfer_priceFiat: priceInFiat {\
          usd\
        }\
      }\
    }\
   }",
  "variables": {},
  "operationName": "marketUpdated",
  "action": "execute"
}

def on_open(ws):
  """
  When the WebSocket is opened, it sends a subscribe command and a message command. The subscribe
  command is sent first, and it subscribes to the channel. The message command is sent after one
  second to get the data from the channel
  
  :param ws: The WebSocket object
  """
  subscribe_command = {"command": "subscribe", "identifier": identifier}
  ws.send(json.dumps(subscribe_command).encode())

  time.sleep(1)

  message_command = {
    "command": "message",
    "identifier": identifier,
    "data": json.dumps(subscription_query)
  }
  ws.send(json.dumps(message_command).encode())
  print('WebSocket Opened')

def on_message(ws, data):
  """
  The on_message function is called when the websocket receives a message.
  
  :param ws: websocket object
  :param data: The data that was sent by the server
  """
  message = json.loads(data)
  message_raw = data
  type = message.get('type')
  if type == 'welcome':
    print(type)
    #pass
  elif type == 'ping':
    pass
  elif message.get('message') is not None:
    with open(outputFolder + 'subscription/marketUpdate_dump_' + datetime.now().strftime("%Y%m%d_%H") + '.json', "a") as output_file:
      output_file.write(str(message_raw) + "\n")

def on_error(ws, error):
  """
  When an error occurs, print the error
  
  :param ws: websocket object
  :param error: The error that was raised
  """
  print('Error:', error)

def on_close(ws, close_status_code, close_message):
  """
  When the websocket is closed, it will call the long_connection function
  
  :param ws: The WebSocket object
  :param close_status_code: The status code of the close message
  :param close_message: The message sent when the WebSocket is closed
  """
  print('WebSocket Closed:', close_message, close_status_code)
  time.sleep(1)
  long_connection()

def long_connection():
  """
  It connects to the websocket, and then waits for data
  """
  ws = websocket.WebSocketApp(
    w_socket,
    on_message=on_message,
    on_close=on_close,
    on_error=on_error,
    on_open=on_open
  )
  ws.run_forever()

# This is a Python idiom that allows the code to be used both in scripts and as a
# module. If the module is imported, the `long_connection()` function will not be called.
if __name__ == '__main__':
  long_connection()