import pandas as pd
import requests
from models.models import generate_create_table_query, SteamPlayerStats, GameInfo
import mysql.connector
import pandas as pd

#Buscar el modelo de la tabla
def get_model_class(table_name: str):
    table_to_class = {
        "SteamPlayerStats": SteamPlayerStats,
        "GameInfo": GameInfo
    }
    return table_to_class.get(table_name)

# Realizar la conexion de la DB
table_name = "steam_player_stats"
conn = mysql.connector.connect(host = 'localhost', user = 'Stivy', passwd = 'Stivy01*', db = 'steam', auth_plugin='mysql_native_password')
cursor = conn.cursor()

cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
exist_table = cursor.fetchone()
print(exist_table)

#Extraer la informacion
steam_request = requests.get("http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/?key=80385AB39B7C5B72CB7F7E24CDF6CF2E&steamid=76561198365092040&format=json")
print(steam_request)
game_list = steam_request.json()['response']['games']

df_game = pd.DataFrame(game_list)
df_game['rtime_last_played'] = pd.to_datetime(df_game['rtime_last_played'], unit = 's').astype(str)
df_game['never_played'] = df_game['playtime_forever'] == 0

print(df_game)

#Usar el modelo de la tabla para generar una query DDL que cree la tabla en la BD
model_class = get_model_class("SteamPlayerStats")

print(model_class)

create_table_query, new_table_name = generate_create_table_query(model_class)
#print(create_table_query)
cursor.execute(create_table_query)

#Llenar de informacion la tabla ya creada

columns = []
for field_name, field_type in model_class.__annotations__.items():
    columns.append(field_name)
try:
    for _, row in df_game.iterrows():
        data_dict = dict(zip(columns, row))
        
        query = f"INSERT INTO {new_table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
        
        values = tuple(data_dict[column] for column in columns)
        print(query, values)
        cursor.execute(query, values)
    conn.commit()
except Exception as e:
    print(f"Failed to upload the information {e}")

#Mismo procedimiento para extraer la informacion de los juegos
game_name = []
game_id = []
for i in range(len(df_game)):
    game_request = requests.get(f"http://api.steampowered.com/ISteamUserStats/GetUserStatsForGame/v0002/?appid={df_game['appid'][i]}&key=80385AB39B7C5B72CB7F7E24CDF6CF2E&steamid=76561198365092040")
    try:
        game_name.append(game_request.json()['playerstats']['gameName'])
        game_id.append(df_game['appid'][i])
    except:
        pass

df_game_info = pd.DataFrame ({
            'game_id': game_id,
            'game_name': game_name
        }
)

df_game_info =  df_game_info[df_game_info['game_name'] != '']
print(df_game_info)

model_class = get_model_class("GameInfo")

print(model_class)

create_table_query, new_table_name = generate_create_table_query(model_class)
#print(create_table_query)
cursor.execute(create_table_query)

# #Llenar de informacion la tabla ya creada

columns = []
for field_name, field_type in model_class.__annotations__.items():
    columns.append(field_name)
try:
    for _, row in df_game_info.iterrows():
        data_dict = dict(zip(columns, row))
        
        query = f"INSERT INTO {new_table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
        
        values = tuple(data_dict[column] for column in columns)
        print(query, values)
        cursor.execute(query, values)
    conn.commit()
except Exception as e:
    print(f"Failed to upload the information {e}")