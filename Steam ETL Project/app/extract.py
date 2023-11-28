import pandas as pd
import requests
from services.mysql_conn import mysql_upload

steam_request = requests.get("http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/?key=80385AB39B7C5B72CB7F7E24CDF6CF2E&steamid=76561198365092040&format=json")
print(steam_request)
game_list = steam_request.json()['response']['games']

df_game = pd.DataFrame(game_list)
df_game['rtime_last_played'] = pd.to_datetime(df_game['rtime_last_played'], unit = 's')
df_game['never_played'] = df_game['playtime_forever'] == 0

print(df_game)

mysql_upload(df_game, "steam_player_stats")