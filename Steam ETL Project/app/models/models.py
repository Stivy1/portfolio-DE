from pydantic import BaseModel
from typing import List
import stringcase

class SteamPlayerStats(BaseModel):
    appid: int
    playtime_forever: int
    playtime_windows_forever: str
    playtime_mac_forever: int
    playtime_linux_forever: int
    rtime_last_played: str
    playtime_disconnected: int
    never_played: str

class GameInfo(BaseModel):
    game_id: int
    game_name: str
    

def generate_create_table_query(model_class):
    table_name = stringcase.snakecase(model_class.__name__)
    print(table_name)
    columns = []

    for field_name, field_type in model_class.__annotations__.items():
        if field_name == "appid" and field_type == int:
            columns.append(f"{field_name} INT AUTO_INCREMENT PRIMARY KEY NOT NULL")
        if field_name == "game_id" and field_type == int:
            columns.append(f"{field_name} INT AUTO_INCREMENT PRIMARY KEY NOT NULL")
        else:
            sql_type = "INT" if field_type == int else "VARCHAR(255) NULL"
            columns.append(f"{field_name} {sql_type}")
    columns_str = ",\n".join(columns)
    return f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns_str}
        )
    """, table_name