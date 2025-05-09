

import duckdb
from nba_api.stats.endpoints import LeagueGameLog

from ..utils.db import REMOTE_DB

_season_type_all_star = 'Playoffs'
_season = '2024-25'
_league_id = '00'


_db = 'nba'
_schema = 'bronze'
_table = 'game_log'



def run():

    # get data
    endpoint = LeagueGameLog(
        season = _season,
        season_type_all_star = _season_type_all_star,
        league_id = _league_id,
    )

    df = endpoint.get_data_frames()[0]

    df = df.loc[df['VIDEO_AVAILABLE'] == 1]

    with duckdb.connect(REMOTE_DB) as conn:

        conn.execute(f"""
            -- ALTER TABLE {_db}.{_schema}.{_table}
            -- ADD CONSTRAINT pk_game_log PRIMARY KEY (SEASON_ID, TEAM_ID, GAME_ID)
            CREATE UNIQUE INDEX IF NOT EXISTS pk_game_log 
            ON {_db}.{_schema}.{_table} (SEASON_ID, TEAM_ID, GAME_ID)
        """)

        conn.execute(f"""
            INSERT INTO {_db}.{_schema}.{_table}
            SELECT
                *
            FROM df
            ON CONFLICT (SEASON_ID, TEAM_ID, GAME_ID)
            DO NOTHING
        """)





