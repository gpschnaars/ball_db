
import time
import duckdb
from nba_api.stats.endpoints import PlayByPlayV2

from ..utils.db import REMOTE_DB


# _season = '2024-25'
# _league_id = '00'

_db = 'nba'
_schema = 'bronze'
_table = 'play_by_play'

def run():

    with duckdb.connect(REMOTE_DB) as conn:

        game_ids_played = set(item[0] for item in conn.sql(f'SELECT distinct game_id FROM {_db}.{_schema}.game_log').fetchall())

        game_ids_added = set(item[0] for item in conn.sql(f'SELECT distinct game_id FROM {_db}.{_schema}.{_table}').fetchall())

        # # probably dont need to do this if im already explicilty skipping game_ids already added
        # conn.execute(f"""
        #     ALTER TABLE {_db}.{_schema}.{_table}
        #     ADD CONSTRAINT pk_player_box_scores PRIMARY KEY (game_id, team_id, player_id)
        # """)


        game_ids_unfetched = game_ids_played - game_ids_added

        n_iterations = len(game_ids_unfetched)

        for i, _game_id in enumerate(game_ids_unfetched):

            print(f'play_by_play, game_id {_game_id} ... iteration {i+1}, {n_iterations - i + 1} remaining')

            endpoint = PlayByPlayV2(
                game_id = _game_id
            )

            df = endpoint.get_data_frames()[0]

            conn.execute(f"""
                INSERT INTO {_db}.{_schema}.{_table}
                SELECT
                    *
                FROM df
                -- ON CONFLICT (SEASON_ID, TEAM_ID, GAME_ID)
                -- DO NOTHING
            """)

            time.sleep(10)
