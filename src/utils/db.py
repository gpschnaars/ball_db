


def get_motherduck_token(
    fp: str = '.secrets/md_token'       
) -> str:
    with open(fp, 'r') as f:
        token = f.read()
    return token



LOCAL_DB = '.duckdb/local.db'
REMOTE_DB = f'md:my_db?motherduck_token={get_motherduck_token()}'