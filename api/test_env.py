from dotenv import load_dotenv
import os, pathlib
print('cwd =', pathlib.Path().resolve())
print('exists .env =', pathlib.Path('.env').exists())
load_dotenv(dotenv_path='.env', override=True)
print('DB_URL =', os.getenv('DB_URL'))
