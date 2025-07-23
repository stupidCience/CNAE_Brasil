# Configuração do banco de dados
# Para usar: copie config_db_local.py.example para config_db_local.py e configure suas credenciais

DB_CONFIG = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': 'SUA_SENHA_AQUI',
    'database': 'cnae',
    'autocommit': False,
    'connection_timeout': 300,
    'auth_plugin': 'mysql_native_password',
    'sql_mode': '',
    'charset': 'utf8mb4',
    'use_unicode': True,
    'buffered': True,
    'raise_on_warnings': False
}

try:
    from config.config_db_local import DB_CONFIG_LOCAL
    DB_CONFIG.update(DB_CONFIG_LOCAL)
except ImportError:
    pass
