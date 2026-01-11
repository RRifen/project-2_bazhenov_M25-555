ALLOWED_COLUMNS_TYPES = ("int", "str", "bool")
ACTION_SKIP_FLAG = 'ACTION_SKIP_FLAG'

INSERT_PATTERN = r'insert\s+into\s+(\w+)\s+values\s*\((.*)\)'
INSERT_VALUE_PATTERN = r"(?:'[^']*'|[^,]+)"
SELECT_PATTERN = r'select\s+from\s+(\w+)(?:\s+where\s+(.+))?'
UPDATE_PATTERN = r'update\s+(\w+)\s+set\s+(.+)\s+where\s+(.+)'
DELETE_PATTERN = r'delete\s+from\s+(\w+)\s+where\s+(.+)'

WHERE_CLAUSE_PATTERN = r'(\w+)\s*=\s*(.+)'
SET_CLAUSE_PATTERN = r'(\w+)\s*=\s*(.+)'

DATA_FOLDER_PATH = "./data"
DB_META_FILEPATH = "./db_meta.json"
