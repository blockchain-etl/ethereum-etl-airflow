class TableDefinitionFileType:
    JSON = 'json'
    SQL = 'sql'


class TableDefinition:
    def __init__(
            self,
            table_name,
            filetype: TableDefinitionFileType,
            filepath,
            content_hash,
            ref_dependencies,
            dependencies
    ):
        self.table_name = table_name
        self.filetype = filetype
        self.filepath = filepath
        self.ref_dependencies = ref_dependencies
        self.content_hash = content_hash
        self.dependencies = dependencies



