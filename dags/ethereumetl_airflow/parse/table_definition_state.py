class TableDefinitionState:
    def __init__(
            self,
            table_definition,
            is_updated_or_dependencies_updated
    ):
        self.table_definition = table_definition
        self.is_updated_or_dependencies_updated = is_updated_or_dependencies_updated


