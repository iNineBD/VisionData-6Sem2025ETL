import os
from typing import Dict, List

import aspectlib
import pandas as pd

from config.aop_logging import log_execution
from config.db_connector import DBConnector
from config.logger import setup_logger

logger = setup_logger(__name__)


class LoadDwService:
    def __init__(self, db_connection: DBConnector):
        self.db = db_connection
        self.schema = os.getenv("DW_SCHEMA")

    def _get_table_name_with_schema(self, table_name: str) -> str:
        if self.schema:
            return f"[{self.schema}].[{table_name}]"
        return f"[{table_name}]"

    def load(self, transformed_data: Dict[str, pd.DataFrame]):
        """CORREÇÃO: Atualiza a lista de colunas para Dim_Users."""
        dimension_tables = [
            ("Dim_Companies", "CompanyId", ["Name", "Segmento", "CNPJ"]),
            (
                "Dim_Users",
                "UserId",
                ["FullName", "CompanyKey", "IsVIP"],
            ),  # <-- CORRIGIDO
            ("Dim_Agents", "AgentId", ["FullName", "DepartmentName", "IsActive"]),
            ("Dim_Products", "ProductId", ["Name", "Code", "IsActive"]),
            (
                "Dim_Categories",
                "SubcategoryId",
                ["CategoryId", "CategoryName", "SubcategoryName"],
            ),
            ("Dim_Status", "StatusId", ["Name"]),
            ("Dim_Priorities", "PriorityId", ["Name", "Weight"]),
            ("Dim_Tags", "TagId", ["Name"]),
            ("Dim_Tickets", "TicketId", ["Channel"]),
        ]
        for table_name, business_key, columns in dimension_tables:
            if (
                table_name in transformed_data
                and not transformed_data[table_name].empty
            ):
                full_table_name = self._get_table_name_with_schema(table_name)
                logger.info(f"Carregando dados para a dimensão: {full_table_name}")
                self._load_dimension(
                    df=transformed_data[table_name],
                    table_name=full_table_name,
                    business_key=business_key,
                    columns_to_update=columns,
                )
        if (
            "Fact_Tickets" in transformed_data
            and not transformed_data["Fact_Tickets"].empty
        ):
            full_table_name = self._get_table_name_with_schema("Fact_Tickets")
            logger.info(f"Carregando dados para a tabela de fatos: {full_table_name}")
            self._load_fact_tickets(transformed_data["Fact_Tickets"])

    def _load_dimension(
        self,
        df: pd.DataFrame,
        table_name: str,
        business_key: str,
        columns_to_update: List[str],
    ):
        for index, row in df.iterrows():
            update_set = ", ".join([f"Target.[{col}] = ?" for col in columns_to_update])
            insert_cols = ", ".join([f"[{col}]" for col in df.columns])
            insert_placeholders = ", ".join(["?"] * len(df.columns))
            merge_sql = f"""
            MERGE {table_name} AS Target
            USING (SELECT ? AS {business_key}) AS Source
            ON Target.[{business_key}] = Source.[{business_key}]
            WHEN MATCHED THEN
                UPDATE SET {update_set}
            WHEN NOT MATCHED BY Target THEN
                INSERT ({insert_cols})
                VALUES ({insert_placeholders});
            """
            params = (
                [row[business_key]]
                + [row[col] for col in columns_to_update]
                + list(row.values)
            )
            self.db.execute_query(merge_sql, params)
        logger.info(
            f"Carga da dimensão {table_name} concluída ({len(df)} linhas processadas)."
        )

    def _load_fact_tickets(self, df: pd.DataFrame):
        fact_table = self._get_table_name_with_schema("Fact_Tickets")
        for index, row in df.iterrows():
            keys = self._get_surrogate_keys(row)
            if not keys.get("TicketKeyD") or not keys.get("UserKey"):
                logger.warning(
                    f"Chaves não encontradas para o ticket {row['TicketId_BK']}. Pulando registro."
                )
                continue
            merge_sql = f"""
            MERGE {fact_table} AS Target
            USING (SELECT ? AS TicketKeyD) AS Source
            ON Target.TicketKeyD = Source.TicketKeyD
            WHEN NOT MATCHED BY Target THEN
                INSERT (TicketKeyD, UserKey, AgentKey, CompanyKey, CategoryKey, PriorityKey, StatusKey, ProductKey, TagKey, QtTickets, TicketId)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """
            params = [
                keys["TicketKeyD"],
                keys["TicketKeyD"],
                keys["UserKey"],
                keys["AgentKey"],
                keys["CompanyKey"],
                keys["CategoryKey"],
                keys["PriorityKey"],
                keys["StatusKey"],
                keys["ProductKey"],
                keys.get("TagKey", -1),
                row["QtTickets"],
                row["TicketId_BK"],
            ]
            self.db.execute_query(merge_sql, params)
        logger.info(f"Carga da {fact_table} concluída ({len(df)} linhas processadas).")

    def _get_surrogate_keys(self, fact_row: pd.Series) -> Dict:
        keys = {}
        lookups = [
            ("Dim_Tickets", "TicketId_BK", "TicketId", "TicketKey"),
            ("Dim_Users", "UserId_BK", "UserId", "UserKey"),
            ("Dim_Agents", "AgentId_BK", "AgentId", "AgentKey"),
            ("Dim_Companies", "CompanyId_BK", "CompanyId", "CompanyKey"),
            ("Dim_Categories", "CategoryId_BK", "CategoryId", "CategoryKey"),
            ("Dim_Priorities", "PriorityId_BK", "PriorityId", "PriorityKey"),
            ("Dim_Status", "StatusId_BK", "StatusId", "StatusKey"),
            ("Dim_Products", "ProductId_BK", "ProductId", "ProductKey"),
        ]
        for dim_table, bk_column, dim_bk_col, key_to_fetch in lookups:
            table_name = self._get_table_name_with_schema(dim_table)
            business_key_value = fact_row.get(bk_column)
            if pd.isna(business_key_value):
                keys[key_to_fetch] = None
                continue
            query = f"SELECT {key_to_fetch} FROM {table_name} WHERE {dim_bk_col} = ?"
            result = self.db.fetch_all(query, [business_key_value])
            if result:
                key_name = "TicketKeyD" if key_to_fetch == "TicketKey" else key_to_fetch
                keys[key_name] = result[0][0]
            else:
                keys[key_to_fetch] = None
        return keys


aspectlib.weave(LoadDwService, log_execution)
