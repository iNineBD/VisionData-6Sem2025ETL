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
        dimension_tables = [
            ("Dim_Companies", "CompanyId", ["Name", "Segmento", "CNPJ"]),
            ("Dim_Users", "UserId", ["FullName", "CompanyKey", "IsVIP"]),
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
            self._load_fact_tickets(transformed_data["Fact_Tickets"])

    def _load_dimension(
        self,
        df: pd.DataFrame,
        table_name: str,
        business_key: str,
        columns_to_update: List[str],
    ):
        """
        Carrega dados para uma tabela de dimensão usando um bulk merge.
        """
        temp_table_name = (
            f"##{table_name.split('.')[-1].replace('[','').replace(']','')}_temp"
        )
        logger.info(
            f"Iniciando carga para a dimensão {table_name}. Total de {len(df)} registros."
        )

        try:
            # 1. Cria a tabela temporária
            logger.debug(f"Criando tabela temporária: {temp_table_name}")
            create_temp_table_sql = (
                f"SELECT TOP 0 * INTO {temp_table_name} FROM {table_name}"
            )
            self.db.execute_query(create_temp_table_sql)

            # 2. Insere os dados do DataFrame na tabela temporária
            if not df.empty:
                df_prepared = df.where(pd.notnull(df), None)

                cols = ", ".join([f"[{c}]" for c in df_prepared.columns])
                placeholders = ", ".join(["?"] * len(df_prepared.columns))
                insert_sql = (
                    f"INSERT INTO {temp_table_name} ({cols}) VALUES ({placeholders})"
                )

                chunksize = 1000
                total_rows = len(df_prepared)
                logger.info(
                    f"Iniciando inserção de {total_rows} registros em lotes de {chunksize}..."
                )
                for i in range(0, total_rows, chunksize):
                    chunk = df_prepared[i : i + chunksize]
                    self.db.cursor.executemany(insert_sql, chunk.values.tolist())
                    # <-- LOG DE PROGRESSO ADICIONADO AQUI -->
                    logger.info(
                        f"Progresso da carga para {table_name}: {i + len(chunk)}/{total_rows} registros inseridos."
                    )

                self.db.connection.commit()
                logger.info("Todos os registros foram inseridos na tabela temporária.")

            # 3. Constrói e executa o MERGE
            logger.info("Iniciando a operação MERGE a partir da tabela temporária.")
            update_set = ", ".join(
                [f"Target.[{col}] = Source.[{col}]" for col in columns_to_update]
            )
            insert_cols = ", ".join([f"[{col}]" for col in df.columns])
            source_cols = ", ".join([f"Source.[{col}]" for col in df.columns])

            merge_sql = f"""
            MERGE {table_name} AS Target
            USING {temp_table_name} AS Source
            ON Target.[{business_key}] = Source.[{business_key}]
            WHEN MATCHED THEN
                UPDATE SET {update_set}
            WHEN NOT MATCHED BY Target THEN
                INSERT ({insert_cols})
                VALUES ({source_cols});
            """
            self.db.execute_query(merge_sql)
            logger.info(f"Operação MERGE para a dimensão {table_name} concluída.")

        except Exception as e:
            logger.error(
                f"Erro durante a carga da dimensão {table_name}: {e}", exc_info=True
            )
            try:
                self.db.connection.rollback()
            except Exception as rb_e:
                logger.error(f"Erro ao tentar fazer rollback: {rb_e}")
            raise

        finally:
            # 4. Garante que a tabela temporária seja removida
            logger.debug(f"Removendo tabela temporária: {temp_table_name}")
            self.db.execute_query(f"DROP TABLE IF EXISTS {temp_table_name}")
            logger.info(f"Carga da dimensão {table_name} finalizada.")

    def _load_fact_tickets(self, df: pd.DataFrame):
        fact_table = self._get_table_name_with_schema("Fact_Tickets")
        temp_fact_table = "##Fact_Tickets_temp"
        logger.info(
            f"Iniciando carga para a tabela de fatos {fact_table}. Total de {len(df)} registros."
        )

        try:
            # 1. Adicionar colunas de chaves substitutas ao DataFrame
            logger.info("Buscando chaves substitutas para a tabela de fatos...")
            keys_df = df.apply(self._get_surrogate_keys, axis=1)
            keys_df = keys_df.apply(pd.Series)

            df_with_keys = pd.concat([df.reset_index(drop=True), keys_df], axis=1)

            initial_rows = len(df_with_keys)
            df_with_keys.dropna(subset=["TicketKeyD", "UserKey"], inplace=True)
            if len(df_with_keys) < initial_rows:
                logger.warning(
                    f"{initial_rows - len(df_with_keys)} registros de fatos foram pulados por falta de chaves."
                )

            if df_with_keys.empty:
                logger.info("Nenhum registro de fato válido para carregar.")
                return

            # 2. Criar e carregar tabela temporária
            logger.info(
                f"Criando e carregando tabela temporária de fatos: {temp_fact_table}"
            )
            self.db.execute_query(
                f"SELECT TOP 0 * INTO {temp_fact_table} FROM {fact_table}"
            )

            insert_cols = [
                "TicketKeyD",
                "UserKey",
                "AgentKey",
                "CompanyKey",
                "CategoryKey",
                "PriorityKey",
                "StatusKey",
                "ProductKey",
                "TagKey",
                "QtTickets",
                "TicketId",
            ]
            df_to_insert = pd.DataFrame()
            df_to_insert["TicketKeyD"] = df_with_keys["TicketKeyD"]
            df_to_insert["UserKey"] = df_with_keys["UserKey"]
            df_to_insert["AgentKey"] = df_with_keys["AgentKey"]
            df_to_insert["CompanyKey"] = df_with_keys["CompanyKey"]
            df_to_insert["CategoryKey"] = df_with_keys["CategoryKey"]
            df_to_insert["PriorityKey"] = df_with_keys["PriorityKey"]
            df_to_insert["StatusKey"] = df_with_keys["StatusKey"]
            df_to_insert["ProductKey"] = df_with_keys["ProductKey"]
            df_to_insert["TagKey"] = df_with_keys.get("TagKey", -1)
            df_to_insert["QtTickets"] = df_with_keys["QtTickets"]
            df_to_insert["TicketId"] = df_with_keys["TicketId_BK"]

            df_to_insert = df_to_insert.where(pd.notnull(df_to_insert), None)

            cols_str = ", ".join([f"[{c}]" for c in insert_cols])
            placeholders = ", ".join(["?"] * len(insert_cols))
            insert_sql = (
                f"INSERT INTO {temp_fact_table} ({cols_str}) VALUES ({placeholders})"
            )
            self.db.cursor.executemany(insert_sql, df_to_insert.values.tolist())
            self.db.connection.commit()
            logger.info(
                f"{len(df_to_insert)} registros inseridos na tabela temporária de fatos."
            )

            # 3. Executa o MERGE
            logger.info("Iniciando a operação MERGE para a tabela de fatos.")
            merge_sql = f"""
            MERGE {fact_table} AS Target
            USING {temp_fact_table} AS Source
            ON Target.TicketKeyD = Source.TicketKeyD
            WHEN NOT MATCHED BY Target THEN
                INSERT ({cols_str})
                VALUES ({', '.join([f'Source.[{c}]' for c in insert_cols])});
            """
            self.db.execute_query(merge_sql)
            logger.info(f"Operação MERGE para a tabela {fact_table} concluída.")

        except Exception as e:
            logger.error(
                f"Erro durante a carga da tabela de fatos {fact_table}: {e}",
                exc_info=True,
            )
            try:
                self.db.connection.rollback()
            except Exception as rb_e:
                logger.error(f"Erro ao tentar fazer rollback: {rb_e}")
            raise
        finally:
            logger.info(f"Removendo tabela temporária de fatos: {temp_fact_table}")
            self.db.execute_query(f"DROP TABLE IF EXISTS {temp_fact_table}")
            logger.info("Carga da tabela de fatos finalizada.")

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
