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
        dimension_mappings = [
            (
                "Dim_Companies",
                "CompanyId_BK",
                "CompanyId_BK",
                ["Name", "Segmento", "CNPJ"],
            ),
            ("Dim_Users", "UserId_BK", "UserId_BK", ["FullName", "IsVIP"]),
            (
                "Dim_Agents",
                "AgentId_BK",
                "AgentId_BK",
                ["FullName", "DepartmentName", "IsActive"],
            ),
            (
                "Dim_Products",
                "ProductId_BK",
                "ProductId_BK",
                ["Name", "Code", "IsActive"],
            ),
            (
                "Dim_Categories",
                "CategoryId_BK",
                "CategoryId_BK",
                ["CategoryName", "SubcategoryName"],
            ),
            ("Dim_Status", "StatusId_BK", "StatusId_BK", ["Name"]),
            ("Dim_Priorities", "PriorityId_BK", "PriorityId_BK", ["Name", "Weight"]),
            ("Dim_Tags", "TagId_BK", "TagId_BK", ["Name"]),
            (
                "Dim_Channel",
                "ChannelName",
                "ChannelName",
                [],
            ),
        ]

        for table_name, bk_col_df, bk_col_db, attribute_cols in dimension_mappings:
            if (
                table_name in transformed_data
                and not transformed_data[table_name].empty
            ):
                df = transformed_data[table_name].copy()
                full_table_name = self._get_table_name_with_schema(table_name)
                self._load_dimension(
                    df=df,
                    table_name=full_table_name,
                    business_key_col=bk_col_df,
                    columns_to_update=attribute_cols,
                )

        if (
            "Fact_Tickets" in transformed_data
            and not transformed_data["Fact_Tickets"].empty
        ):
            self._load_fact_tickets(transformed_data["Fact_Tickets"])

    def _load_dimension(
        self,
        df: pd.DataFrame,
        table_name: str,
        business_key_col: str,
        columns_to_update: List[str],
    ):
        temp_table_name = (
            f"##{table_name.split('.')[-1].replace('[','').replace(']','')}_temp"
        )
        logger.info(
            f"Iniciando carga para a dimensão {table_name}. Total de {len(df)} registros."
        )

        try:
            cols_to_insert = [business_key_col] + columns_to_update
            df_dim = df[cols_to_insert].drop_duplicates()

            self.db.execute_query(
                f"SELECT TOP 0 * INTO {temp_table_name} FROM {table_name}"
            )

            if not df_dim.empty:
                df_prepared = df_dim.where(pd.notnull(df_dim), None)

                cols_str = ", ".join([f"[{c}]" for c in df_prepared.columns])
                placeholders = ", ".join(["?"] * len(df_prepared.columns))
                insert_sql = f"INSERT INTO {temp_table_name} ({cols_str}) VALUES ({placeholders})"

                self.db.cursor.executemany(insert_sql, df_prepared.values.tolist())
                self.db.connection.commit()

            logger.info("Iniciando a operação MERGE a partir da tabela temporária.")

            update_clause = ""
            if columns_to_update:
                update_set = ", ".join(
                    [f"Target.[{col}] = Source.[{col}]" for col in columns_to_update]
                )
                update_clause = f"WHEN MATCHED THEN UPDATE SET {update_set}"

            insert_cols_list = [business_key_col] + columns_to_update
            insert_cols = ", ".join([f"[{c}]" for c in insert_cols_list])
            source_cols = ", ".join([f"Source.[{c}]" for c in insert_cols_list])

            merge_sql = f"""
            MERGE {table_name} AS Target
            USING {temp_table_name} AS Source
            ON Target.[{business_key_col}] = Source.[{business_key_col}]
            {update_clause}
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
            raise

        finally:
            self.db.execute_query(f"DROP TABLE IF EXISTS {temp_table_name}")
            logger.info(f"Carga da dimensão {table_name} finalizada.")

    def _get_surrogate_keys_bulk(
        self,
        dim_table: str,
        bk_column: str,
        sk_column: str,
        business_keys: list,
    ) -> pd.DataFrame:
        table_name = self._get_table_name_with_schema(dim_table)
        placeholders = ", ".join(["?"] * len(business_keys))

        query = f"SELECT DISTINCT [{bk_column}] as bk, [{sk_column}] as sk FROM {table_name} WHERE [{bk_column}] IN ({placeholders})"
        results = self.db.fetch_all(query, business_keys)

        if results:
            plain_results = [tuple(row) for row in results]
            df = pd.DataFrame(plain_results, columns=[bk_column, sk_column])
            return df

        return pd.DataFrame(columns=[bk_column, sk_column])

    def _load_fact_tickets(self, df: pd.DataFrame):
        fact_table = self._get_table_name_with_schema("Fact_Tickets")
        temp_fact_table = "##Fact_Tickets_temp"
        logger.info(
            f"Iniciando carga para a tabela de fatos {fact_table}. Total de {len(df)} registros."
        )

        try:
            lookups = [
                ("Dim_Users", "UserId_BK", "UserId_BK", "UserKey"),
                ("Dim_Agents", "AgentId_BK", "AgentId_BK", "AgentKey"),
                ("Dim_Companies", "CompanyId_BK", "CompanyId_BK", "CompanyKey"),
                ("Dim_Categories", "CategoryId_BK", "CategoryId_BK", "CategoryKey"),
                ("Dim_Priorities", "PriorityId_BK", "PriorityId_BK", "PriorityKey"),
                ("Dim_Status", "StatusId_BK", "StatusId_BK", "StatusKey"),
                ("Dim_Products", "ProductId_BK", "ProductId_BK", "ProductKey"),
                ("Dim_Tags", "TagId_BK", "TagId_BK", "TagKey"),
                ("Dim_Channel", "Channel_BK", "ChannelName", "ChannelKey"),
            ]

            df_with_keys = df.copy()

            for dim_table, bk_col_df, bk_col_db, sk_to_fetch in lookups:
                unique_bks = df_with_keys[bk_col_df].dropna().unique().tolist()
                if not unique_bks:
                    df_with_keys[sk_to_fetch] = pd.NA
                    continue

                keys_df = self._get_surrogate_keys_bulk(
                    dim_table, bk_col_db, sk_to_fetch, unique_bks
                )

                if not keys_df.empty:
                    df_with_keys[bk_col_df] = df_with_keys[bk_col_df].astype(str)
                    keys_df[bk_col_db] = keys_df[bk_col_db].astype(str)

                    df_with_keys = pd.merge(
                        df_with_keys,
                        keys_df,
                        left_on=bk_col_df,
                        right_on=bk_col_db,
                        how="left",
                    )
                    df_with_keys.drop(
                        columns=[bk_col_db], inplace=True, errors="ignore"
                    )
                else:
                    df_with_keys[sk_to_fetch] = pd.NA

            bk_cols_to_drop = [lkp[1] for lkp in lookups]
            df_with_keys.drop(columns=bk_cols_to_drop, inplace=True, errors="ignore")

            df_with_keys.dropna(subset=["TicketKey", "UserKey"], inplace=True)

            if df_with_keys.empty:
                logger.info("Nenhum registro de fato válido para carregar.")
                return

            final_columns = [
                "UserKey",
                "AgentKey",
                "CompanyKey",
                "CategoryKey",
                "PriorityKey",
                "StatusKey",
                "ProductKey",
                "TagKey",
                "ChannelKey",
                "QtTickets",
            ]
            if "TicketKey" in df_with_keys.columns:
                df_to_insert = df_with_keys[final_columns].copy()
            else:
                df_to_insert = df_with_keys[final_columns].copy()

            cols_with_types = []
            for col in df_to_insert.columns:
                if "Key" in col:
                    cols_with_types.append(f"[{col}] BIGINT")
                else:
                    cols_with_types.append(f"[{col}] INT")

            create_temp_table_sql = (
                f"CREATE TABLE {temp_fact_table} ({', '.join(cols_with_types)})"
            )
            self.db.execute_query(f"DROP TABLE IF EXISTS {temp_fact_table}")
            self.db.execute_query(create_temp_table_sql)

            for col in final_columns:
                if "Key" in col and col != "TicketKey":
                    if col in df_to_insert.columns:
                        df_to_insert[col] = df_to_insert[col].astype("Int64")

            def to_native(val):
                if pd.isna(val):
                    return None
                if hasattr(val, "item"):
                    return val.item()
                return val

            data_to_insert = [
                [to_native(x) for x in row]
                for row in df_to_insert.itertuples(index=False, name=None)
            ]

            cols_str = ", ".join([f"[{c}]" for c in df_to_insert.columns])
            placeholders = ", ".join(["?"] * len(df_to_insert.columns))
            insert_sql = (
                f"INSERT INTO {temp_fact_table} ({cols_str}) VALUES ({placeholders})"
            )

            chunk_size = 1000
            total_chunks = (len(data_to_insert) + chunk_size - 1) // chunk_size

            logger.info(
                f"Iniciando inserção de {len(data_to_insert)} registros em {total_chunks} lotes de {chunk_size}."
            )

            for i in range(0, len(data_to_insert), chunk_size):
                chunk = data_to_insert[i : i + chunk_size]
                self.db.cursor.executemany(insert_sql, chunk)
                self.db.connection.commit()
                logger.info(
                    f"Lote {i // chunk_size + 1}/{total_chunks} carregado com sucesso."
                )

            merge_sql = f"""
            MERGE {fact_table} AS Target
            USING {temp_fact_table} AS Source
                ON Target.[UserKey] = Source.[UserKey] AND Target.[TagKey] = Source.[TagKey]
            WHEN NOT MATCHED BY Target THEN
                INSERT ({cols_str})
                VALUES ({', '.join([f'Source.[{c}]' for c in df_to_insert.columns])});
            """

            self.db.execute_query(merge_sql)
            logger.info(f"Operação MERGE para a tabela {fact_table} concluída.")

        except Exception as e:
            logger.error(
                f"Erro durante a carga da tabela de fatos {fact_table}: {e}",
                exc_info=True,
            )
            raise
        finally:
            self.db.execute_query(f"DROP TABLE IF EXISTS {temp_fact_table}")
            logger.info("Carga da tabela de fatos finalizada.")


aspectlib.weave(LoadDwService, log_execution)
