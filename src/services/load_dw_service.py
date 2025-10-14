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

    def load(self, transformed_data: Dict[str, pd.DataFrame]):
        dimension_mappings = [
            (
                "Dim_Dates",
                None,
                None,
                ["Year", "Month", "Day", "Hour", "Minute"],
            ),
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
            ("Dim_Priorities", "PriorityId_BK", "PriorityId_BK", ["name"]),
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
                full_table_name = table_name
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
            f"Starting load for dimension {table_name}. Total of {len(df)} records."
        )

        try:
            if business_key_col is None:
                df_dim = df.reset_index().rename(columns={"index": "DateKey"})
                cols_to_insert = ["DateKey"] + columns_to_update
                df_dim = df_dim[cols_to_insert].dropna()
            else:
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

                self.db.cursor.fast_executemany = True
                self.db.cursor.executemany(insert_sql, df_prepared.values.tolist())
                self.db.connection.commit()
                self.db.cursor.fast_executemany = False

            if business_key_col is None:
                logger.info(
                    f"Optimizing and loading data for {table_name} using EXCEPT..."
                )

                pk_cols = ", ".join([f"[{c}]" for c in columns_to_update])
                index_name = f"IX_{temp_table_name.replace('#','')}_PK"
                create_index_sql = f"CREATE CLUSTERED INDEX {index_name} ON {temp_table_name}({pk_cols});"
                self.db.execute_query(create_index_sql)

                insert_cols_str = ", ".join([f"[{c}]" for c in cols_to_insert])
                insert_except_sql = f"""
                INSERT INTO {table_name} ({insert_cols_str})
                SELECT {insert_cols_str} FROM {temp_table_name}
                EXCEPT
                SELECT {insert_cols_str} FROM {table_name};
                """
                self.db.execute_query(insert_except_sql)
                logger.info(
                    f"INSERT...EXCEPT operation for dimension {table_name} completed."
                )

            else:
                logger.info(f"Optimizing and merging data for {table_name}...")

                index_name = f"IX_{temp_table_name.replace('#','')}"
                create_index_sql = f"CREATE UNIQUE CLUSTERED INDEX {index_name} ON {temp_table_name}([{business_key_col}]);"
                self.db.execute_query(create_index_sql)
                logger.info(f"Index created on temporary table for {table_name}.")

                update_clause = ""
                if columns_to_update:
                    update_set = ", ".join(
                        [
                            f"Target.[{col}] = Source.[{col}]"
                            for col in columns_to_update
                        ]
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
                logger.info(f"MERGE operation for dimension {table_name} completed.")

        except Exception as e:
            logger.error(
                f"Error during load of dimension {table_name}: {e}", exc_info=True
            )
            raise

        finally:
            self.db.execute_query(f"DROP TABLE IF EXISTS {temp_table_name}")
            logger.info(f"Load for dimension {table_name} finished.")

    def _get_surrogate_keys_bulk(
        self,
        dim_table: str,
        bk_column: str,
        sk_column: str,
        business_keys: list,
    ) -> pd.DataFrame:
        table_name = dim_table
        placeholders = ", ".join(["?"] * len(business_keys))

        query = f"SELECT DISTINCT [{bk_column}] as bk, [{sk_column}] as sk FROM {table_name} WHERE [{bk_column}] IN ({placeholders})"
        results = self.db.fetch_all(query, business_keys)

        if results:
            plain_results = [tuple(row) for row in results]
            df = pd.DataFrame(plain_results, columns=[bk_column, sk_column])
            return df

        return pd.DataFrame(columns=[bk_column, sk_column])

    def _load_fact_tickets(self, df: pd.DataFrame):
        fact_table = "Fact_Tickets"
        temp_fact_table = "##Fact_Tickets_temp"
        logger.info(
            f"Starting load for fact table {fact_table}. Total of {len(df)} records."
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
                logger.info("No valid fact record to load.")
                return

            final_columns = [
                "TicketKey",
                "UserKey",
                "AgentKey",
                "CompanyKey",
                "CategoryKey",
                "PriorityKey",
                "StatusKey",
                "ProductKey",
                "TagKey",
                "ChannelKey",
                "EntryDateKey",
                "ClosedDateKey",
                "FirstResponseDateKey",
                "QtTickets",
            ]
            df_to_insert = df_with_keys[
                [col for col in final_columns if col in df_with_keys.columns]
            ].copy()

            cols_with_types = []
            for col in df_to_insert.columns:
                dtype = "BIGINT" if "Key" in col else "INT"
                cols_with_types.append(f"[{col}] {dtype}")

            create_temp_table_sql = (
                f"CREATE TABLE {temp_fact_table} ({', '.join(cols_with_types)})"
            )
            self.db.execute_query(f"DROP TABLE IF EXISTS {temp_fact_table}")
            self.db.execute_query(create_temp_table_sql)

            if not df_to_insert.empty:
                logger.info(
                    f"Starting optimized bulk insert of {len(df_to_insert)} records..."
                )

                def to_native(val):
                    if pd.isna(val):
                        return None
                    if hasattr(val, "item"):
                        return val.item()
                    return val

                data_to_insert = [
                    tuple(to_native(x) for x in row)
                    for row in df_to_insert.itertuples(index=False, name=None)
                ]
                cols_str = ", ".join([f"[{c}]" for c in df_to_insert.columns])
                placeholders = ", ".join(["?"] * len(df_to_insert.columns))
                insert_sql = f"INSERT INTO {temp_fact_table} ({cols_str}) VALUES ({placeholders})"

                self.db.cursor.fast_executemany = True
                self.db.cursor.executemany(insert_sql, data_to_insert)
                self.db.connection.commit()
                self.db.cursor.fast_executemany = False
                logger.info("Bulk insert completed.")

            logger.info("Optimizing temporary table for MERGE operation...")
            index_name = f"IX_{temp_fact_table.replace('#','')}"
            self.db.execute_query(
                f"CREATE INDEX {index_name} ON {temp_fact_table}([TicketKey], [TagKey]);"
            )
            logger.info("Index created on temporary fact table.")

            cols_str = ", ".join([f"[{c}]" for c in df_to_insert.columns])
            source_cols_str = ", ".join([f"Source.[{c}]" for c in df_to_insert.columns])
            merge_on_clause = """
            (Target.[TicketKey] = Source.[TicketKey]) AND
            (Target.[TagKey] = Source.[TagKey] OR (Target.[TagKey] IS NULL AND Source.[TagKey] IS NULL))
            """

            merge_sql = f"""
            MERGE {fact_table} AS Target
            USING {temp_fact_table} AS Source
                ON {merge_on_clause}
            WHEN NOT MATCHED BY Target THEN
                INSERT ({cols_str})
                VALUES ({source_cols_str});
            """

            try:
                logger.info(f"Enabling IDENTITY_INSERT for {fact_table}...")
                self.db.execute_query(f"SET IDENTITY_INSERT {fact_table} ON;")

                logger.info(f"Executing MERGE operation for fact table {fact_table}...")
                self.db.execute_query(merge_sql)
                logger.info(f"MERGE operation for fact table {fact_table} completed.")

            finally:
                logger.info(f"Disabling IDENTITY_INSERT for {fact_table}...")
                self.db.execute_query(f"SET IDENTITY_INSERT {fact_table} OFF;")

        except Exception as e:
            logger.error(
                f"Error during load of fact table {fact_table}: {e}", exc_info=True
            )
            raise
        finally:
            self.db.execute_query(f"DROP TABLE IF EXISTS {temp_fact_table}")
            logger.info("Load for fact table finished.")


aspectlib.weave(LoadDwService, log_execution)
