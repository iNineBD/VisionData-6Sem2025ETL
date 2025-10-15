from typing import Dict, List

import aspectlib
import numpy as np
import pandas as pd

from config.aop_logging import log_execution


class TransformeElasticService:
    """Service responsible for transforming ticket data to Elasticsearch format using optimized, vectorized operations."""

    @staticmethod
    def _calculate_sla_metrics(df: pd.DataFrame) -> pd.Series:
        """Calculates SLA metrics in a vectorized way."""
        created_at = pd.to_datetime(df["created_at"], errors="coerce")
        first_response_at = pd.to_datetime(df["first_response_at"], errors="coerce")
        closed_at = pd.to_datetime(df["closed_at"], errors="coerce")

        first_response_time = (first_response_at - created_at).dt.total_seconds() / 60
        resolution_time = (closed_at - created_at).dt.total_seconds() / 60

        metrics = pd.DataFrame(index=df.index)
        metrics["first_response_time_minutes"] = first_response_time.fillna(0).astype(
            int
        )
        metrics["resolution_time_minutes"] = resolution_time.fillna(0).astype(int)
        metrics["first_response_sla_breached"] = (
            first_response_time > df["sla_first_response_mins"]
        ).fillna(False)
        metrics["resolution_sla_breached"] = (
            resolution_time > df["sla_resolution_mins"]
        ).fillna(False)

        return metrics.to_dict("records")

    @staticmethod
    def _create_search_text(df: pd.DataFrame) -> pd.Series:
        """Creates search text by combining relevant fields in a vectorized way."""
        text_fields = [
            "title",
            "description",
            "company_name",
            "user_full_name",
            "agent_full_name",
            "product_name",
            "category_name",
            "subcategory_name",
        ]
        return df[text_fields].fillna("").agg(" ".join, axis=1)

    @staticmethod
    def _format_date_series(series: pd.Series) -> pd.Series:
        """Helper function to safely format a Series of date objects."""
        dt_series = pd.to_datetime(series, errors="coerce")
        return dt_series.dt.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _process_nested_data(
        df: pd.DataFrame, nested_map: Dict, col_name: str, date_col: str
    ) -> pd.Series:
        """
        Efficiently processes and formats dates in nested data structures.
        """
        nested_series = df["ticket_id_str"].map(nested_map).fillna("").apply(list)

        flat_list = []
        for ticket_id, items in nested_series.items():
            if items and isinstance(items, list):
                for item in items:
                    if isinstance(item, dict):
                        item_copy = item.copy()
                        item_copy["ticket_id_str"] = df.loc[ticket_id, "ticket_id_str"]
                        flat_list.append(item_copy)

        if not flat_list:
            return pd.Series([[]] * len(df), index=df.index, name=col_name)

        nested_df = pd.DataFrame(flat_list)

        if date_col in nested_df.columns:
            nested_df[date_col] = TransformeElasticService._format_date_series(
                nested_df[date_col]
            )

        nested_df = nested_df.replace({np.nan: None, pd.NaT: None})

        grouped = nested_df.groupby("ticket_id_str").apply(
            lambda x: x.drop(columns=["ticket_id_str"], errors="ignore").to_dict(
                "records"
            )
        )
        grouped.name = col_name

        return grouped

    @staticmethod
    def transform_tickets_batch(extracted_data: Dict) -> List[Dict]:
        """
        Transforms a batch of extracted tickets to Elasticsearch format using Pandas for high performance.
        """
        tickets = extracted_data.get("tickets", [])
        if not tickets:
            return []

        df = pd.DataFrame(tickets)
        df["ticket_id_str"] = df["ticket_id"].astype(str)
        df = df.set_index("ticket_id_str", drop=False)

        df["sla_metrics"] = TransformeElasticService._calculate_sla_metrics(df)
        df["search_text"] = TransformeElasticService._create_search_text(df)

        df["created_at_fmt"] = TransformeElasticService._format_date_series(
            df["created_at"]
        )
        df["first_response_at_fmt"] = TransformeElasticService._format_date_series(
            df["first_response_at"]
        )
        df["closed_at_fmt"] = TransformeElasticService._format_date_series(
            df["closed_at"]
        )

        status_history_series = TransformeElasticService._process_nested_data(
            df, extracted_data.get("status_history", {}), "status_history", "changed_at"
        )
        attachments_series = TransformeElasticService._process_nested_data(
            df, extracted_data.get("attachments", {}), "attachments", "uploaded_at"
        )
        audit_logs_series = TransformeElasticService._process_nested_data(
            df, extracted_data.get("audit_logs", {}), "audit_logs", "performed_at"
        )

        df = df.join([status_history_series, attachments_series, audit_logs_series])
        df["tags"] = (
            df["ticket_id_str"]
            .map(extracted_data.get("tags", {}))
            .fillna("")
            .apply(list)
        )

        for col in ["status_history", "attachments", "audit_logs", "tags"]:
            if col not in df.columns:
                df[col] = [[] for _ in range(len(df))]
            else:
                df[col] = df[col].apply(lambda d: d if isinstance(d, list) else [])

        df = df.replace({np.nan: None, pd.NaT: None})

        final_documents = []
        for _, row in df.iterrows():
            final_documents.append(
                {
                    "ticket_id": row["ticket_id_str"],
                    "title": row.get("title"),
                    "description": row.get("description"),
                    "channel": row.get("channel"),
                    "device": row.get("device"),
                    "current_status": row.get("current_status"),
                    "sla_plan": row.get("sla_plan"),
                    "priority": row.get("priority"),
                    "dates": {
                        "created_at": row["created_at_fmt"],
                        "first_response_at": row["first_response_at_fmt"],
                        "closed_at": row["closed_at_fmt"],
                    },
                    "company": {
                        "id": row.get("company_id"),
                        "name": row.get("company_name"),
                        "cnpj": row.get("company_cnpj"),
                        "segment": row.get("company_segment"),
                    },
                    "created_by_user": {
                        "id": row.get("user_id"),
                        "full_name": row.get("user_full_name"),
                        "email": row.get("user_email"),
                        "phone": row.get("user_phone"),
                        "cpf": row.get("user_cpf"),
                        "is_vip": bool(row.get("user_is_vip", False)),
                    },
                    "assigned_agent": {
                        "id": row.get("agent_id"),
                        "full_name": row.get("agent_full_name"),
                        "email": row.get("agent_email"),
                        "department": row.get("agent_department"),
                    },
                    "product": {
                        "id": row.get("product_id"),
                        "name": row.get("product_name"),
                        "code": row.get("product_code"),
                        "description": row.get("product_description"),
                    },
                    "category": {
                        "id": row.get("category_id"),
                        "name": row.get("category_name"),
                    },
                    "subcategory": {
                        "id": row.get("subcategory_id"),
                        "name": row.get("subcategory_name"),
                    },
                    "attachments": row.get("attachments", []),
                    "tags": row.get("tags", []),
                    "status_history": row.get("status_history", []),
                    "audit_logs": row.get("audit_logs", []),
                    "sla_metrics": row["sla_metrics"],
                    "search_text": row["search_text"],
                }
            )

        return final_documents


aspectlib.weave(TransformeElasticService, log_execution)
