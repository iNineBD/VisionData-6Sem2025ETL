from typing import Dict, List

import aspectlib
import pandas as pd

from config.aop_logging import log_execution


class TransformeElasticService:
    """Service responsible for transforming ticket data to Elasticsearch format using optimized, vectorized operations."""

    @staticmethod
    def _calculate_sla_metrics(df: pd.DataFrame) -> pd.DataFrame:
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
        )
        metrics["resolution_sla_breached"] = resolution_time > df["sla_resolution_mins"]

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
    def transform_tickets_batch(extracted_data: Dict) -> List[Dict]:
        """
        Transforms a batch of extracted tickets to Elasticsearch format using Pandas for high performance.
        """
        tickets = extracted_data.get("tickets", [])
        if not tickets:
            return []

        df = pd.DataFrame(tickets)
        df["ticket_id_str"] = df["ticket_id"].astype(str)

        attachments_map = extracted_data.get("attachments", {})
        tags_map = extracted_data.get("tags", {})
        status_history_map = extracted_data.get("status_history", {})
        audit_logs_map = extracted_data.get("audit_logs", {})

        df["sla_metrics"] = TransformeElasticService._calculate_sla_metrics(df)
        df["search_text"] = TransformeElasticService._create_search_text(df)

        for col in ["created_at", "first_response_at", "closed_at"]:
            df[col] = (
                pd.to_datetime(df[col], errors="coerce")
                .dt.strftime("%Y-%m-%d %H:%M:%S")
                .fillna(None)
            )

        def map_and_clean(series, mapping):
            mapped_series = series.map(mapping)
            return [d if isinstance(d, list) else [] for d in mapped_series]

        df["attachments_list"] = map_and_clean(df["ticket_id_str"], attachments_map)
        df["tags_list"] = map_and_clean(df["ticket_id_str"], tags_map)
        df["status_history_list"] = map_and_clean(
            df["ticket_id_str"], status_history_map
        )
        df["audit_logs_list"] = map_and_clean(df["ticket_id_str"], audit_logs_map)

        docs = df.to_dict("records")

        final_documents = []
        for doc in docs:
            final_documents.append(
                {
                    "ticket_id": doc.get("ticket_id_str"),
                    "title": doc.get("title"),
                    "description": doc.get("description"),
                    "channel": doc.get("channel"),
                    "device": doc.get("device"),
                    "current_status": doc.get("current_status"),
                    "sla_plan": doc.get("sla_plan"),
                    "priority": doc.get("priority"),
                    "dates": {
                        "created_at": doc.get("created_at"),
                        "first_response_at": doc.get("first_response_at"),
                        "closed_at": doc.get("closed_at"),
                    },
                    "company": {
                        "id": doc.get("company_id"),
                        "name": doc.get("company_name"),
                        "cnpj": doc.get("company_cnpj"),
                        "segment": doc.get("company_segment"),
                    },
                    "created_by_user": {
                        "id": doc.get("user_id"),
                        "full_name": doc.get("user_full_name"),
                        "email": doc.get("user_email"),
                        "phone": doc.get("user_phone"),
                        "cpf": doc.get("user_cpf"),
                        "is_vip": bool(doc.get("user_is_vip", False)),
                    },
                    "assigned_agent": {
                        "id": doc.get("agent_id"),
                        "full_name": doc.get("agent_full_name"),
                        "email": doc.get("agent_email"),
                        "department": doc.get("agent_department"),
                    },
                    "product": {
                        "id": doc.get("product_id"),
                        "name": doc.get("product_name"),
                        "code": doc.get("product_code"),
                        "description": doc.get("product_description"),
                    },
                    "category": {
                        "id": doc.get("category_id"),
                        "name": doc.get("category_name"),
                    },
                    "subcategory": {
                        "id": doc.get("subcategory_id"),
                        "name": doc.get("subcategory_name"),
                    },
                    "attachments": doc.get("attachments_list"),
                    "tags": doc.get("tags_list"),
                    "status_history": doc.get("status_history_list"),
                    "audit_logs": doc.get("audit_logs_list"),
                    "sla_metrics": doc.get("sla_metrics"),
                    "search_text": doc.get("search_text"),
                }
            )

        return final_documents


aspectlib.weave(TransformeElasticService, log_execution)
