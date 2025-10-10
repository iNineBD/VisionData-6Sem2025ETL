from typing import Dict, List

import aspectlib
import pandas as pd

from config.aop_logging import log_execution


class TransformeElasticService:
    """Service responsible for transforming ticket data to Elasticsearch format using optimized, vectorized operations."""

    @staticmethod
    def _calculate_sla_metrics(df: pd.DataFrame) -> List[Dict]:
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
    def _format_date(date_obj):
        """Helper function to safely format date objects."""
        if pd.isna(date_obj):
            return None
        dt = pd.to_datetime(date_obj, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def transform_tickets_batch(extracted_data: Dict) -> List[Dict]:
        """
        Transforms a batch of extracted tickets to Elasticsearch format using Pandas for high performance.
        """
        tickets = extracted_data.get("tickets", [])
        if not tickets:
            return []

        df = pd.DataFrame(tickets)

        sla_metrics_list = TransformeElasticService._calculate_sla_metrics(df)
        search_text_series = TransformeElasticService._create_search_text(df)

        attachments_map = extracted_data.get("attachments", {})
        tags_map = extracted_data.get("tags", {})
        status_history_map = extracted_data.get("status_history", {})
        audit_logs_map = extracted_data.get("audit_logs", {})

        docs_to_process = df.to_dict("records")
        final_documents = []

        for i, doc in enumerate(docs_to_process):
            ticket_id_str = str(doc.get("ticket_id"))

            status_history = status_history_map.get(ticket_id_str, [])
            for item in status_history:
                item["changed_at"] = TransformeElasticService._format_date(
                    item.get("changed_at")
                )

            attachments = attachments_map.get(ticket_id_str, [])
            for item in attachments:
                item["uploaded_at"] = TransformeElasticService._format_date(
                    item.get("uploaded_at")
                )

            audit_logs = audit_logs_map.get(ticket_id_str, [])
            for item in audit_logs:
                item["performed_at"] = TransformeElasticService._format_date(
                    item.get("performed_at")
                )

            final_documents.append(
                {
                    "ticket_id": ticket_id_str,
                    "title": doc.get("title"),
                    "description": doc.get("description"),
                    "channel": doc.get("channel"),
                    "device": doc.get("device"),
                    "current_status": doc.get("current_status"),
                    "sla_plan": doc.get("sla_plan"),
                    "priority": doc.get("priority"),
                    "dates": {
                        "created_at": TransformeElasticService._format_date(
                            doc.get("created_at")
                        ),
                        "first_response_at": TransformeElasticService._format_date(
                            doc.get("first_response_at")
                        ),
                        "closed_at": TransformeElasticService._format_date(
                            doc.get("closed_at")
                        ),
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
                    "attachments": attachments,
                    "tags": tags_map.get(ticket_id_str, []),
                    "status_history": status_history,
                    "audit_logs": audit_logs,
                    "sla_metrics": sla_metrics_list[i],
                    "search_text": search_text_series[i],
                }
            )

        return final_documents


aspectlib.weave(TransformeElasticService, log_execution)
