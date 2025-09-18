from typing import Any, Dict, List

import aspectlib
import pandas as pd

from config.aop_logging import log_execution


class TransformDwService:
    def transform(self, extracted_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        tickets_df = pd.DataFrame(extracted_data.get("tickets", []))
        if tickets_df.empty:
            return {}

        dim_companies = self._create_dim_companies(tickets_df)
        dim_users = self._create_dim_users(tickets_df)
        dim_agents = self._create_dim_agents(tickets_df)
        dim_products = self._create_dim_products(tickets_df)
        dim_categories = self._create_dim_categories(tickets_df)
        dim_status = self._create_dim_status(tickets_df)
        dim_priorities = self._create_dim_priorities(tickets_df)
        dim_tags = self._create_dim_tags(extracted_data.get("tags", {}))
        dim_tickets = self._create_dim_tickets(tickets_df)
        fact_tickets = self._create_fact_tickets(tickets_df)

        return {
            "Dim_Companies": dim_companies,
            "Dim_Users": dim_users,
            "Dim_Agents": dim_agents,
            "Dim_Products": dim_products,
            "Dim_Categories": dim_categories,
            "Dim_Status": dim_status,
            "Dim_Priorities": dim_priorities,
            "Dim_Tags": dim_tags,
            "Dim_Tickets": dim_tickets,
            "Fact_Tickets": fact_tickets,
        }

    def _create_dim_companies(self, df: pd.DataFrame) -> pd.DataFrame:
        dim = df[
            ["company_id", "company_name", "company_segment", "company_cnpj"]
        ].copy()
        dim.rename(
            columns={
                "company_id": "CompanyId",
                "company_name": "Name",
                "company_segment": "Segmento",
                "company_cnpj": "CNPJ",
            },
            inplace=True,
        )
        return dim.drop_duplicates(subset=["CompanyId"]).reset_index(drop=True)

    def _create_dim_users(self, df: pd.DataFrame) -> pd.DataFrame:
        """CORREÇÃO: Renomeia company_id para CompanyKey."""
        dim = df[["user_id", "user_full_name", "company_id", "user_is_vip"]].copy()
        dim.rename(
            columns={
                "user_id": "UserId",
                "user_full_name": "FullName",
                "company_id": "CompanyKey",  # <-- CORRIGIDO
                "user_is_vip": "IsVIP",
            },
            inplace=True,
        )
        return dim.drop_duplicates(subset=["UserId"]).reset_index(drop=True)

    def _create_dim_agents(self, df: pd.DataFrame) -> pd.DataFrame:
        dim = df[["agent_id", "agent_full_name", "agent_department"]].copy()
        dim["IsActive"] = True
        dim.rename(
            columns={
                "agent_id": "AgentId",
                "agent_full_name": "FullName",
                "agent_department": "DepartmentName",
            },
            inplace=True,
        )
        return dim.drop_duplicates(subset=["AgentId"]).reset_index(drop=True)

    def _create_dim_products(self, df: pd.DataFrame) -> pd.DataFrame:
        dim = df[["product_id", "product_name", "product_code"]].copy()
        dim["IsActive"] = True
        dim.rename(
            columns={
                "product_id": "ProductId",
                "product_name": "Name",
                "product_code": "Code",
            },
            inplace=True,
        )
        return dim.drop_duplicates(subset=["ProductId"]).reset_index(drop=True)

    def _create_dim_categories(self, df: pd.DataFrame) -> pd.DataFrame:
        dim = df[
            ["category_id", "category_name", "subcategory_id", "subcategory_name"]
        ].copy()
        dim.rename(
            columns={
                "category_id": "CategoryId",
                "category_name": "CategoryName",
                "subcategory_id": "SubcategoryId",
                "subcategory_name": "SubcategoryName",
            },
            inplace=True,
        )
        return dim.drop_duplicates(subset=["SubcategoryId"]).reset_index(drop=True)

    def _create_dim_status(self, df: pd.DataFrame) -> pd.DataFrame:
        dim = df[["current_status"]].copy()
        dim.rename(columns={"current_status": "StatusId"}, inplace=True)
        dim["Name"] = "Status_" + dim["StatusId"].astype(str)
        return dim.drop_duplicates(subset=["StatusId"]).reset_index(drop=True)

    def _create_dim_priorities(self, df: pd.DataFrame) -> pd.DataFrame:
        dim = df[["priority"]].copy()
        dim.rename(columns={"priority": "PriorityId"}, inplace=True)
        dim["Name"] = "Priority_" + dim["PriorityId"].astype(str)
        dim["Weight"] = 0
        return dim.drop_duplicates(subset=["PriorityId"]).reset_index(drop=True)

    def _create_dim_tags(self, tags_data: Dict[str, List[Dict]]) -> pd.DataFrame:
        """CORREÇÃO: Processa ID e Nome da tag."""
        all_tags = {}
        for ticket_tags in tags_data.values():
            for tag_dict in ticket_tags:
                tag_id = tag_dict.get("tag_id")
                tag_name = tag_dict.get("tag_name")
                if tag_id is not None:
                    all_tags[tag_id] = tag_name
        if not all_tags:
            return pd.DataFrame(columns=["TagId", "Name"])
        df = pd.DataFrame(list(all_tags.items()), columns=["TagId", "Name"])
        return df

    def _create_dim_tickets(self, df: pd.DataFrame) -> pd.DataFrame:
        dim = df[["ticket_id", "channel"]].copy()
        dim.rename(
            columns={"ticket_id": "TicketId", "channel": "Channel"}, inplace=True
        )
        return dim.drop_duplicates(subset=["TicketId"]).reset_index(drop=True)

    def _create_fact_tickets(self, df: pd.DataFrame) -> pd.DataFrame:
        fact = df[
            [
                "ticket_id",
                "user_id",
                "agent_id",
                "company_id",
                "category_id",
                "priority",
                "current_status",
                "product_id",
            ]
        ].copy()
        fact.rename(
            columns={
                "ticket_id": "TicketId_BK",
                "user_id": "UserId_BK",
                "agent_id": "AgentId_BK",
                "company_id": "CompanyId_BK",
                "category_id": "CategoryId_BK",
                "priority": "PriorityId_BK",
                "current_status": "StatusId_BK",
                "product_id": "ProductId_BK",
            },
            inplace=True,
        )
        fact["TagKey"] = -1
        fact["QtTickets"] = 1
        fact["TicketKeyD"] = -1
        return fact.reset_index(drop=True)


aspectlib.weave(TransformDwService, log_execution)
