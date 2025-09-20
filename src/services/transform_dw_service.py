from typing import Any, Dict, List

import aspectlib
import pandas as pd

from config.aop_logging import log_execution


class TransformDwService:
    def transform(self, extracted_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        tickets_df = pd.DataFrame(extracted_data.get("tickets", []))
        if tickets_df.empty:
            return {}

        tags_data = extracted_data.get("tags", {})

        # As funções agora retornam DataFrames com chaves de negócio consistentes
        dim_companies = self._create_dim_companies(tickets_df)
        dim_users = self._create_dim_users(tickets_df)
        dim_agents = self._create_dim_agents(tickets_df)
        dim_products = self._create_dim_products(tickets_df)
        dim_categories = self._create_dim_categories(tickets_df)
        dim_status = self._create_dim_status(tickets_df)
        dim_priorities = self._create_dim_priorities(tickets_df)
        dim_tags = self._create_dim_tags(tags_data)
        dim_channel = self._create_dim_channel(tickets_df)
        fact_tickets = self._create_fact_tickets(tickets_df, tags_data)

        return {
            "Dim_Companies": dim_companies,
            "Dim_Users": dim_users,
            "Dim_Agents": dim_agents,
            "Dim_Products": dim_products,
            "Dim_Categories": dim_categories,
            "Dim_Status": dim_status,
            "Dim_Priorities": dim_priorities,
            "Dim_Tags": dim_tags,
            "Dim_Channel": dim_channel,
            "Fact_Tickets": fact_tickets,
        }

    def _create_dim_companies(self, df: pd.DataFrame) -> pd.DataFrame:
        dim = df[
            ["company_id", "company_name", "company_segment", "company_cnpj"]
        ].copy()
        dim.rename(
            columns={
                "company_id": "CompanyId_BK",  # Renomeado para Business Key
                "company_name": "Name",
                "company_segment": "Segmento",
                "company_cnpj": "CNPJ",
            },
            inplace=True,
        )
        return dim.drop_duplicates(subset=["CompanyId_BK"]).reset_index(drop=True)

    def _create_dim_users(self, df: pd.DataFrame) -> pd.DataFrame:
        dim = df[["user_id", "user_full_name", "user_is_vip"]].copy()
        dim.rename(
            columns={
                "user_id": "UserId_BK",  # Renomeado para Business Key
                "user_full_name": "FullName",
                "user_is_vip": "IsVIP",
            },
            inplace=True,
        )
        return dim.drop_duplicates(subset=["UserId_BK"]).reset_index(drop=True)

    def _create_dim_agents(self, df: pd.DataFrame) -> pd.DataFrame:
        dim = df[["agent_id", "agent_full_name", "agent_department"]].copy()
        dim["IsActive"] = True
        dim.rename(
            columns={
                "agent_id": "AgentId_BK",  # Renomeado para Business Key
                "agent_full_name": "FullName",
                "agent_department": "DepartmentName",
            },
            inplace=True,
        )
        return dim.drop_duplicates(subset=["AgentId_BK"]).reset_index(drop=True)

    def _create_dim_products(self, df: pd.DataFrame) -> pd.DataFrame:
        dim = df[["product_id", "product_name", "product_code"]].copy()
        dim["IsActive"] = True
        dim.rename(
            columns={
                "product_id": "ProductId_BK",  # Renomeado para Business Key
                "product_name": "Name",
                "product_code": "Code",
            },
            inplace=True,
        )
        return dim.drop_duplicates(subset=["ProductId_BK"]).reset_index(drop=True)

    def _create_dim_categories(self, df: pd.DataFrame) -> pd.DataFrame:
        dim = df[["category_id", "category_name", "subcategory_name"]].copy()
        dim.rename(
            columns={
                "category_id": "CategoryId_BK",  # Renomeado para Business Key
                "category_name": "CategoryName",
                "subcategory_name": "SubcategoryName",
            },
            inplace=True,
        )
        return dim.drop_duplicates(subset=["CategoryId_BK"]).reset_index(drop=True)

    def _create_dim_status(self, df: pd.DataFrame) -> pd.DataFrame:
        dim = df[["current_status"]].copy()
        dim.rename(
            columns={"current_status": "StatusId_BK"}, inplace=True
        )  # Renomeado para Business Key
        dim["Name"] = "Status_" + dim["StatusId_BK"].astype(str)
        return dim.drop_duplicates(subset=["StatusId_BK"]).reset_index(drop=True)

    def _create_dim_priorities(self, df: pd.DataFrame) -> pd.DataFrame:
        dim = df[["priority"]].copy()
        dim.rename(
            columns={"priority": "PriorityId_BK"}, inplace=True
        )  # Renomeado para Business Key
        dim["Name"] = "Priority_" + dim["PriorityId_BK"].astype(str)
        dim["Weight"] = 0
        return dim.drop_duplicates(subset=["PriorityId_BK"]).reset_index(drop=True)

    def _create_dim_tags(self, tags_data: Dict[str, List[Dict]]) -> pd.DataFrame:
        all_tags = {}
        for ticket_tags in tags_data.values():
            for tag_dict in ticket_tags:
                tag_id = tag_dict.get("tag_id")
                tag_name = tag_dict.get("tag_name")
                if tag_id is not None:
                    all_tags[tag_id] = tag_name
        if not all_tags:
            return pd.DataFrame(columns=["TagId_BK", "Name"])
        df = pd.DataFrame(
            list(all_tags.items()), columns=["TagId_BK", "Name"]
        )  # Renomeado para Business Key
        return df

    def _create_dim_channel(self, df: pd.DataFrame) -> pd.DataFrame:
        dim = df[["channel"]].copy().dropna().drop_duplicates()
        dim.rename(columns={"channel": "ChannelName"}, inplace=True)
        # Esta dimensão não tem uma chave de negócio numérica, o próprio nome é a chave.
        return dim.reset_index(drop=True)

    def _create_fact_tickets(
        self, df: pd.DataFrame, tags_data: Dict[str, List[Dict]]
    ) -> pd.DataFrame:
        ticket_to_tags_map = {
            ticket_id: [tag.get("tag_id") for tag in tags]
            for ticket_id, tags in tags_data.items()
        }

        df["TagId_BK_List"] = df["ticket_id"].astype(str).map(ticket_to_tags_map)
        df["TagId_BK_List"] = df["TagId_BK_List"].apply(
            lambda x: x if (isinstance(x, list) and x) else [None]
        )

        fact_exploded = df.explode("TagId_BK_List").rename(
            columns={"TagId_BK_List": "TagId_BK"}
        )

        fact = fact_exploded[
            [
                "ticket_id",
                "user_id",
                "agent_id",
                "company_id",
                "category_id",
                "priority",
                "current_status",
                "product_id",
                "channel",
                "TagId_BK",
            ]
        ].copy()

        fact.rename(
            columns={
                "ticket_id": "TicketKey",  # A chave do fato pode ser mantida
                "user_id": "UserId_BK",
                "agent_id": "AgentId_BK",
                "company_id": "CompanyId_BK",
                "category_id": "CategoryId_BK",
                "priority": "PriorityId_BK",
                "current_status": "StatusId_BK",
                "product_id": "ProductId_BK",
                "channel": "Channel_BK",
            },
            inplace=True,
        )

        fact["QtTickets"] = 1
        return fact.reset_index(drop=True)


aspectlib.weave(TransformDwService, log_execution)
