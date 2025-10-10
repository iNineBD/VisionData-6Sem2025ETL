from typing import Any, Dict, List

import aspectlib
import pandas as pd

from config.aop_logging import log_execution, setup_logger

logger = setup_logger(__name__)


class TransformDwService:
    def transform(self, extracted_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        tickets_df = pd.DataFrame(extracted_data.get("tickets", []))
        if tickets_df.empty:
            return {}

        tags_data = extracted_data.get("tags", {})

        dim_dates = self._create_dim_dates(tickets_df)
        dim_companies = self._create_dim_companies(tickets_df)
        dim_users = self._create_dim_users(tickets_df)
        dim_agents = self._create_dim_agents(tickets_df)
        dim_products = self._create_dim_products(tickets_df)
        dim_categories = self._create_dim_categories(tickets_df)
        dim_status = self._create_dim_status(tickets_df)
        dim_priorities = self._create_dim_priorities(tickets_df)
        dim_tags = self._create_dim_tags(tags_data)
        dim_channel = self._create_dim_channel(tickets_df)

        fact_tickets = self._create_fact_tickets(tickets_df, tags_data, dim_dates)

        return {
            "Dim_Dates": dim_dates,
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

    def _create_dim_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        date_cols = ["first_response_at", "created_at", "closed_at"]
        dim_prep = pd.DataFrame()
        for col in date_cols:
            if col in df.columns:
                temp = df[[col]].copy()
                temp = temp.rename(columns={col: "datetime"})
                dim_prep = pd.concat([dim_prep, temp], ignore_index=True)
        dim_prep = (
            dim_prep.dropna(subset=["datetime"])
            .drop_duplicates()
            .reset_index(drop=True)
        )

        dim_prep["datetime"] = pd.to_datetime(dim_prep["datetime"], errors="coerce")
        dim_prep = dim_prep.dropna(subset=["datetime"]).reset_index(drop=True)

        dim_prep["Year"] = dim_prep["datetime"].dt.year
        dim_prep["Month"] = dim_prep["datetime"].dt.month
        dim_prep["Day"] = dim_prep["datetime"].dt.day
        dim_prep["Hour"] = dim_prep["datetime"].dt.hour
        dim_prep["Minute"] = dim_prep["datetime"].dt.minute

        dim = (
            dim_prep[["Year", "Month", "Day", "Hour", "Minute"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )
        dim.index = dim.index + 1
        return dim

    def _create_dim_channel(self, df: pd.DataFrame) -> pd.DataFrame:
        dim = df[["channel"]].copy().dropna().drop_duplicates()
        dim.rename(columns={"channel": "ChannelName"}, inplace=True)
        return dim.reset_index(drop=True)

    def _create_dim_companies(self, df: pd.DataFrame) -> pd.DataFrame:
        dim = df[
            ["company_id", "company_name", "company_segment", "company_cnpj"]
        ].copy()
        dim.rename(
            columns={
                "company_id": "CompanyId_BK",
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
                "user_id": "UserId_BK",
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
                "agent_id": "AgentId_BK",
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
                "product_id": "ProductId_BK",
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
                "category_id": "CategoryId_BK",
                "category_name": "CategoryName",
                "subcategory_name": "SubcategoryName",
            },
            inplace=True,
        )
        return dim.drop_duplicates(subset=["CategoryId_BK"]).reset_index(drop=True)

    def _create_dim_status(self, df: pd.DataFrame) -> pd.DataFrame:
        dim = df[["current_status"]].copy()
        dim.rename(columns={"current_status": "StatusId_BK"}, inplace=True)
        dim["Name"] = "Status_" + dim["StatusId_BK"].astype(str)
        return dim.drop_duplicates(subset=["StatusId_BK"]).reset_index(drop=True)

    def _create_dim_priorities(self, df: pd.DataFrame) -> pd.DataFrame:
        dim = df[["priorityId", "name"]].copy()
        dim.rename(columns={"priorityId": "PriorityId_BK"}, inplace=True)
        dim["name"] = dim["name"].astype(str)
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
            df = pd.DataFrame(columns=["TagId_BK", "Name"])
        else:
            df = pd.DataFrame(list(all_tags.items()), columns=["TagId_BK", "Name"])

        na_tag = pd.DataFrame([{"TagId_BK": "-1", "Name": "N/A"}])

        df = pd.concat([df, na_tag], ignore_index=True).drop_duplicates(
            subset=["TagId_BK"]
        )

        return df

    def _create_fact_tickets(
        self,
        df: pd.DataFrame,
        tags_data: Dict[str, List[Dict]],
        dim_dates: pd.DataFrame = None,
    ) -> pd.DataFrame:
        logger.info("Starting creation of fact table...")

        ticket_to_tags_map = {
            ticket_id: [tag.get("tag_id") for tag in tags]
            for ticket_id, tags in tags_data.items()
        }

        df["TagId_BK_List"] = df["ticket_id"].astype(str).map(ticket_to_tags_map)
        df["TagId_BK_List"] = df["TagId_BK_List"].apply(
            lambda x: x if (isinstance(x, list) and x) else ["-1"]
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
                "priorityId",
                "current_status",
                "product_id",
                "channel",
                "TagId_BK",
                "created_at",
                "closed_at",
                "first_response_at",
            ]
        ].copy()

        fact.rename(
            columns={
                "ticket_id": "TicketKey",
                "user_id": "UserId_BK",
                "agent_id": "AgentId_BK",
                "company_id": "CompanyId_BK",
                "category_id": "CategoryId_BK",
                "priorityId": "PriorityId_BK",
                "current_status": "StatusId_BK",
                "product_id": "ProductId_BK",
                "channel": "Channel_BK",
            },
            inplace=True,
        )

        logger.info("Optimizing date key lookups...")

        dim_dates_with_key = dim_dates.reset_index().rename(
            columns={"index": "DateKey"}
        )

        for date_col_name, new_key_name in [
            ("created_at", "EntryDateKey"),
            ("closed_at", "ClosedDateKey"),
            ("first_response_at", "FirstResponseDateKey"),
        ]:
            temp_dates = pd.to_datetime(fact[date_col_name], errors="coerce")
            fact_date_parts = pd.DataFrame(
                {
                    "Year": temp_dates.dt.year,
                    "Month": temp_dates.dt.month,
                    "Day": temp_dates.dt.day,
                    "Hour": temp_dates.dt.hour,
                    "Minute": temp_dates.dt.minute,
                }
            ).reset_index()

            merged_keys = pd.merge(
                fact_date_parts,
                dim_dates_with_key,
                on=["Year", "Month", "Day", "Hour", "Minute"],
                how="left",
            )

            fact[new_key_name] = merged_keys.sort_values("index")["DateKey"]

        fact["QtTickets"] = 1
        fact = fact.drop(columns=["created_at", "closed_at", "first_response_at"])

        logger.info("Fact table creation completed.")
        return fact.reset_index(drop=True)


aspectlib.weave(TransformDwService, log_execution)
