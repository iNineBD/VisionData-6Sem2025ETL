from config.aop_logging import log_execution
import aspectlib
from typing import Dict, List, Optional, Any
import math


class ExtractElasticService:
    """Serviço responsável por extrair dados dos tickets do banco de dados"""

    def __init__(self, db_connection):
        """
        Args:
            db_connection: Instância da classe DBConnector
        """
        self.db = db_connection

    def _execute_in_chunks(self, base_query, id_list, chunk_size=2000):
        """
        Executa uma consulta SQL com uma cláusula IN, dividindo-a em lotes menores
        para evitar o limite de parâmetros do SQL Server.
        """
        if not id_list:
            return []

        all_results = []
        num_chunks = int(math.ceil(len(id_list) / float(chunk_size)))

        for i in range(num_chunks):
            start_index = i * chunk_size
            end_index = (i + 1) * chunk_size
            chunk_ids = id_list[start_index:end_index]

            if not chunk_ids:
                continue

            placeholders = ", ".join(["?"] * len(chunk_ids))
            query = f"{base_query} ({placeholders})"

            try:
                results_chunk = self.db.fetch_all(query, chunk_ids)
                if results_chunk:
                    all_results.extend(results_chunk)
            except Exception as e:
                # É uma boa prática logar o erro aqui
                print(f"Error executing chunk {i+1}/{num_chunks}: {e}")
                # Dependendo da necessidade, você pode querer parar a execução
                # ou apenas continuar para o próximo lote.
                # Aqui, vamos continuar, mas sem adicionar resultados deste lote.
                pass

        return all_results

    def get_tickets_base_data(
        self, ticket_ids: Optional[List[str]] = None, limit: Optional[int] = None
    ) -> List[Dict]:
        """
        Extrai dados principais dos tickets com relacionamentos básicos

        Args:
            ticket_ids: Lista de IDs específicos de tickets (opcional)
            limit: Limite de registros (opcional)

        Returns:
            Lista de dicionários com dados básicos dos tickets
        """
        select_fields = """
            t.TicketId as ticket_id,
            t.Title as title,
            t.Description as description,
            t.Channel as channel,
            t.Device as device,
            t.CurrentStatusId as current_status,
            t.SLAPlanId as sla_plan,
            t.PriorityId as priority,
            t.CreatedAt as created_at,
            t.FirstResponseAt as first_response_at,
            t.ClosedAt as closed_at,
            -- Company data
            c.CompanyId as company_id,
            c.Name as company_name,
            c.CNPJ as company_cnpj,
            c.Segmento as company_segment,
            -- User data (created by)
            u.UserId as user_id,
            u.FullName as user_full_name,
            u.Email as user_email,
            u.Phone as user_phone,
            u.CPF as user_cpf,
            u.IsVIP as user_is_vip,
            -- Agent data (assigned to)
            a.AgentId as agent_id,
            a.FullName as agent_full_name,
            a.Email as agent_email,
            a.DepartmentId as agent_department,
            -- Product data
            p.ProductId as product_id,
            p.Name as product_name,
            p.Code as product_code,
            p.Description as product_description,
            -- Category data
            cat.CategoryId as category_id,
            cat.Name as category_name,
            -- Subcategory data
            sub.SubcategoryId as subcategory_id,
            sub.Name as subcategory_name,
            -- SLA Plan data
            sla.Name as sla_plan_name,
            sla.FirstResponseMins as sla_first_response_mins,
            sla.ResolutionMins as sla_resolution_mins
        """

        if limit:
            base_query = f"SELECT TOP {limit} {select_fields} FROM Tickets t "
        else:
            base_query = f"SELECT {select_fields} FROM Tickets t "

        base_query += "LEFT JOIN Companies c ON t.CompanyId = c.CompanyId "
        base_query += "LEFT JOIN Users u ON t.CreatedByUserId = u.UserId "
        base_query += "LEFT JOIN Agents a ON t.AssignedAgentId = a.AgentId "
        base_query += "LEFT JOIN Products p ON t.ProductId = p.ProductId "
        base_query += "LEFT JOIN Categories cat ON t.CategoryId = cat.CategoryId "
        base_query += (
            "LEFT JOIN Subcategories sub ON t.SubcategoryId = sub.SubcategoryId "
        )
        base_query += "LEFT JOIN SLA_Plans sla ON t.SLAPlanId = sla.SLAPlanId "

        # Adiciona filtros
        conditions = []
        params = []

        if ticket_ids:
            placeholders = ",".join(["?"] * len(ticket_ids))  # SQL Server usa ?
            conditions.append(f"t.TicketId IN ({placeholders})")
            params.extend(ticket_ids)

        if conditions:
            base_query += " WHERE " + " AND ".join(conditions)
        base_query += " ORDER BY t.CreatedAt DESC"

        # Usa o método fetch_all da sua classe DBConnector
        results = self.db.fetch_all(base_query, params if params else None)

        if not results:
            return []

        # Converte pyodbc.Row para dicionários
        columns = (
            [column[0] for column in results[0].cursor_description] if results else []
        )
        return [dict(zip(columns, row)) for row in results]

    def get_attachments(self, ticket_ids: list[str]) -> dict[str, list[dict]]:
        """Extrai anexos dos tickets especificados."""
        if not ticket_ids:
            return {}

        base_query = """
        SELECT 
            AttachmentId as id,
            TicketId as ticket_id,
            FileName as filename,
            MimeType as mime_type,
            SizeBytes as size_bytes,
            StoragePath as storage_path,
            UploadedAt as uploaded_at
        FROM Attachments
        WHERE TicketId IN
        """

        results = self._execute_in_chunks(base_query, ticket_ids)

        if not results:
            return {}

        columns = [column[0] for column in results[0].cursor_description]
        attachments_data = [dict(zip(columns, row)) for row in results]

        attachments_by_ticket = {}
        for attachment in attachments_data:
            ticket_id = attachment.pop("ticket_id")
            if ticket_id not in attachments_by_ticket:
                attachments_by_ticket[ticket_id] = []
            attachments_by_ticket[ticket_id].append(attachment)

        return attachments_by_ticket

    def get_tags(self, ticket_ids: list[str]) -> dict[str, list[str]]:
        """Extrai tags dos tickets especificados."""
        if not ticket_ids:
            return {}

        base_query = """
        SELECT 
            tt.TicketId as ticket_id,
            t.Name as tag_name
        FROM TicketTags tt
        JOIN Tags t ON tt.TagId = t.TagId
        WHERE tt.TicketId IN
        """

        results = self._execute_in_chunks(base_query, ticket_ids)

        if not results:
            return {}

        columns = [column[0] for column in results[0].cursor_description]
        tags_data = [dict(zip(columns, row)) for row in results]

        tags_by_ticket = {}
        for tag in tags_data:
            ticket_id = tag["ticket_id"]
            if ticket_id not in tags_by_ticket:
                tags_by_ticket[ticket_id] = []
            tags_by_ticket[ticket_id].append(tag["tag_name"])

        return tags_by_ticket

    def get_status_history(self, ticket_ids: list[str]) -> dict[str, list[dict]]:
        """Extrai histórico de status dos tickets especificados."""
        if not ticket_ids:
            return {}

        base_query = """
        SELECT 
            tsh.TicketId as ticket_id,
            tsh.FromStatusId as from_status,
            tsh.ToStatusId as to_status,
            tsh.ChangedAt as changed_at,
            tsh.ChangedByAgentId as changed_by_agent_id,
            a.FullName as changed_by_agent_name
        FROM TicketStatusHistory tsh
        LEFT JOIN Agents a ON tsh.ChangedByAgentId = a.AgentId
        WHERE tsh.TicketId IN
        """

        results = self._execute_in_chunks(base_query, ticket_ids)

        if not results:
            return {}

        columns = [column[0] for column in results[0].cursor_description]
        history_data = [dict(zip(columns, row)) for row in results]

        history_by_ticket = {}
        for history in history_data:
            ticket_id = history.pop("ticket_id")
            if ticket_id not in history_by_ticket:
                history_by_ticket[ticket_id] = []
            history_by_ticket[ticket_id].append(history)

        return history_by_ticket

    def get_audit_logs(self, ticket_ids: list[str]) -> dict[str, list[dict]]:
        """Extrai logs de auditoria dos tickets especificados."""
        if not ticket_ids:
            return {}

        base_query = """
        SELECT 
            al.EntityId as ticket_id,
            al.AuditId as id,
            al.EntityType as entity_type,
            al.EntityId as entity_id,
            al.Operation as operation,
            al.PerformedBy as performed_by,
            al.PerformedAt as performed_at,
            al.DetailsJson as details
        FROM AuditLogs al
        WHERE al.EntityType = 'ticket' AND al.EntityId IN
        """

        # Usando a função _execute_in_chunks para buscar os dados
        results = self._execute_in_chunks(base_query, ticket_ids)

        if not results:
            return {}

        # Converte pyodbc.Row para dicionários
        columns = [column[0] for column in results[0].cursor_description]
        audit_data = [dict(zip(columns, row)) for row in results]

        # Agrupa por ticket_id
        audit_by_ticket = {}
        for audit in audit_data:
            ticket_id = audit.pop("ticket_id")
            if ticket_id not in audit_by_ticket:
                audit_by_ticket[ticket_id] = []
            audit_by_ticket[ticket_id].append(audit)

        return audit_by_ticket

    def extract_complete_tickets_data(
        self, ticket_ids: Optional[List[str]] = None, limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Extrai todos os dados necessários dos tickets

        Returns:
            Dicionário com:
            - tickets: dados principais dos tickets
            - attachments: anexos agrupados por ticket_id
            - tags: tags agrupadas por ticket_id
            - status_history: histórico agrupado por ticket_id
            - audit_logs: logs agrupados por ticket_id
        """
        # Extrai dados principais
        tickets_data = self.get_tickets_base_data(ticket_ids, limit)

        if not tickets_data:
            return {
                "tickets": [],
                "attachments": {},
                "tags": {},
                "status_history": {},
                "audit_logs": {},
            }

        # Extrai IDs para buscar dados relacionados
        extracted_ticket_ids = [str(ticket["ticket_id"]) for ticket in tickets_data]

        # Extrai dados relacionados
        attachments = self.get_attachments(extracted_ticket_ids)
        tags = self.get_tags(extracted_ticket_ids)
        status_history = self.get_status_history(extracted_ticket_ids)
        audit_logs = self.get_audit_logs(extracted_ticket_ids)

        return {
            "tickets": tickets_data,
            "attachments": attachments,
            "tags": tags,
            "status_history": status_history,
            "audit_logs": audit_logs,
        }


aspectlib.weave(ExtractElasticService, log_execution)
