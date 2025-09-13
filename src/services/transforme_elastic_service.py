from datetime import datetime
import json
import aspectlib
from typing import Dict, List, Optional
from config.aop_logging import log_execution

class TransformeElasticService:
    """Serviço responsável por transformar dados dos tickets para o formato Elasticsearch"""
    
    @staticmethod
    def _format_datetime(dt) -> Optional[str]:
        """Formata datetime para o padrão do Elasticsearch"""
        if dt is None:
            return None
        if isinstance(dt, str):
            return dt
        if hasattr(dt, 'strftime'):
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        return str(dt)
    
    @staticmethod
    def _calculate_sla_metrics(ticket: Dict) -> Dict:
        """Calcula métricas de SLA baseado nos dados do ticket"""
        metrics = {
            "first_response_time_minutes": None,
            "resolution_time_minutes": None,
            "first_response_sla_breached": False,
            "resolution_sla_breached": False
        }
        
        created_at = ticket.get('created_at')
        first_response_at = ticket.get('first_response_at')
        closed_at = ticket.get('closed_at')
        sla_first_response_mins = ticket.get('sla_first_response_mins')
        sla_resolution_mins = ticket.get('sla_resolution_mins')
        
        # Converte strings para datetime se necessário
        def parse_datetime(dt):
            if dt is None:
                return None
            if isinstance(dt, str):
                try:
                    return datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    return None
            return dt
        
        created_at = parse_datetime(created_at)
        first_response_at = parse_datetime(first_response_at)
        closed_at = parse_datetime(closed_at)
        
        # Calcula tempo de primeira resposta
        if created_at and first_response_at:
            first_response_time = (first_response_at - created_at).total_seconds() / 60
            metrics["first_response_time_minutes"] = int(first_response_time)
            
            if sla_first_response_mins and first_response_time > sla_first_response_mins:
                metrics["first_response_sla_breached"] = True
        
        # Calcula tempo de resolução
        if created_at and closed_at:
            resolution_time = (closed_at - created_at).total_seconds() / 60
            metrics["resolution_time_minutes"] = int(resolution_time)
            
            if sla_resolution_mins and resolution_time > sla_resolution_mins:
                metrics["resolution_sla_breached"] = True
        
        return metrics
    
    @staticmethod
    def _create_search_text(ticket: Dict) -> str:
        """Cria texto de busca combinando campos relevantes"""
        search_parts = []
        
        # Campos de texto relevantes para busca
        text_fields = [
            'title', 'description', 'company_name', 'user_full_name', 
            'agent_full_name', 'product_name', 'category_name', 'subcategory_name'
        ]
        
        for field in text_fields:
            value = ticket.get(field)
            if value and isinstance(value, str) and value.strip():
                search_parts.append(value.strip())
        
        return ' '.join(search_parts)
    
    @staticmethod
    def _transform_attachments(attachments: List[Dict]) -> List[Dict]:
        """Transforma anexos para o formato Elasticsearch"""
        transformed = []
        for attachment in attachments:
            transformed_attachment = {
                "id": attachment.get('id'),
                "filename": attachment.get('filename'),
                "mime_type": attachment.get('mime_type'),
                "size_bytes": attachment.get('size_bytes'),
                "storage_path": attachment.get('storage_path'),
                "uploaded_at": TransformeElasticService._format_datetime(attachment.get('uploaded_at'))
            }
            transformed.append(transformed_attachment)
        return transformed
    
    @staticmethod
    def _transform_status_history(history: List[Dict]) -> List[Dict]:
        """Transforma histórico de status para o formato Elasticsearch"""
        transformed = []
        for status in history:
            transformed_status = {
                "from_status": status.get('from_status'),
                "to_status": status.get('to_status'),
                "changed_at": TransformeElasticService._format_datetime(status.get('changed_at')),
                "changed_by_agent_id": status.get('changed_by_agent_id'),
                "changed_by_agent_name": status.get('changed_by_agent_name')
            }
            transformed.append(transformed_status)
        return transformed
    
    @staticmethod
    def _transform_audit_logs(logs: List[Dict]) -> List[Dict]:
        """Transforma logs de auditoria para o formato Elasticsearch"""
        transformed = []
        for log in logs:
            # Processa o campo details (JSON)
            details = log.get('details')
            if isinstance(details, str):
                try:
                    details = json.loads(details)
                except json.JSONDecodeError:
                    details = {}
            elif details is None:
                details = {}
            
            transformed_log = {
                "entity_type": log.get('entity_type'),
                "entity_id": log.get('entity_id'),
                "operation": log.get('operation'),
                "performed_by": log.get('performed_by'),
                "performed_at": TransformeElasticService._format_datetime(log.get('performed_at')),
                "details": details
            }
            transformed.append(transformed_log)
        return transformed
    
    @staticmethod
    def _transform_single_ticket(ticket: Dict, related_data: Dict) -> Dict:
        """
        Transforma um único ticket para o formato Elasticsearch
        
        Args:
            ticket: Dados do ticket
            related_data: Dicionário com dados relacionados (attachments, tags, etc.)
        
        Returns:
            Documento formatado para Elasticsearch
        """
        ticket_id = str(ticket['ticket_id'])
        
        # Extrai dados relacionados
        attachments = related_data.get('attachments', {}).get(ticket_id, [])
        tags = related_data.get('tags', {}).get(ticket_id, [])
        status_history = related_data.get('status_history', {}).get(ticket_id, [])
        audit_logs = related_data.get('audit_logs', {}).get(ticket_id, [])
        
        # Documento base para o Elasticsearch
        es_document = {
            "ticket_id": ticket_id,
            "title": ticket.get('title'),
            "description": ticket.get('description'),
            "channel": ticket.get('channel'),
            "device": ticket.get('device'),
            "current_status": ticket.get('current_status'),
            "sla_plan": ticket.get('sla_plan'),
            "priority": ticket.get('priority'),
            
            # Datas
            "dates": {
                "created_at": TransformeElasticService._format_datetime(ticket.get('created_at')),
                "first_response_at": TransformeElasticService._format_datetime(ticket.get('first_response_at')),
                "closed_at": TransformeElasticService._format_datetime(ticket.get('closed_at'))
            },
            
            # Empresa
            "company": {
                "id": ticket.get('company_id'),
                "name": ticket.get('company_name'),
                "cnpj": ticket.get('company_cnpj'),
                "segment": ticket.get('company_segment')
            },
            
            # Usuário que criou o ticket
            "created_by_user": {
                "id": ticket.get('user_id'),
                "full_name": ticket.get('user_full_name'),
                "email": ticket.get('user_email'),
                "phone": ticket.get('user_phone'),
                "cpf": ticket.get('user_cpf'),
                "is_vip": bool(ticket.get('user_is_vip', False))
            },
            
            # Agente atribuído
            "assigned_agent": {
                "id": ticket.get('agent_id'),
                "full_name": ticket.get('agent_full_name'),
                "email": ticket.get('agent_email'),
                "department": ticket.get('agent_department')
            },
            
            # Produto
            "product": {
                "id": ticket.get('product_id'),
                "name": ticket.get('product_name'),
                "code": ticket.get('product_code'),
                "description": ticket.get('product_description')
            },
            
            # Categoria
            "category": {
                "id": ticket.get('category_id'),
                "name": ticket.get('category_name')
            },
            
            # Subcategoria
            "subcategory": {
                "id": ticket.get('subcategory_id'),
                "name": ticket.get('subcategory_name')
            },
            
            # Dados relacionados transformados
            "attachments": TransformeElasticService._transform_attachments(attachments),
            "tags": tags,
            "status_history": TransformeElasticService._transform_status_history(status_history),
            "audit_logs": TransformeElasticService._transform_audit_logs(audit_logs),
            
            # Métricas de SLA
            "sla_metrics": TransformeElasticService._calculate_sla_metrics(ticket),
            
            # Texto de busca
            "search_text": TransformeElasticService._create_search_text(ticket)
        }
        
        return es_document
    
    @staticmethod
    def transform_tickets_batch(extracted_data: Dict) -> List[Dict]:
        """
        Transforma lote de tickets extraídos para o formato Elasticsearch
        
        Args:
            extracted_data: Dados extraídos pelo TicketExtractService
            
        Returns:
            Lista de documentos formatados para Elasticsearch
        """
        tickets = extracted_data.get('tickets', [])
        
        if not tickets:
            return []
        
        # Prepara dados relacionados
        related_data = {
            'attachments': extracted_data.get('attachments', {}),
            'tags': extracted_data.get('tags', {}),
            'status_history': extracted_data.get('status_history', {}),
            'audit_logs': extracted_data.get('audit_logs', {})
        }
        
        # Transforma cada ticket
        transformed_documents = []
        for ticket in tickets:
            es_document = TransformeElasticService._transform_single_ticket(ticket, related_data)
            transformed_documents.append(es_document)
        
        return transformed_documents

aspectlib.weave(TransformeElasticService,log_execution)