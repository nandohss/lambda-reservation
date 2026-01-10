import json
from reservations import (
    add_reservation,
    check_availability,
    cancel_reservation,
    listar_reservas_usuario,
    listar_reservas_cohoster,
    listar_reservas_cohoster,
    update_reservation_status
)
import base64

def get_normalized_body(event):
    body = event.get('body')
    if not body:
        return {}
        
    if event.get('isBase64Encoded', False):
        try:
            body = base64.b64decode(body).decode('utf-8')
        except Exception:
            print("❌ Falha ao decodificar Base64")
            return {}

    if isinstance(body, str):
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            print("❌ Falha ao fazer parse do JSON body")
            return {}
            
    return body if isinstance(body, dict) else {}


def lambda_handler(event, context):
    # Log do evento para depuração
    try:
        print(">>> Evento recebido:", json.dumps(event))
    except Exception:
        print(">>> Evento recebido (não serializável em JSON)")

    # Compatível com API Gateway HTTP API (v2.0)
    request_context = event.get('requestContext', {})
    http_info = request_context.get('http', {})
    http_method = http_info.get('method')
    raw_path = event.get('rawPath')

    if not http_method or not raw_path:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'Método ou caminho ausente.'})
        }

    print(f">>> Método: {http_method} | Caminho: {raw_path}")

    # Roteamento
    # POST /reservations -> criar reserva OU atualizar status (via flag action)
    if http_method == 'POST' and raw_path.endswith('/reservations'):
        body_data = get_normalized_body(event)
        print(f"DEBUG: POST body keys: {list(body_data.keys())}")
        
        if body_data.get('action') == 'update_status':
            return update_reservation_status(event, context)
            
        return add_reservation(event, context)

    # GET /reservations -> Roteamento Inteligente
    # Se tiver 'hours' e 'date', é check_availability
    # Senão, é listar_reservas_cohoster
    elif http_method == 'GET' and raw_path.endswith('/reservations'):
        qs = event.get('queryStringParameters') or {}
        if qs.get('hours') and qs.get('date'):
             return check_availability(event, context)
        return listar_reservas_cohoster(event, context)

    # GET /reservations/availability -> checar disponibilidade (caminho explícito)
    elif http_method == 'GET' and raw_path.endswith('/reservations/availability'):
        return check_availability(event, context)

    # DELETE /reservations -> cancelar reserva
    elif http_method == 'DELETE' and raw_path.endswith('/reservations'):
        return cancel_reservation(event, context)

    # PATCH /reservations/status -> atualizar status (aprovar/recusar)
    elif http_method == 'PATCH' and raw_path.endswith('/reservations/status'):
        return update_reservation_status(event, context)

    # GET /reservations/user -> listar reservas por usuário
    elif http_method == 'GET' and raw_path.endswith('/reservations/user'):
        return listar_reservas_usuario(event, context)

    # GET /reservations/cohoster -> (opcional) manter compatibilidade explícita
    elif http_method == 'GET' and raw_path.endswith('/reservations/cohoster'):
        return listar_reservas_cohoster(event, context)

    # Rota não suportada
    else:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': f'Rota não suportada: {http_method} {raw_path}'})
        }