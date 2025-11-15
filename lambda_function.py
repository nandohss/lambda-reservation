import json
from reservations import (
    add_reservation,
    check_availability,
    cancel_reservation,
    listar_reservas_usuario,
    listar_reservas_cohoster
)

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
    # POST /reservations -> criar reserva
    if http_method == 'POST' and raw_path.endswith('/reservations'):
        return add_reservation(event, context)

    # GET /reservations -> listar reservas do hoster (aceita hosterId ou coHosterId)
    elif http_method == 'GET' and raw_path.endswith('/reservations'):
        return listar_reservas_cohoster(event, context)

    # GET /reservations/availability -> checar disponibilidade
    elif http_method == 'GET' and raw_path.endswith('/reservations/availability'):
        return check_availability(event, context)

    # DELETE /reservations -> cancelar reserva
    elif http_method == 'DELETE' and raw_path.endswith('/reservations'):
        return cancel_reservation(event, context)

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