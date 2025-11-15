import json
import decimal  # para usar decimal.Decimal no decimal_default
from datetime import datetime
import boto3
from boto3.dynamodb.conditions import Attr

dynamodb = boto3.resource('dynamodb', region_name='sa-east-1')
reservations_table = dynamodb.Table('reservation')
coworking_table = dynamodb.Table('coworking-spaces')
users_table = dynamodb.Table('users')


def decimal_default(obj):
    # Corrigido: usa o módulo 'decimal' importado acima
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError(f'Type {type(obj)} not serializable')


def user_exists(user_id):
    response = users_table.get_item(Key={'userId': user_id})
    print("Response from user exists check:", response)
    return 'Item' in response


def space_exists_and_available(space_id):
    try:
        response = coworking_table.get_item(Key={'spaceId': space_id})
        print("Coworking space response:", response)
        item = response.get('Item')
        return item is not None and item.get('availability', False)
    except Exception as e:
        print("Erro ao verificar disponibilidade do espaço:", str(e))
        return False


def add_reservation(event, context):
    try:
        body = json.loads(event['body'])

        space_id = body['spaceId_reservation']
        user_id = body['userId']
        date_reservation = body['date_reservation']
        hours = body['hours_reservation']
        status = body.get('status', 'reserved')
        created_at = datetime.utcnow().isoformat() + 'Z'

        if not space_exists_and_available(space_id):
            print(f"Espaço não encontrado ou não disponível: {space_id}")
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Espaço não disponível'})
            }

        if not user_exists(user_id):
            print(f"Usuário não encontrado: {user_id}")
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Usuário não encontrado'})
            }

        for hour in hours:
            try:
                datetime_reservation = f"{date_reservation}T{int(hour):02d}:00:00Z"

                # Tentativa de inserção condicional
                reservations_table.put_item(
                    Item={
                        'spaceId_reservation': space_id,
                        'datetime_reservation': datetime_reservation,
                        'userId': user_id,
                        'status': status,
                        'date_reservation': date_reservation,
                        'hour_reservation': str(hour),
                        'created_at': created_at
                    },
                    ConditionExpression='attribute_not_exists(spaceId_reservation) AND attribute_not_exists(datetime_reservation)'
                )

                print(f"✅ Reserva adicionada com sucesso: {datetime_reservation}")

            except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
                print(f"⚠️ Horário já reservado (conflito de escrita): {datetime_reservation}")
                return {
                    'statusCode': 400,
                    'body': json.dumps({'message': f'Horário {hour}h já reservado'})
                }

            except Exception as e:
                print(f"❌ Erro ao processar horário {hour}: {str(e)}")
                return {
                    'statusCode': 500,
                    'body': json.dumps({'message': f'Erro ao salvar horário {hour}: {str(e)}'})
                }

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Reserva completa registrada com sucesso.',
                'spaceId_reservation': space_id,
                'date_reservation': date_reservation,
                'hours_reserved': hours
            })
        }

    except KeyError as ke:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': f'Campo obrigatório ausente: {str(ke)}'})
        }
    except json.JSONDecodeError:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'JSON inválido no corpo da requisição'})
        }
    except Exception as e:
        print(f"❌ Erro inesperado em add_reservation: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': f'Erro interno: {str(e)}'})
        }


def check_availability(event, context):
    params = event.get('queryStringParameters', {}) or {}

    space_id = params.get('spaceId')
    date = params.get('date')
    # 'hours' vem como JSON string (ex.: \"[9,10,11]\")
    try:
        hours = json.loads(params.get('hours', '[]'))
    except json.JSONDecodeError:
        hours = []

    if not space_id or not date or not hours:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'Parâmetros obrigatórios ausentes'})
        }

    conflicts = []

    for hour in hours:
        datetime_str = f"{date}T{int(hour):02d}:00:00Z"

        response = reservations_table.get_item(
            Key={
                'spaceId_reservation': space_id,
                'datetime_reservation': datetime_str
            }
        )

        if 'Item' in response:
            conflicts.append(hour)

    return {
        'statusCode': 200,
        'body': json.dumps({
            'available': len(conflicts) == 0,
            'conflicts': conflicts
        })
    }


def cancel_reservation(event, context):
    try:
        body = json.loads(event['body'])
    except Exception:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'JSON inválido no corpo da requisição'})
        }

    space_id_reservation = body.get('spaceId')
    datetime_reservation = body.get('datetime')
    user_id = body.get('userId')

    if not space_id_reservation or not datetime_reservation or not user_id:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'Parâmetros obrigatórios ausentes'})  # mensagem consistente
        }

    response = reservations_table.get_item(
        Key={
            'spaceId_reservation': space_id_reservation,
            'datetime_reservation': datetime_reservation
        }
    )

    if 'Item' not in response or response['Item'].get('userId') != user_id:
        return {
            'statusCode': 400,
            'body': json.dumps({
                'message': 'Reserva não encontrada ou não pertence ao usuário'
            })
        }

    reservations_table.delete_item(
        Key={
            'spaceId_reservation': space_id_reservation,
            'datetime_reservation': datetime_reservation
        }
    )

    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Reserva cancelada com sucesso.',
            'spaceId_reservation': space_id_reservation,
            'datetime_reservation': datetime_reservation
        })
    }


def listar_reservas_usuario(event, context):
    params = event.get('queryStringParameters', {}) or {}
    user_id = params.get('userId')

    if not user_id:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'Parâmetro userId é obrigatório'})
        }

    try:
        response = reservations_table.scan(
            FilterExpression=Attr("userId").eq(user_id)
        )
        reservas = response.get('Items', [])

        return {
            'statusCode': 200,
            'body': json.dumps(reservas, default=decimal_default)
        }

    except Exception as e:
        print("❌ Erro em listar_reservas_usuario:", str(e))
        return {
            'statusCode': 500,
            'body': json.dumps({'message': f'Erro interno: {str(e)}'})
        }


def listar_reservas_cohoster(event, context):
    # Aceita hosterId (preferencial) e coHosterId por compatibilidade
    params = event.get('queryStringParameters') or {}
    hoster_id = params.get('hosterId') or params.get('coHosterId')

    if not hoster_id:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'Parâmetro hosterId (ou coHosterId) é obrigatório'})
        }

    try:
        # 1) Buscar todos os espaços cujo campo 'hoster' == hoster_id
        spaces_resp = coworking_table.scan(
            FilterExpression=Attr("hoster").eq(hoster_id)
        )
        spaces = spaces_resp.get('Items', [])
        print(f"Hoster {hoster_id} tem {len(spaces)} espacos.")

        # (Opcional) Filtro por status, se vier na query
        status_filter = params.get('status')  # ex.: 'CONFIRMED', 'PENDING', 'CANCELED', 'REFUSED', 'reserved'

        # 2) Para cada espaço, buscar reservas na tabela 'reservation'
        normalized = []
        for space in spaces:
            sid = space['spaceId']
            space_name = space.get('name', '—')

            filter_expr = Attr("spaceId_reservation").eq(sid)
            if status_filter:
                filter_expr = filter_expr & Attr("status").eq(status_filter)

            res_resp = reservations_table.scan(
                FilterExpression=filter_expr
            )

            for reserva in res_resp.get('Items', []):
                # Enriquecimento de usuário
                user_id = reserva.get('userId')
                user_resp = users_table.get_item(Key={'userId': user_id})
                user_item = user_resp.get('Item', {})

                # Campos originais
                space_id_res = reserva.get('spaceId_reservation')
                dt_res = reserva.get('datetime_reservation')
                status = reserva.get('status')

                # Mapeia para o DTO do app
                item = {
                    "id": f"{space_id_res}|{dt_res}",
                    "spaceId": space_id_res,
                    "userId": user_id,
                    "hosterId": hoster_id,
                    "startDate": dt_res,
                    "endDate": dt_res,  # sem fim real; repetir startDate
                    "status": status,
                    "spaceName": space_name,
                    "userName": user_item.get('name', '—'),
                    "userEmail": user_item.get('email', '—')
                }
                normalized.append(item)

        return {
            'statusCode': 200,
            'body': json.dumps(normalized, default=decimal_default)
        }

    except Exception as e:
        print("❌ Erro em listar_reservas_cohoster:", str(e))
        return {
            'statusCode': 500,
            'body': json.dumps({'message': f'Erro interno: {str(e)}'})
        }