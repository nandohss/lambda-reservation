import json
import decimal  # para usar decimal.Decimal no decimal_default
from datetime import datetime, timedelta
import boto3
from boto3.dynamodb.conditions import Attr, Key

dynamodb = boto3.resource('dynamodb', region_name='sa-east-1')
reservations_table = dynamodb.Table('reservation')
coworking_table = dynamodb.Table('coworking-spaces')
users_table = dynamodb.Table('users')


def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError(f'Type {type(obj)} not serializable')


def safe_int(value, default=0):
    try:
        if value is None:
            return default
        # Handle Decimal
        if isinstance(value, decimal.Decimal):
            return int(value)
        # Handle String
        return int(value)
    except (ValueError, TypeError):
        return default


def safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        if isinstance(value, decimal.Decimal):
            return float(value)
        return float(value)
    except (ValueError, TypeError):
        return default


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
        print(f"DEBUG: add_reservation BODY: {body}")

        space_id = body['spaceId_reservation']
        user_id = body['userId']
        date_reservation = body['date_reservation']
        hours = body['hours_reservation']
        status = body.get('status', 'PENDING')
        created_at = (datetime.utcnow() - timedelta(hours=3)).isoformat(timespec='seconds') + '-03:00'

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
                datetime_reservation = f"{date_reservation}T{int(hour):02d}:00:00-03:00"

                # Tentativa de inserção condicional
                reservations_table.put_item(
                    Item={
                        'spaceId_reservation': space_id,
                        'datetime_reservation': datetime_reservation,
                        'userId': user_id,
                        'status': status,
                        'date_reservation': date_reservation,
                        'hour_reservation': str(hour),
                        'created_at': created_at,
                        'updated_at': created_at
                    },
                    ConditionExpression='attribute_not_exists(spaceId_reservation) AND attribute_not_exists(datetime_reservation)'
                )

                print(f"✅ Reserva adicionada com sucesso: {datetime_reservation}")

            except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
                print(f"⚠️ Horário já reservado (conflito de escrita): Space={space_id} DateTime={datetime_reservation}")
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
        datetime_str = f"{date}T{int(hour):02d}:00:00-03:00"

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
            'body': json.dumps({'message': 'Parâmetros obrigatórios ausentes'})
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


def batch_get_users(user_ids):
    if not user_ids:
        return {}
    
    user_map = {}
    id_list = list(user_ids)
    
    # Chunking 100 items (DynamoDB limit)
    for i in range(0, len(id_list), 100):
        chunk = id_list[i:i+100]
        keys = [{'userId': uid} for uid in chunk]
        
        try:
            response = dynamodb.batch_get_item(
                RequestItems={
                    users_table.name: {
                        'Keys': keys
                    }
                }
            )
            
            for item in response.get('Responses', {}).get(users_table.name, []):
                user_map[item['userId']] = item
                
        except Exception as e:
            print(f"⚠️ Erro no batch_get_users chunk: {e}")
            
    return user_map


def listar_reservas_cohoster(event, context):
    params = event.get('queryStringParameters') or {}
    hoster_id = params.get('hosterId') or params.get('coHosterId')

    if not hoster_id:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'Parâmetro hosterId (ou coHosterId) é obrigatório'})
        }

    try:
        print(f"DEBUG: Iniciando busca otimizada para hoster_id={hoster_id}")
        
        # 1) Buscar todos os espaços
        spaces_resp = coworking_table.scan(
            FilterExpression=Attr("hoster").eq(hoster_id)
        )
        spaces = spaces_resp.get('Items', [])
        print(f"DEBUG: Encontrados {len(spaces)} espaços.")

        status_filter = params.get('status') 
        
        all_res_metrics = []
        unique_user_ids = set()

        # 2) Coletar reservas de todos os espaços (usando Query)
        for space in spaces:
            try:
                sid = space['spaceId']
                
                key_expr = Key("spaceId_reservation").eq(sid)
                query_args = {'KeyConditionExpression': key_expr}
                
                if status_filter:
                    query_args['FilterExpression'] = Attr("status").eq(status_filter)

                res_resp = reservations_table.query(**query_args)
                items = res_resp.get('Items', [])
                
                for r in items:
                    all_res_metrics.append((space, r))
                    if r.get('userId'):
                        unique_user_ids.add(r.get('userId'))
                        
            except Exception as e_space:
                print(f"❌ Erro ao buscar reservas do espaço {space.get('spaceId')}: {e_space}")
                continue

        print(f"DEBUG: Buscando detalhes de {len(unique_user_ids)} usuários.")
        user_map = batch_get_users(unique_user_ids)

        normalized = []
        for space, reserva in all_res_metrics:
            try:
                user_id = reserva.get('userId')
                user_item = user_map.get(user_id, {})
                
                space_id_res = reserva.get('spaceId_reservation')
                dt_res = reserva.get('datetime_reservation')
                status = reserva.get('status')
                
                # --- Lógica de Auto-Recusa
                if status == 'PENDING' and dt_res:
                    try:
                        now_iso = datetime.utcnow().isoformat() + 'Z'
                        if dt_res < now_iso:
                            updated_at = (datetime.utcnow() - timedelta(hours=3)).isoformat(timespec='seconds') + '-03:00'
                            reservations_table.update_item(
                                Key={'spaceId_reservation': space_id_res, 'datetime_reservation': dt_res},
                                UpdateExpression="set #st = :val, #ua = :ua_val",
                                ExpressionAttributeNames={'#st': 'status', '#ua': 'updated_at'},
                                ExpressionAttributeValues={':val': 'REFUSED', ':ua_val': updated_at}
                            )
                            status = 'REFUSED'
                    except Exception:
                        pass

                # --- Cálculo de Data Final ---
                try:
                    s_dt = dt_res.replace('Z', '+00:00')
                    dt_obj = datetime.fromisoformat(s_dt)
                    end_dt = (dt_obj + timedelta(hours=1)).isoformat(timespec='seconds')
                except Exception:
                    end_dt = dt_res

                price_hour = safe_float(space.get('precoHora'), 0.0)
                is_full_day = space.get('diaInteiro', False)
                price_day = safe_float(space.get('precoDia'), 0.0)

                item = {
                    "id": f"{space_id_res}|{dt_res}",
                    "spaceId": space_id_res,
                    "userId": user_id,
                    "hosterId": hoster_id,
                    "startDate": dt_res,
                    "endDate": end_dt,
                    "totalValue": price_hour,
                    "hourlyRate": price_hour,
                    "dailyRate": price_day,
                    "isFullDay": is_full_day,
                    "createdAt": reserva.get('created_at'),
                    "dateReservation": reserva.get('date_reservation'),
                    "status": status,
                    "spaceName": space.get('name', '—'),
                    "capacity": safe_int(space.get('capacity')),
                    "userName": user_item.get('name', '—'),
                    "userEmail": user_item.get('email', '—')
                }
                normalized.append(item)
            except Exception as e_item:
                print(f"❌ Erro ao normalizar item {reserva.get('datetime_reservation')}: {e_item}")
                continue

        print(f"DEBUG: Retornando {len(normalized)} itens.")
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


def update_reservation_status(event, context):
    try:
        body = json.loads(event['body'])
    except Exception:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'JSON inválido no corpo da requisição'})
        }

    space_id_reservation = body.get('spaceId')
    datetime_reservation = body.get('datetime')
    new_status = body.get('status')

    if not space_id_reservation or not datetime_reservation or not new_status:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'Parâmetros obrigatórios ausentes: spaceId, datetime, status'})
        }

    valid_statuses = ['PENDING', 'CONFIRMED', 'REFUSED', 'CANCELED']
    if new_status not in valid_statuses:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': f'Status inválido. Permitidos: {valid_statuses}'})
        }

    try:
        updated_at = (datetime.utcnow() - timedelta(hours=3)).isoformat(timespec='seconds') + '-03:00'
        
        response = reservations_table.update_item(
            Key={
                'spaceId_reservation': space_id_reservation,
                'datetime_reservation': datetime_reservation
            },
            UpdateExpression="set #st = :val, #ua = :ua_val",
            ExpressionAttributeNames={'#st': 'status', '#ua': 'updated_at'},
            ExpressionAttributeValues={':val': new_status, ':ua_val': updated_at},
            ReturnValues="UPDATED_NEW"
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Status atualizado para {new_status}',
                'updatedAttributes': response.get('Attributes')
            }, default=decimal_default)
        }

    except Exception as e:
        print(f"❌ Erro ao atualizar status da reserva: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': f'Erro ao atualizar status: {str(e)}'})
        }