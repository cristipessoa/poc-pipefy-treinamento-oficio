from app.services.pipefy import Pipefy


class RegraNegocioException(Exception):
    pass


def run(request):
    print("request:", request)

    try:

        # TODO Exemplo de validação entrada pipefy
        """
        # Validações Entrada Pipefy        
        card_action = request['data']['action']

        # Validacao Card Create
        if (card_action != 'card.create'):
            raise RegraNegocioException("Webhook " + card_action + " não mapeado")

        #Validacao Card Field Update
        if (card_action != 'card.field_update'):
            raise RegraNegocioException("Webhook " + card_action + " não mapeado")

        field_id = request['data']['field']['id']
        if not (field_id == 'tipo'):
            raise RegraNegocioException("Campo não mapeado")

        # Validacao Card Move
        if (card_action != 'card.move'):
            raise RegraNegocioException("Webhook " + card_action + " não mapeado")

        phase_id_from = request['data']['from']['id']
        phase_id_to = request['data']['to']['id']

        if not (phase_id_to == 7840446 and phase_id_from == 7910381):
            raise RegraNegocioException("Fase não mapeado")
        """

        # TODO Exemplo de chamada de serviço externo
        #pipefy = Pipefy()
        #print(pipefy.phase(7370321, search={'title': '2200100080203574801'}))

        # print(pipefy.phase(8360160,search={'title':'62084415'}))

        # Consulta de Card e Recuperação de valor
        # response = pipefy.card(64386929)
        # print(pipefy.getFieldValueById(response,'tipo_de_solicita_o'))

        # Consulta de Table Records e Recuperação de valor
        # response = pipefy.tableRecords('xO8rvBdP',search={'title':'NÃO TRABALHAR - RCV'})
        # print(pipefy.getFieldValueById(response['edges'][0]['node'], 'para'))

        # Atualizar Campo
        # pipefy.updateCardField(999999999, 'id_do_campo', 999)
        # TODO: Adicionar Regras de Negocio

    except RegraNegocioException as e:
        print(e)
