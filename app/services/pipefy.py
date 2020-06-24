import requests
import json
import re
import os
import base64
from time import sleep
from app.resources.config import load_config_ini
from schema import Schema, And, Use, Optional, SchemaError
from sys import platform


class PipefyException(Exception):
    pass


class Pipefy(object):
    """ Integration class with Pipefy rest api. """

    def __init__(self):
        """ altered by: silvio.angelo@accenture.com  """
        self.config = load_config_ini()
        self.token = self.config['Pipefy']['api_key']
        self.endpoint = self.config['Pipefy']['api_url']
        self.qtdTentativasReconexao = int(self.config['Pipefy']['qtd_tentativas_reconexao'])
        self.timeoutConexao = int(self.config['Pipefy']['timeout_conexao'])
        self.headers = {'Content-Type': 'application/json', 'Authorization': self.token}

    def request(self, query, headers={}):
        """ altered by: silvio.angelo@accenture.com  """
        print("query:", query)

        for i in range(self.qtdTentativasReconexao):
            try:
                _headers = self.headers
                _headers.update(headers)
                response = requests.post(
                    self.endpoint,
                    json={"query": query},
                    headers=_headers
                )
                try:
                    status_code = response.status_code
                    response = json.loads(response.text)
                except ValueError:
                    print("response:", response.text)
                    raise PipefyException(response.text)

                if response.get('error'):
                    print("response:", response.get('error'))
                    raise PipefyException(response.get('error_description', response.get('error')))

                if response.get('errors'):
                    for error in response.get('errors'):
                        print("response:", response.get('message'))
                        raise PipefyException(error.get('message'))

                if status_code != requests.codes.ok:
                    print("response:", response.get('error'))
                    raise PipefyException(response.get('error_description', response.get('error')))

                if "DOCTYPE html" in response:
                    print("response:", response)
                    raise PipefyException("Error Http 429 - Too Many Requests")

                return response
            except Exception as e:
                print("Tentativa:", i, " Error Message:", e)
                print("Aguardando ", self.timeoutConexao, " seg para realizar nova tentativa")
                sleep(self.timeoutConexao)
                if i > self.qtdTentativasReconexao:
                    raise e

    def __prepare_json_dict(self, data_dict):
        data_response = json.dumps(data_dict)
        rex = re.compile(r'"(\S+)":')
        for field in rex.findall(data_response):
            data_response = data_response.replace('"%s"' % field, field)
        return data_response

    def __prepare_json_list(self, data_list):
        return '[ %s ]' % ', '.join([self.__prepare_json_dict(data) for data in data_list])

    def pipes(self, ids=[], response_fields=None, headers={}):
        """ List pipes: Get pipes by their identifiers. """

        response_fields = response_fields or 'id name phases { name cards (first: 5)' \
                                             ' { edges { node { id title } } } }'
        query = '{ pipes (ids: [%(ids)s]) { %(response_fields)s } }' % {
            'ids': ', '.join([json.dumps(id) for id in ids]),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('pipes', [])

    def pipe(self, id, response_fields=None, headers={}):
        """ Show pipe: Get a pipe by its identifier. """

        response_fields = response_fields or 'id name start_form_fields { label id }' \
                                             ' labels { name id } phases { name fields { label id }' \
                                             ' cards(first: 5) { edges { node { id, title } } } }'
        query = '{ pipe (id: %(id)s) { %(response_fields)s } }' % {
            'id': json.dumps(id),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('pipe', [])

    def clonePipes(self, organization_id, pipe_template_ids=[], response_fields=None, headers={}):
        """ Clone pipe: Mutation to clone a pipe, in case of success a query is returned. """

        response_fields = response_fields or 'pipes { id name }'
        query = 'mutation { clonePipes(input: { organization_id: %(organization_id)s' \
                ' pipe_template_ids: [%(pipe_template_ids)s] }) { %(response_fields)s } }' % {
                    'organization_id': json.dumps(organization_id),
                    'pipe_template_ids': ', '.join([json.dumps(id) for id in pipe_template_ids]),
                    'response_fields': response_fields,
                }
        return self.request(query, headers).get('data', {}).get('clonePipes', {}).get('pipe', [])

    def createPipe(self, organization_id, name, labels=[], members=[], phases=[],
                   start_form_fields=[], preferences={}, response_fields=None, headers={}):
        """ Create pipe: Mutation to create a pipe, in case of success a query is returned. """

        response_fields = response_fields or 'pipe { id name }'
        query = '''
            mutation {
              createPipe(
                input: {
                  organization_id: %(organization_id)s
                  name: %(name)s
                  labels: %(labels)s
                  members: %(members)s
                  phases: %(phases)s
                  start_form_fields: %(start_form_fields)s
                  preferences: %(preferences)s
                }
              ) { %(response_fields)s }
            }
        ''' % {
            'organization_id': json.dumps(organization_id),
            'name': json.dumps(name),
            'labels': self.__prepare_json_list(labels),
            'members': self.__prepare_json_list(members),
            'phases': self.__prepare_json_list(phases),
            'start_form_fields': self.__prepare_json_list(start_form_fields),
            'preferences': self.__prepare_json_dict(preferences),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('createPipe', {}).get('pipe', [])

    def updatePipe(self, id, icon=None, title_field_id=None, public=None, public_form=None,
                   only_assignees_can_edit_cards=None, anyone_can_create_card=None,
                   expiration_time_by_unit=None, expiration_unit=None, response_fields=None, headers={}):
        """ Update pipe: Mutation to update a pipe, in case of success a query is returned. """

        response_fields = response_fields or 'pipe { id name }'
        query = '''
            mutation {
              updatePipe(
                input: {
                  id: %(id)s
                  icon: %(icon)s
                  title_field_id: %(title_field_id)s
                  public: %(public)s
                  public_form: %(public_form)s
                  only_assignees_can_edit_cards: %(only_assignees_can_edit_cards)s
                  anyone_can_create_card: %(anyone_can_create_card)s
                  expiration_time_by_unit: %(expiration_time_by_unit)s
                  expiration_unit: %(expiration_unit)s
                }
              ) { %(response_fields)s }
            }
        ''' % {
            'id': json.dumps(id),
            'icon': json.dumps(icon),
            'title_field_id': json.dumps(title_field_id),
            'public': json.dumps(public),
            'public_form': json.dumps(public_form),
            'only_assignees_can_edit_cards': json.dumps(only_assignees_can_edit_cards),
            'anyone_can_create_card': json.dumps(anyone_can_create_card),
            'expiration_time_by_unit': json.dumps(expiration_time_by_unit),
            'expiration_unit': json.dumps(expiration_unit),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('updatePipe', {}).get('pipe', [])

    def deletePipe(self, id, response_fields=None, headers={}):
        """ Delete pipe: Mutation to delete a pipe, in case of success success: true is returned. """

        response_fields = response_fields or 'success'
        query = 'mutation { deletePipe(input: { id: %(id)s }) { %(response_fields)s }' % {
            'id': json.dumps(id),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('deletePipe', {})

    def phase(self, id, count=10, search={}, response_fields=None, response_card_fields=None, headers={}):
        """ altered by: silvio.angelo@accenture.com  """
        """ Show phase: Get a phase by its identifier and get cards by pipe identifier. """

        response_fields = response_fields or 'id name cards_count'

        response_card_fields = response_card_fields or 'edges { node { id title assignees { id name email }' \
                                                       ' comments { text } comments_count current_phase { id name } done due_date ' \
                                                       'fields { field{id type} name value array_value} labels { id name } phases_history { phase { id name } firstTimeIn lastTimeOut } url } }'

        query = '{ phase(id: %(phase_id)s ) { %(response_fields)s cards(first:%(count)s, search: %(search)s) { %(response_card_fields)s } } }' % {
            'phase_id': json.dumps(id),
            'count': json.dumps(count),
            'search': self.__prepare_json_dict(search),
            'response_fields': response_fields,
            'response_card_fields': response_card_fields
        }

        return self.request(query, headers).get('data', {}).get('phase');

    def createPhase(self, pipe_id, name, done, lateness_time, description, can_receive_card_directly_from_draft,
                    response_fields=None, headers={}):
        """ Create phase: Mutation to create a phase, in case of success a query is returned. """

        response_fields = response_fields or 'phase { id name }'
        query = '''
            mutation {
              createPhase(
                input: {
                  pipe_id: %(pipe_id)s
                  name: %(name)s
                  done: %(done)s
                  lateness_time: %(lateness_time)s
                  description: %(description)s
                  can_receive_card_directly_from_draft: %(can_receive_card_directly_from_draft)s
                }
              ) { %(response_fields)s }
            }
        ''' % {
            'pipe_id': json.dumps(pipe_id),
            'name': json.dumps(name),
            'done': json.dumps(done),
            'lateness_time': json.dumps(lateness_time),
            'description': json.dumps(description),
            'can_receive_card_directly_from_draft': json.dumps(can_receive_card_directly_from_draft),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('createPhase', {}).get('phase')

    def updatePhase(self, id, name, done, description, can_receive_card_directly_from_draft,lateness_time,
                    response_fields=None, headers={}):
        """ Update phase: Mutation to update a phase, in case of success a query is returned. """

        response_fields = response_fields or 'phase { id name }'
        query = '''
            mutation {
              updatePhase(
                input: {
                  id: %(id)s
                  name: %(name)s
                  done: %(done)s
                  description: %(description)s
                  can_receive_card_directly_from_draft: %(can_receive_card_directly_from_draft)s
                }
              ) { %(response_fields)s }
            }
        ''' % {
            'id': json.dumps(id),
            'name': json.dumps(name),
            'done': json.dumps(done),
            'lateness_time': json.dumps(lateness_time),
            'description': json.dumps(description),
            'can_receive_card_directly_from_draft': json.dumps(can_receive_card_directly_from_draft),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('updatePhase', {}).get('phase')

    def deletePhase(self, id, response_fields=None, headers={}):
        """ Delete phase: Mutation to delete a phase of a pipe, in case of success success: true is returned. """

        response_fields = response_fields or 'success'
        query = 'mutation { deletePhase(input: { id: %(id)s }) { %(response_fields)s }' % {
            'id': json.dumps(id),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('deletePhase', {})

    def createPhaseField(self, phase_id, type, label, options, description, required, editable,
                         response_fields=None, headers={}):
        """ Create phase field: Mutation to create a phase field, in case of success a query is returned. """

        response_fields = response_fields or 'phase_field { id label }'
        query = '''
            mutation {
              createPhaseField(
                input: {
                  phase_id: %(phase_id)s
                  type: %(type)s
                  label: %(label)s
                  options: %(options)s
                  description: %(description)s
                  required: %(required)s
                  editable: %(editable)s
                }
              ) { %(response_fields)s }
            }
        ''' % {
            'phase_id': json.dumps(phase_id),
            'type': json.dumps(type),
            'label': json.dumps(label),
            'options': self.__prepare_json_list(options),
            'description': json.dumps(description),
            'required': json.dumps(required),
            'editable': json.dumps(editable),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('createPhaseField', {}).get('phase_field')

    def updatePhaseField(self, id, label, options, required, editable, response_fields=None, headers={}):
        """ Update phase field: Mutation to update a phase field, in case of success a query is returned. """

        response_fields = response_fields or 'phase_field { id label }'
        query = '''
            mutation {
              updatePhaseField(
                input: {
                  id: %(id)s
                  label: %(label)s
                  options: %(options)s
                  required: %(required)s
                  editable: %(editable)s
                }
              ) { %(response_fields)s }
            }
        ''' % {
            'id': json.dumps(id),
            'label': json.dumps(label),
            'options': self.__prepare_json_list(options),
            'required': json.dumps(required),
            'editable': json.dumps(editable),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('updatePhaseField', {}).get('phase_field')

    def deletePhaseField(self, id, response_fields=None, headers={}):
        """ Delete phase field: Mutation to delete a phase field, in case of success success: true is returned. """

        response_fields = response_fields or 'success'
        query = 'mutation { deletePhaseField(input: { id: %(id)s }) { %(response_fields)s }' % {
            'id': json.dumps(id),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('deletePhaseField', {})

    def createLabel(self, pipe_id, name, color, response_fields=None, headers={}):
        """ Create label: Mutation to create a label, in case of success a query is returned. """

        response_fields = response_fields or 'label { id name }'
        query = '''
            mutation {
              createLabel(
                input: {
                  pipe_id: %(pipe_id)s
                  name: %(name)s
                  color: %(color)s
                }
              ) { %(response_fields)s }
            }
        ''' % {
            'pipe_id': json.dumps(pipe_id),
            'name': json.dumps(name),
            'color': json.dumps(color),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('createLabel', {}).get('label')

    def updateLabel(self, id, name, color, response_fields=None, headers={}):
        """ Update label: Mutation to update a label, in case of success a query is returned. """

        response_fields = response_fields or 'label { id name }'
        query = '''
            mutation {
              updateLabel(
                input: {
                  id: %(id)s
                  name: %(name)s
                  color: %(color)s
                }
              ) { %(response_fields)s }
            }
        ''' % {
            'id': json.dumps(id),
            'name': json.dumps(name),
            'color': json.dumps(color),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('updateLabel', {}).get('label')

    def deleteLabel(self, id, response_fields=None, headers={}):
        """ Delete label: Mutation to delete a label, in case of success success: true is returned. """

        response_fields = response_fields or 'success'
        query = 'mutation { deleteLabel(input: { id: %(id)s }) { %(response_fields)s }' % {
            'id': json.dumps(id),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('deleteLabel', {})

    def cards(self, pipe_id, count=10, search={}, response_fields=None, headers={}):
        """ List cards: Get cards by pipe identifier. """

        response_fields = response_fields or 'edges { node { id title assignees { id name email }' \
                                             ' comments { text } comments_count current_phase { id name } done due_date ' \
                                             'fields { field{id type} name value array_value} labels { id name } phases_history { phase { id name } firstTimeIn lastTimeOut } url } }'

        query = '{ cards(pipe_id: %(pipe_id)s, first: %(count)s, search: %(search)s) { %(response_fields)s } }' % {
            'pipe_id': json.dumps(pipe_id),
            'count': json.dumps(count),
            'search': self.__prepare_json_dict(search),
            'response_fields': response_fields,
        }

        return self.request(query, headers).get('data', {}).get('cards', [])

    def allCards(self, pipe_id, filter="", response_fields=None, headers={}):
        """ List cards: Get cards by pipe identifier. """

        response_fields = response_fields or 'edges { node { id title assignees { id }' \
                                             ' comments { text } comments_count current_phase { name } done due_date ' \
                                             'fields { name value } labels { name } phases_history { phase { name } firstTimeIn lastTimeOut } url } }'
        query = '{ allCards(pipeId: %(pipe_id)s, filter: %(filter)s) { %(response_fields)s } }' % {
            'pipe_id': json.dumps(pipe_id),
            'filter': filter,
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('allCards', [])

    def card(self, id, response_fields=None, headers={}):
        """ Show card: Get a card by its identifier. """

        response_fields = response_fields or 'title assignees { id name email } comments { id } comments_count' \
                                             ' current_phase { id name } pipe { id name } done due_date fields { field{id type} name value array_value } labels { id name } phases_history ' \
                                             '{ phase { id name } firstTimeIn lastTimeOut } url '
        query = '{ card(id: %(id)s) { %(response_fields)s } }' % {
            'id': json.dumps(id),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('card', [])

    def createCard(self, pipe_id, fields_attributes, parent_ids=[], response_fields=None, headers={}):
        """ Create card: Mutation to create a card, in case of success a query is returned. """

        response_fields = response_fields or 'card { id title }'
        query = '''
            mutation {
              createCard(
                input: {
                  pipe_id: %(pipe_id)s
                  fields_attributes: %(fields_attributes)s
                  parent_ids: [ %(parent_ids)s ]
                }
              ) { %(response_fields)s }
            }
        ''' % {
            'pipe_id': json.dumps(pipe_id),
            'fields_attributes': self.__prepare_json_dict(fields_attributes),
            'parent_ids': ', '.join([json.dumps(id) for id in parent_ids]),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('createCard', {}).get('card')

    def updateCard(self, id, title=None, due_date=None, assignee_ids=[], label_ids=[], response_fields=None,
                   headers={}):
        """ Update card: Mutation to update a card, in case of success a query is returned. """

        response_fields = response_fields or 'card { id title }'
        query = '''
            mutation {
              updateCard(
                input: {
                  id: %(id)s
                  title: %(title)s
                  due_date: %(due_date)s
                  assignee_ids: [ %(assignee_ids)s ]
                  label_ids: [ %(label_ids)s ]
                }
              ) { %(response_fields)s }
            }
        ''' % {
            'id': json.dumps(id),
            'title': json.dumps(title),
            'due_date': due_date.strftime('%Y-%m-%dT%H:%M:%S+00:00') if due_date else json.dumps(due_date),
            'assignee_ids': ', '.join([json.dumps(id) for id in assignee_ids]),
            'label_ids': ', '.join([json.dumps(id) for id in label_ids]),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('updateCard', {}).get('card')

    def deleteCard(self, id, response_fields=None, headers={}):
        """ Delete card: Mutation to delete a card, in case of success success: true is returned. """

        response_fields = response_fields or 'success'
        query = 'mutation { deleteCard(input: { id: %(id)s }) { %(response_fields)s }' % {
            'id': json.dumps(id),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('deleteCard', {})

    def moveCardToPhase(self, card_id, destination_phase_id, response_fields=None, headers={}):
        """ Move card to phase: Mutation to move a card to a phase, in case of success a query is returned. """

        response_fields = response_fields or 'card{ id current_phase { name } }'
        query = '''
            mutation {
              moveCardToPhase(
                input: {
                  card_id: %(card_id)s
                  destination_phase_id: %(destination_phase_id)s
                }
              ) { %(response_fields)s }
            }
        ''' % {
            'card_id': json.dumps(card_id),
            'destination_phase_id': json.dumps(destination_phase_id),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('moveCardToPhase', {}).get('card')

    def updateCardField(self, card_id, field_id, new_value, response_fields=None, headers={}):
        """ Update card field: Mutation to update a card field, in case of success a query is returned. """

        response_fields = response_fields or 'card{ id }'
        query = '''
            mutation {
              updateCardField(
                input: {
                  card_id: %(card_id)s
                  field_id: %(field_id)s
                  new_value: %(new_value)s
                }
              ) {success %(response_fields)s }
            }
        ''' % {
            'card_id': json.dumps(card_id),
            'field_id': json.dumps(field_id),
            'new_value': json.dumps(new_value),
            'response_fields': response_fields,
        }
        response = self.request(query, headers).get('data', {}).get('updateCardField', {})
        print('Resposta Pipefy Atualização Campo: ', response)
        return response

    def createComment(self, card_id, text, response_fields=None, headers={}):
        """ Create comment: Mutation to create a comment, in case of success a query is returned. """

        response_fields = response_fields or 'comment { id text }'
        query = '''
            mutation {
              createComment(
                input: {
                  card_id: %(card_id)s
                  text: %(text)s
                }
              ) { %(response_fields)s }
            }
        ''' % {
            'card_id': json.dumps(card_id),
            'text': json.dumps(text),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('createComment', {}).get('comment')

    def updateComment(self, id, text, response_fields=None, headers={}):
        """ Update comment: Mutation to update a comment, in case of success a query is returned. """

        response_fields = response_fields or 'comment { id text }'
        query = '''
            mutation {
              updateComment(
                input: {
                  id: %(id)s
                  text: %(text)s
                }
              ) { %(response_fields)s }
            }
        ''' % {
            'id': json.dumps(id),
            'text': json.dumps(text),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('updateComment', {}).get('comment')

    def deleteComment(self, id, response_fields=None, headers={}):
        """ Delete comment: Mutation to delete a comment, in case of success success: true is returned. """

        response_fields = response_fields or 'success'
        query = 'mutation { deleteComment(input: { id: %(id)s }) { %(response_fields)s }' % {
            'id': json.dumps(id),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('deleteComment', {})

    def setRole(self, pipe_id, member, response_fields=None, headers={}):
        """ Set role: Mutation to set a user's role, in case of success a query is returned. """

        response_fields = response_fields or 'member{ user{ id } role_name }'
        query = '''
            mutation {
              setRole(
                input: {
                  pipe_id: %(pipe_id)s
                  member: %(member)s
                }
              ) { %(response_fields)s }
            }
        ''' % {
            'pipe_id': json.dumps(pipe_id),
            'member': self.__prepare_json_dict(member),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('setRole', {}).get('comment')

    def pipe_relations(self, ids, response_fields=None, headers={}):
        """ Show pipe relations: Get pipe relations by their identifiers. """

        response_fields = response_fields or 'id name allChildrenMustBeDoneToMoveParent allChildrenMustBeDoneToFinishParent' \
                                             ' canCreateNewItems canConnectExistingItems canConnectMultipleItems childMustExistToMoveParent ' \
                                             'childMustExistToFinishParent'
        query = '{ pipe_relations(ids: [%(ids)s]) { %(response_fields)s } }' % {
            'ids': ', '.join([json.dumps(id) for id in ids]),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('pipe_relations')

    def createPipeRelation(self, parentId, childId, name, allChildrenMustBeDoneToFinishParent,
                           childMustExistToMoveParent,
                           childMustExistToFinishParent, allChildrenMustBeDoneToMoveParent, canCreateNewItems,
                           canConnectExistingItems,
                           canConnectMultipleItems, response_fields=None, headers={}):
        """ Create pipe relation: Mutation to create a pipe relation between two pipes, in case of success a query is returned. """

        response_fields = response_fields or 'pipeRelation { id name }'
        query = '''
            mutation {
              createPipeRelation(
                input: {
                  parentId: %(parentId)s
                  childId: %(childId)s
                  name: %(name)s
                  allChildrenMustBeDoneToFinishParent: %(allChildrenMustBeDoneToFinishParent)s
                  childMustExistToMoveParent: %(childMustExistToMoveParent)s
                  childMustExistToFinishParent: %(childMustExistToFinishParent)s
                  allChildrenMustBeDoneToMoveParent: %(allChildrenMustBeDoneToMoveParent)s
                  canCreateNewItems: %(canCreateNewItems)s
                  canConnectExistingItems: %(canConnectExistingItems)s
                  canConnectMultipleItems: %(canConnectMultipleItems)s
                }
              ) { %(response_fields)s }
            }
        ''' % {
            'parentId': json.dumps(parentId),
            'childId': json.dumps(childId),
            'name': json.dumps(name),
            'allChildrenMustBeDoneToFinishParent': json.dumps(allChildrenMustBeDoneToFinishParent),
            'childMustExistToMoveParent': json.dumps(childMustExistToMoveParent),
            'childMustExistToFinishParent': json.dumps(childMustExistToFinishParent),
            'allChildrenMustBeDoneToMoveParent': json.dumps(allChildrenMustBeDoneToMoveParent),
            'canCreateNewItems': json.dumps(canCreateNewItems),
            'canConnectExistingItems': json.dumps(canConnectExistingItems),
            'canConnectMultipleItems': json.dumps(canConnectMultipleItems),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('createPipeRelation', {}).get('pipeRelation')

    def updatePipeRelation(self, id, name, allChildrenMustBeDoneToFinishParent, childMustExistToMoveParent,
                           childMustExistToFinishParent, allChildrenMustBeDoneToMoveParent, canCreateNewItems,
                           canConnectExistingItems,
                           canConnectMultipleItems, response_fields=None, headers={}):
        """ Update pipe relation: Mutation to update a pipe relation, in case of success a query is returned. """

        response_fields = response_fields or 'pipeRelation { id name }'
        query = '''
            mutation {
              updatePipeRelation(
                input: {
                  id: %(id)s
                  name: %(name)s
                  allChildrenMustBeDoneToFinishParent: %(allChildrenMustBeDoneToFinishParent)s
                  childMustExistToMoveParent: %(childMustExistToMoveParent)s
                  childMustExistToFinishParent: %(childMustExistToFinishParent)s
                  allChildrenMustBeDoneToMoveParent: %(allChildrenMustBeDoneToMoveParent)s
                  canCreateNewItems: %(canCreateNewItems)s
                  canConnectExistingItems: %(canConnectExistingItems)s
                  canConnectMultipleItems: %(canConnectMultipleItems)s
                }
              ) { %(response_fields)s }
            }
        ''' % {
            'id': json.dumps(id),
            'name': json.dumps(name),
            'allChildrenMustBeDoneToFinishParent': json.dumps(allChildrenMustBeDoneToFinishParent),
            'childMustExistToMoveParent': json.dumps(childMustExistToMoveParent),
            'childMustExistToFinishParent': json.dumps(childMustExistToFinishParent),
            'allChildrenMustBeDoneToMoveParent': json.dumps(allChildrenMustBeDoneToMoveParent),
            'canCreateNewItems': json.dumps(canCreateNewItems),
            'canConnectExistingItems': json.dumps(canConnectExistingItems),
            'canConnectMultipleItems': json.dumps(canConnectMultipleItems),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('updatePipeRelation', {}).get('pipeRelation')

    def deletePipeRelation(self, id, response_fields=None, headers={}):
        """ Delete pipe relation: Mutation to delete a pipe relation, in case of success success: true is returned. """

        response_fields = response_fields or 'success'
        query = 'mutation { deletePipeRelation(input: { id: %(id)s }) { %(response_fields)s }' % {
            'id': json.dumps(id),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('deletePipeRelation', {})

    def tables(self, ids, response_fields=None, headers={}):
        """ List tables: Get tables through table ids. """

        response_fields = response_fields or 'id name url'
        query = '{ tables(ids: [%(ids)s]) { %(response_fields)s } }' % {
            'ids': ', '.join([json.dumps(id) for id in ids]),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('tables')

    def table(self, id, response_fields=None, headers={}):
        """ Show table: Get a table through table id. """

        response_fields = response_fields or 'authorization create_record_button_label description' \
                                             ' icon id labels { id } members { role_name user { id } } my_permissions { can_manage_record ' \
                                             'can_manage_table } name public public_form summary_attributes { id } summary_options { name } ' \
                                             'table_fields { id } table_records { edges { node { id } } } table_records_count title_field { id } url }'
        query = '{ table(id: %(id)s) { %(response_fields)s } }' % {
            'id': json.dumps(id),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('table')

    def tableRecords(self, table_id, count=10, search={}, response_fields=None, headers={}):
        """ List table Records: Get records by table identifier. """

        response_fields = response_fields or 'edges { node { id title created_at status {id name}' \
                                             'record_fields {array_value value field {id type}}}}'

        query = '{ table_records(table_id: %(table_id)s, first: %(count)s, search: %(search)s) { %(response_fields)s } }' % {
            'table_id': json.dumps(table_id),
            'count': json.dumps(count),
            'search': self.__prepare_json_dict(search),
            'response_fields': response_fields,
        }

        return self.request(query, headers).get('data', {}).get('table_records', [])

    def createTable(self, organization_id, name, description, public, authorization, response_fields=None, headers={}):
        """ Create table: Mutation to create a table, in case of success a query is returned. """

        response_fields = response_fields or 'table { id name description public authorization }'
        query = '''
            mutation {
              createTable(
                input: {
                  organization_id: %(organization_id)s
                  name: %(name)s
                  description: %(description)s
                  public: %(public)s
                  authorization: %(authorization)s
                }
              ) { %(response_fields)s }
            }
        ''' % {
            'organization_id': json.dumps(organization_id),
            'name': json.dumps(name),
            'description': json.dumps(description),
            'public': json.dumps(public),
            'authorization': json.dumps(authorization),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('createTable', {}).get('table')

    def updateTable(self, id, name, description, public, authorization, icon, create_record_button_label,
                    title_field_id, public_form, summary_attributes, response_fields=None, headers={}):
        """ Update table: Mutation to update a table, in case of success a query is returned. """

        response_fields = response_fields or 'table { id name description public authorization }'
        query = '''
            mutation {
              updateTable(
                input: {
                  id: %(id)s
                  name: %(name)s
                  description: %(description)s
                  public: %(public)s
                  authorization: %(authorization)s
                  icon: %(icon)s
                  create_record_button_label: %(create_record_button_label)s
                  title_field_id: %(title_field_id)s
                  public_form: %(public_form)s
                  summary_attributes: [ %(summary_attributes)s ]
                }
              ) { %(response_fields)s }
            }
        ''' % {
            'id': json.dumps(id),
            'name': json.dumps(name),
            'description': json.dumps(description),
            'public': json.dumps(public),
            'authorization': json.dumps(authorization),
            'icon': json.dumps(icon),
            'create_record_button_label': json.dumps(create_record_button_label),
            'title_field_id': json.dumps(title_field_id),
            'public_form': json.dumps(public_form),
            'summary_attributes': ', '.join([json.dumps(summary) for summary in summary_attributes]),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('updateTable', {}).get('table')

    def deleteTable(self, id, response_fields=None, headers={}):
        """ Delete table: Mutation to delete a table, in case of success a query with the field success is returned. """

        response_fields = response_fields or 'success'
        query = 'mutation { deleteTable(input: { id: %(id)s }) { %(response_fields)s }' % {
            'id': json.dumps(id),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('deleteTable', {})

    def createTableField(self, table_id, type, label, options, description, help, required,
                         minimal_view, custom_validation, response_fields=None, headers={}):
        """ Create table field: Mutation to create a table field, in case of success a query is returned. """

        response_fields = response_fields or 'table_field { id label type options description help required minimal_view custom_validation }'
        query = '''
            mutation {
              createTableField(
                input: {
                  table_id: %(table_id)s
                  type: %(type)s
                  label: %(label)s
                  options: %(options)s
                  description: %(description)s
                  help: %(help)s
                  required: %(required)s
                  minimal_view: %(minimal_view)s
                  custom_validation: %(custom_validation)s
                }
              ) { %(response_fields)s }
            }
        ''' % {
            'table_id': json.dumps(table_id),
            'type': json.dumps(type),
            'label': json.dumps(label),
            'options': self.__prepare_json_list(options),
            'description': json.dumps(description),
            'help': json.dumps(help),
            'required': json.dumps(required),
            'minimal_view': json.dumps(minimal_view),
            'custom_validation': json.dumps(custom_validation),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('createTableField', {}).get('table_field')

    def updateTableField(self, table_id, id, label, options, description, help, required,
                         minimal_view, custom_validation, response_fields=None, headers={}):
        """ Update table field: Mutation to update a table field, in case of success a query is returned. """

        response_fields = response_fields or 'table_field { id label type options description help required minimal_view custom_validation }'
        query = '''
            mutation {
              updateTableField(
                input: {
                  table_id: %(table_id)s
                  id: %(id)s
                  label: %(label)s
                  options: %(options)s
                  description: %(description)s
                  help: %(help)s
                  required: %(required)s
                  minimal_view: %(minimal_view)s
                  custom_validation: %(custom_validation)s
                }
              ) { %(response_fields)s }
            }
        ''' % {
            'table_id': json.dumps(table_id),
            'id': json.dumps(id),
            'label': json.dumps(label),
            'options': self.__prepare_json_list(options),
            'description': json.dumps(description),
            'help': json.dumps(help),
            'required': json.dumps(required),
            'minimal_view': json.dumps(minimal_view),
            'custom_validation': json.dumps(custom_validation),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('updateTableField', {}).get('table_field')

    def setTableFieldOrder(self, table_id, field_ids, response_fields=None, headers={}):
        """ Set table record field value Mutation to set a table field order, in case of success a query with the field success is returned. """

        response_fields = response_fields or 'table_field { id }'
        query = '''
            mutation {
              setTableFieldOrder(
                input: {
                  table_id: %(table_id)s
                  field_ids: %(field_ids)s
                }
              ) { %(response_fields)s }
            }
        ''' % {
            'table_id': json.dumps(table_id),
            'field_ids': self.__prepare_json_list(field_ids),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('setTableFieldOrder', {}).get('table_field')

    def deleteTableField(self, table_id, id, response_fields=None, headers={}):
        """ Delete table field: Mutation to delete a table field, in case of success a query with the field success is returned. """

        response_fields = response_fields or 'success'
        query = 'mutation { deleteTableField(input: { table_id: %(table_id)s id: %(id)s }) { %(response_fields)s }' % {
            'table_id': json.dumps(table_id),
            'id': json.dumps(id),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('deleteTableField', {})

    def table_records(self, table_id, first=10, response_fields=None, headers={}, search={}):
        """ List table records: Get table records with pagination through table id. """

        response_fields = response_fields or 'edges { cursor node { id title url } } pageInfo { endCursor hasNextPage hasPreviousPage startCursor }'
        query = '{ table_records(first: %(first)s, table_id: %(table_id)s, search: %(search)s) { %(response_fields)s } }' % {
            'first': json.dumps(first),
            'table_id': json.dumps(table_id),
            'response_fields': response_fields,
            'search': self.__prepare_json_dict(search),
        }
        return self.request(query, headers).get('data', {}).get('table_records')

    def table_record(self, id, response_fields=None, headers={}):
        """ Show table record: Get table record through table record id. """

        response_fields = response_fields or 'assignees { id name } created_at created_by { id name } due_date' \
                                             ' finished_at id labels { id name } parent_relations { name source_type } record_fields { array_value ' \
                                             'field {id} date_value datetime_value filled_at float_value name required updated_at value } summary { title value } ' \
                                             'table { id } title updated_at url }'
        query = '{ table_record(id: %(id)s) { %(response_fields)s } ' % {
            'id': json.dumps(id),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('table_record')

    def createTableRecord(self, table_id, title='', due_date=None, fields_attributes=[], response_fields=None,
                          headers={}):
        """ Create table record: Mutation to create a table record, in case of success a query is returned. """

        response_fields = response_fields or 'table_record { id title due_date record_fields { name value } }'
        query = '''
            mutation {
              createTableRecord(
                input: {
                  table_id: %(table_id)s
                  %(title)s
                  %(due_date)s
                  fields_attributes: %(fields_attributes)s
                }
              ) { %(response_fields)s }
            }
        ''' % {
            'table_id': json.dumps(table_id),
            'title': u'title: %s' % json.dumps(title) if title else '',
            'due_date': u'due_date: %s' % due_date.strftime('%Y-%m-%dT%H:%M:%S+00:00') if due_date else '',
            'fields_attributes': self.__prepare_json_list(fields_attributes),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('createTableRecord', {}).get('table_record')

    def updateTableRecord(self, id, title, due_date, response_fields=None, headers={}):
        """ Update table record: Mutation to update a table record, in case of success a query is returned. """

        response_fields = response_fields or 'table_record { id title due_date record_fields { name value } }'
        query = '''
            mutation {
              updateTableRecord(
                input: {
                  id: %(id)s
                  title: %(title)s
                  due_date: %(due_date)s
                }
              ) { %(response_fields)s }
            }
        ''' % {
            'id': json.dumps(id),
            'title': json.dumps(title),
            'due_date': due_date.strftime('%Y-%m-%dT%H:%M:%S+00:00'),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('updateTableRecord', {}).get('table_record')

    def setTableRecordFieldValue(self, table_record_id, field_id, value, response_fields=None, headers={}):
        """ Set table record field value: Mutation to set a table record field value, in case of success a query with the field success is returned. """

        response_fields = response_fields or 'table_record { id title } table_record_field { value }'
        query = '''
            mutation {
              setTableRecordFieldValue(
                input: {
                  table_record_id: %(table_record_id)s
                  field_id: %(field_id)s
                  value: %(value)s
                }
              ) { %(response_fields)s }
            }
        ''' % {
            'table_record_id': json.dumps(table_record_id),
            'field_id': json.dumps(field_id),
            'value': json.dumps(value),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('setTableRecordFieldValue', {})

    def deleteTableRecord(self, id, response_fields=None, headers={}):
        """ Delete table record: Mutation to delete a table record, in case of success a query with the field success is returned. """

        response_fields = response_fields or 'success'
        query = '''
                    mutation { 
                        deleteTableRecord(
                            input: { 
                                id: %(id)s 
                            }) { %(response_fields)s 
                        }
                    }
                ''' % {
            'id': json.dumps(id),
            'response_fields': response_fields,
        }
        return self.request(query, headers).get('data', {}).get('deleteTableRecord', {})

    def getFieldValueById(self, payload, field_id, array_value=None):
        """ created by: silvio.angelo@accenture.com  """
        """ Recupera o valor do campo informado"""
        valor_final = ""

        try:
            payload_field = payload['fields']
        except:
            payload_field = payload['record_fields']

        for field in payload_field:
            valor_array = field['array_value']
            valor = field['value']
            if valor is None:
                valor = ""
            else:
                valor = valor.strip()

            if field['field']['id'] == field_id and valor != '':
                if field['field']['type'] in ['connector', 'attachment', 'label_select', 'checklist_vertical',
                                              'assignee_select'] and array_value is None:
                    valor_final = valor_array
                else:
                    if array_value is None:
                        valor_final = valor
                    else:
                        valor_final = json.loads(valor)

        return valor_final

    def createPresignedUrl(self, organization_id, filename, response_fields=None, headers={}):
        """ created by: silvio.angelo@accenture.com  """
        """ Create the PresignedUrl: Mutation that returns a url based on organization id and filename. """

        response_fields = response_fields or 'url'
        query = '''
                    mutation{
                        createPresignedUrl(
                            input: { 
                                organizationId: %(organizationId)s, 
                                fileName: %(fileName)s 
                            }){ %(response_fields)s 
                        }
                    }
                ''' % {
            'organizationId': json.dumps(organization_id),
            'fileName': json.dumps(filename),
            'response_fields': response_fields
        }

        return self.request(query, headers).get('data', {}).get('createPresignedUrl', {})

    def uploadFileToAws(self, url, data):
        """ created by: silvio.angelo@accenture.com  """
        payload = data
        r = requests.put(url, data=payload)
        return r.status_code == 200

    def updateAttachmentFilesToCard(self, data):
        """ created by: silvio.angelo@accenture.com  """
        """ Attach a File to Card: Mutation that make a upload to a specific attachment field  """
        '''
        is_local=True -> Test Local Upload setting file on C:\tmp
        Must send a dictionary like follows:
            
            data = {
                'organization_id': 999,
                'card_id': 999,
                'field_id': '',
                'attachment': [
                    {
                        'type': '',  # local/url/base64/aws options
                        'data': '', # data following the type options above
                        'filename': ''  # filename with extension
                    }
                ]
            }
            
            Ex.
            data = {
            'organization_id': 99999,
            'card_id': 99999999,
            'field_id': 'anexo',
            'attachment': [
                {
                    'type': 'base64',
                    'data': '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',
                    'filename': 'image.jpg'
                },
                {
                    'type': 'url',
                    'data': 'https://www.tutorialspoint.com/3d_figures_and_volumes/images/logo.png',
                    'filename': 'logo.png'
                },
                {
                    'type': 'local',
                    'data': 'C:/Users/silvio.angelo/Downloads/ANEXO_TESTE.pdf',
                    'filename': 'ANEXO_TESTE.pdf'
                },
                {
                    'type': 'pipefy',
                    'data': 'https://pipefy-prd-us-east-1.s3.amazonaws.com/uploads/dbfd2e82-4ac3-4333-9634-e5e7a0e61582/ANEXO_TESTE_2.pdf',
                    'filename': 'ANEXO_TESTE_2.pdf'
                }
            ]
        }
        '''
        conf_schema = Schema(
            {
                'organization_id': And(Use(int)),
                'card_id': And(Use(int)),
                'field_id': And(Use(str)),
                'attachment': [
                    {
                        'type': And(Use(str)),
                        'data': And(Use(str)),
                        'filename': And(Use(str))
                    }
                ]
            }

        )

        try:
            conf_schema.validate(data)
            if len(data['attachment']) == 0:
                raise SchemaError('attachment length equal to zero')
        except SchemaError as e:
            print("attachFileToCard Error: data : {}, SchemaError: {}".format(str(data), str(e)))
            raise e

        file_path = '/tmp/'
        if platform == "win32" or  platform == "win64":
            file_path = 'C:/tmp/'
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        for attachment in data['attachment']:
            type_att = attachment['type']
            data_att = attachment['data']
            filename_att = attachment['filename']
            upload_aws = False

            if type_att == 'base64':
                upload_aws = True
                if not type(data_att) == 'binary':
                    data_att = data_att.encode('utf-8')

                with open(file_path+filename_att, 'wb') as file_to_save:
                    decoded_data = base64.decodebytes(data_att)
                    file_to_save.write(decoded_data)

            elif type_att == 'url':
                upload_aws = True
                r = requests.get(data_att, allow_redirects=True)
                open(file_path+filename_att, 'wb').write(r.content)
                decoded_data = open(file_path+filename_att, 'rb').read()

            elif type_att == 'local':
                upload_aws = True
                decoded_data = open(data_att, 'rb').read()

            elif type_att == 'pipefy':
                attachment['url_aws'] = data_att
                url_aws = data_att

            else:
                attachment['url_aws'] = ''

            if upload_aws:
                response = self.createPresignedUrl(data['organization_id'], filename_att)
                url_aws = response['url']
                self.uploadFileToAws(url_aws, decoded_data)

            attachment['url_aws'] = url_aws

            if url_aws.find('/orgs/') > 0:
                start = url_aws.find('/orgs/')
            else:
                start = url_aws.find('/uploads/')

            if url_aws.find('?') > 0:
                end = url_aws.find('?')
            else:
                end = len(url_aws)

            attachment['url_pipefy'] = url_aws[start:end]

        attachments_url_pipefy = [attachment['url_pipefy'] for attachment in data['attachment']]

        self.updateCardField(data['card_id'], data['field_id'], attachments_url_pipefy)
