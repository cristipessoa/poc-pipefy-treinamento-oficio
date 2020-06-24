from flask import Flask, request, jsonify
from app.app import run
from app.resources.config import load_config_ini


def process(request_process):
    config = load_config_ini()

    code = 200
    message = 'success'

    try:
        run(request_process.get_json())
    except Exception as ex:
        code = 500
        message = str(ex)

    response = {
        'gcp-function': {
            'name': config['Python']['name'],
            'version': config['Python']['version'],
            'deploy_hour_minutes': config['Python']['deploy_hour_minutes']
        },
        'regra-negocio': {
            'code': code,
            'message': message
        }
    }
    return jsonify(response)


# Region Metodo para a Google Cloud Plataform
def main(request_main):
    return process(request_main)


# End Region

# Region Metodo para Testar Local via Postman/Rest Client
# url:http://127.0.0.1:8090/main


app = Flask(__name__)


@app.route('/main', methods=['POST'])
def main_flask():
    return process(request)


app.run(host='0.0.0.0', port=8090, debug=True)


# End Region