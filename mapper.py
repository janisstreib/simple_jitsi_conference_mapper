from flask import Flask, jsonify, abort, request
import redis
from random import randint
import re

app = Flask(__name__)
app.config.from_pyfile('config.py')

@app.route('/')
def map():
    with redis.Redis(db=int(app.config.get('DB_INDEX'))) as r:
        if 'id' in request.args:
            return jsonify(get_conf_by_id(r, request.args['id']))
        if 'conference' in request.args:
            if not re.match(re.compile(app.config.get('ALLOWED_CONF_REGEX', '.*')), request.args['conference']):
                return abort(403, "Invalid conference name")
            pipe = r.pipeline()
            conf = get_id_by_conf(r, request.args['conference'])
            newid = None
            if conf is not None:
                newid = conf['id']
            else:
                newid = randint(app.config.get('CONF_PIN_MIN', 100000), app.config.get('CONF_PIN_MAX', 999999))
                while r.exists(newid):
                    newid = randint(app.config.get('CONF_PIN_MIN', 100000), app.config.get('CONF_PIN_MAX', 999999))
            pipe.watch(newid, f"conf_{request.args.get('conference')}")
            pipe.multi()
            pipe.mset({newid: request.args.get('conference')})
            pipe.mset({f"conf_{request.args.get('conference')}":newid})
            pipe.expire(newid, app.config.get('ID_EXPIRY_TIME'))
            pipe.expire(f"conf_{request.args.get('conference')}", app.config.get('ID_EXPIRY_TIME'))
            pipe.execute()
            return jsonify({'id': newid, 'conference': request.args['conference']})

def get_id_by_conf(r, conf):
        q_conf = f'conf_{conf}'
        if not r.exists(q_conf):
            return None
        id = int(r.get(q_conf))
        return {'conference': conf, 'id':id}

def get_conf_by_id(r, id):
        try:
            id = int(id)
            if not r.exists(id):
                return None
            conf = r.get(id).decode('utf-8')
            return {'id': id, 'conference':conf}
        except:
            return None

