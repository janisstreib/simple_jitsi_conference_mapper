from flask import Flask, jsonify, abort, request
import redis
from random import randint

app = Flask(__name__)
app.config.from_pyfile('config.py')

@app.route('/')
def map():
    with redis.Redis(db=int(app.config.get('DB_INDEX'))) as r:
        if 'id' in request.args:
            return jsonify(get_conf_by_id(r, request.args['id']))
        if 'conference' in request.args:
            pipe = r.pipeline()
            conf = get_id_by_conf(r, request.args['conference'])
            if conf is not None:
                return jsonify(conf)
            newid = randint(100000, 999999)
            while r.exists(newid):
                newid = randint(100000, 999999)
            pipe.watch(newid, request.args.get('conference'))
            pipe.multi()
            pipe.mset({newid: request.args.get('conference')})
            pipe.mset({f"conf_{request.args.get('conference')}":newid})
            pipe.expire(newid, config.get('ID_EXPIRY_TIME'))
            pipe.expire(request.args.get('conference'), 60*60*12)
            pipe.execute()
            return jsonify({'id': newid, 'conference': request.args['conference']})

def get_id_by_conf(r, conf):
        q_conf = f'conf_{conf}'
        if not r.exists(q_conf):
            return None
        id = int(r.get(q_conf))
        return {'conference': conf, 'id':id}

def get_conf_by_id(r, id):
        id = int(id)
        if not r.exists(id):
            return None
        conf = r.get(id).decode('utf-8')
        return {'id': id, 'conference':conf}

