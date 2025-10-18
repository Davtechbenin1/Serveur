#Coding:utf-8
import asyncio
import json
from fastapi import WebSocket, WebSocketDisconnect

class ConnectionManager:
    """
    Gère connexions WebSocket et abonnements par 'table'.
    On stocke les websockets dans un dict {id(ws): ws} et les subscriptions par id pour être sûr
    d'éviter des problèmes d'unhashable.
    """
    def __init__(self):
        self.active_connections: dict[int, WebSocket] = {}
        self.subscriptions: dict[str, set[int]] = {}
        self.lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        async with self.lock:
            self.active_connections[id(websocket)] = websocket

    async def disconnect(self, websocket: WebSocket):
        async with self.lock:
            ws_id = id(websocket)
            # retire la connexion active
            self.active_connections.pop(ws_id, None)
            # retire des subscriptions
            for s in list(self.subscriptions.keys()):
                self.subscriptions[s].discard(ws_id)
                if not self.subscriptions[s]:
                    self.subscriptions.pop(s, None)

    async def subscribe(self, websocket: WebSocket, table: str):
        async with self.lock:
            ws_id = id(websocket)
            self.subscriptions.setdefault(table, set()).add(ws_id)

    async def unsubscribe(self, websocket: WebSocket, table: str):
        async with self.lock:
            ws_id = id(websocket)
            if table in self.subscriptions:
                self.subscriptions[table].discard(ws_id)
                if not self.subscriptions[table]:
                    self.subscriptions.pop(table, None)

    async def broadcast_table_update(self, table: str, payload: dict):
        """Envoie payload (dict) en JSON à tous les abonnés de 'table'."""
        # copie des destinataires pour éviter mutation concurrente
        async with self.lock:
            recipient_ids = set(self.subscriptions.get(table, set()))
            dests = [self.active_connections.get(i) for i in recipient_ids if i in self.active_connections]
        if not dests:
            return
        msg = json.dumps(payload)
        # envoi concurrent
        await asyncio.gather(*(self._safe_send(ws, msg) for ws in dests))

    async def _safe_send(self, websocket: WebSocket, message: str):
        try:
            await websocket.send_text(message)
        except Exception:
            # si erreur -> on déconnecte proprement
            await self.disconnect(websocket)
