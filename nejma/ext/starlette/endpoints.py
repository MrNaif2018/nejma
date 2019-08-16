from starlette.endpoints import WebSocketEndpoint as BaseWebSocketEndpoint


from nejma.layers import Channel, channel_layer


class WebSocketEndpoint(BaseWebSocketEndpoint):
    encoding = "json"

    async def on_connect(self, websocket, **kwargs):
        await super().on_connect(websocket, **kwargs)

        if self.encoding == "json":
            send_ = websocket.send_json
        elif self.encoding == "text":
            send_ = websocket.send_text
        elif self.encoding == "bytes":
            send_ = websocket.send_bytes
        else:
            send_ = websocket.send
        self.channel = Channel()
        self.channel_layer.send = send_

    async def on_disconnect(self, websocket, close_code):
        await self.channel_layer.remove_channel(self.channel)
