from aredis import StrictRedis


class RedisHandler:
    def __init__(self):
        self.r = None
        self.p = None

    def connect(self, conf):
        if self.r is None:
            self.r = StrictRedis(host=conf[0], port=conf[1], password=conf[2], db=conf[3], decode_responses=True)
            self.p = self.r.pubsub(ignore_subscribe_messages=True)
        return self.r, self.p

