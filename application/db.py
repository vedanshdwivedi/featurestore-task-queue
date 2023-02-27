class DBConnection:
    __instance = None
    __mongoInstance = None
    engine = None
    mongoEngine = None

    @staticmethod
    def getMongoInstance():
        if DBConnection.__mongoInstance is None:
            DBConnection()
        return DBConnection.__mongoInstance

    @staticmethod
    def getInstance():
        if DBConnection.__instance is None:
            DBConnection()
        return DBConnection.__instance

    def __init__(self):
        if DBConnection.__instance is not None or DBConnection.__mongoInstance is not None:
            raise Exception("This is a singleton class")
        else:
            from factory import db, mongo

            self.engine = db.get_engine()
            self.mongoEngine = mongo
            DBConnection.__mongoInstance = self
            DBConnection.__instance = self

    def get_engine(self):
        if self.engine is None:
            raise Exception("Engine Not Initialized")
        return self.engine

    def get_mongo_engine(self):
        if self.mongoEngine is None:
            raise Exception("Mongo Engine Not Initialized")
        return self.mongoEngine
