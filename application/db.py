class DBConnection:
    __instance = None
    engine = None

    @staticmethod
    def getInstance():
        if DBConnection.__instance is None:
            DBConnection()
        return DBConnection.__instance

    def __init__(self):
        if DBConnection.__instance is not None:
            raise Exception("This is a singleton class")
        else:
            from factory import db

            self.engine = db.get_engine()
            DBConnection.__instance = self

    def get_engine(self):
        if self.engine is None:
            raise Exception("Engine Not Initialized")
        return self.engine
