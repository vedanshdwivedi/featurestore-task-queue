import os
from pprint import pprint

import ElasticEmail
from ElasticEmail.api import emails_api
from ElasticEmail.model.email_message_data import EmailMessageData
from ElasticEmail.model.email_recipient import EmailRecipient


class EmailConnection:
    __instance = None
    email_client = None

    @staticmethod
    def getInstance():
        if EmailConnection.__instance is None:
            EmailConnection()
        return EmailConnection.__instance

    def __init__(self):
        if EmailConnection.__instance is not None:
            raise Exception("This is a singleton class")
        else:
            connection_string = os.getenv("EMAIL_API_KEY")
            self.email_client = ElasticEmail.Configuration()
            self.email_client.api_key["apikey"] = connection_string
            EmailConnection.__instance = self

    def send_email(self, receipient: str, subject: str, content: str):
        with ElasticEmail.ApiClient(self.email_client) as api_client:
            api_instance = emails_api.EmailsApi(api_client)
            email_message_data = EmailMessageData(
                recipients=[EmailRecipient(email=receipient),],
                content={
                    "Body": [{"ContentType": "HTML", "Content": content}],
                    "Subject": subject,
                    "From": "tappsvd@gmail.com",
                },
            )

            try:
                api_response = api_instance.emails_post(email_message_data)
                pprint(api_response)
            except ElasticEmail.ApiException as e:
                print("Exception when calling EmailsApi->emails_post: %s\n" % e)
