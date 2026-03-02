import smtplib
from email.utils import make_msgid

from django.conf import settings
from django.core.mail.backends.smtp import EmailBackend as DjangoSMTPEmailBackend


class EHLOAndMsgIdEmailBackend(DjangoSMTPEmailBackend):
    def _ehlo_fqdn(self) -> str:
        return getattr(settings, "SMTP_EHLO_FQDN", "")

    def _msgid_domain(self) -> str:
        return getattr(settings, "SMTP_MSGID_DOMAIN", "")

    def open(self):
        if self.connection:
            return False


        self.connection = self.connection_class(
            self.host,
            self.port,
            local_hostname=self._ehlo_fqdn(), #sets EHLO
            timeout=self.timeout,
        )

        if self.use_tls:
            self.connection.starttls(context=self.ssl_context)

        if self.username and self.password:
            self.connection.login(self.username, self.password)

        return True

    def send_messages(self, email_messages):
        if not email_messages:
            return 0

        new_conn_created = self.open()
        if not self.connection:
            return 0

        sent = 0
        for email_message in email_messages:
            msg = email_message.message()

            # Force Message-ID domain
            new_id = make_msgid(domain=self._msgid_domain())
            if msg.get("Message-ID"):
                msg.replace_header("Message-ID", new_id)
            else:
                msg["Message-ID"] = new_id

            self.connection.sendmail(
                email_message.from_email,
                email_message.recipients(),
                msg.as_bytes(linesep="\r\n"),
            )
            sent += 1

        if new_conn_created:
            self.close()

        return sent
