"""Module which contains base class for ScraPy's 'spiders' """
from OpenSSL import SSL
from scrapy.core.downloader.contextfactory import BrowserLikeContextFactory


class CustomCipherContextFactory(BrowserLikeContextFactory):
    """A more protocol-flexible TLS/SSL context factory.

    A TLS/SSL connection established with [SSLv23_METHOD] may understand
    the SSLv3, TLSv1, TLSv1.1 and TLSv1.2 protocols.
    See https://www.openssl.org/docs/manmaster/ssl/SSL_CTX_new.html
    """

    def __init__(self, *args, method=SSL.TLSv1_2_METHOD, **kwargs):
        return super().__init__(method=method, *args, **kwargs)

    def getContext(self, hostname=None, port=None):
        ctx = super().getContext(hostname, port)
        ctx.set_cipher_list('ECDHE-RSA-AES256-GCM-SHA384')
        return ctx
