import datetime
import os
from dataclasses import dataclass, field

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from typing import List


def generate_ssh_key(path):
    ssh_privkey_filename = path
    ssh_pubkey_filename = f"{path}.pub"

    print("Generating SSH Key ...")
    privkey = rsa.generate_private_key(65537, 2048)
    with open(ssh_privkey_filename, "wb") as f:
        f.write(
            privkey.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.OpenSSH,
                serialization.NoEncryption(),
            )
        )
    pubkey = privkey.public_key().public_bytes(serialization.Encoding.OpenSSH, serialization.PublicFormat.OpenSSH)
    with open(ssh_pubkey_filename, "wb") as f:
        f.write(pubkey)
    print("SSH Key has been generated, Public Key:\n")
    print(pubkey.decode("ASCII"))
    print()


@dataclass
class CertificateInformation:
    country: str
    state_or_province: str
    locality: str
    organization: str
    common_name: str

    passphare: str = None
    dns_names: List[str] = field(default_factory=list)


def generate_self_signed_cert_file(dir_path, **cert_info: CertificateInformation):
    """
    This helper method generates SSL certificate, which can be passed to TORNADO_SETTINGS to support HTTPS.
    The pkcs12 file should be added to browser, but we still need to manually tell browser to allow us access our hub.

    Example Tornado settings:
    ```
        import ssl

        ssl_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_ctx.load_cert_chain(
            os.path.join(path_to_cert_file),
            os.path.join(path_to_key_file),
        )

        TORNADO_SETTINGS = {
            "ssl_options": ssl_ctx
        }
    ```

    Ref:
    https://cryptography.io/en/latest/x509/tutorial/#creating-a-self-signed-certificate
    https://cryptography.io/en/latest/hazmat/primitives/asymmetric/serialization/#cryptography.hazmat.primitives.serialization.pkcs12.serialize_key_and_certificates
    """  # NOQA

    key_filename = os.path.join(dir_path, "key.pem")
    cert_filename = os.path.join(dir_path, "cert.pem")
    pkcs12_filename = os.path.join(dir_path, "cert.p12")
    cert_info = CertificateInformation(**cert_info)

    print("Generating private key ...")
    key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )

    # Write our key to disk for safe keeping
    print("Write private key to file ...")
    encryption_algorithm = serialization.NoEncryption()
    if cert_info.passphare:
        encryption_algorithm = serialization.BestAvailableEncryption(cert_info.passphare.encode())
    with open(key_filename, "wb") as f:
        f.write(
            key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=encryption_algorithm,
            )
        )

    # Various details about who we are. For a self-signed certificate the
    # subject and issuer are always the same.
    print("Generating certificate ...")
    subject = issuer = x509.Name(
        [
            x509.NameAttribute(NameOID.COUNTRY_NAME, cert_info.country),
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, cert_info.state_or_province),
            x509.NameAttribute(NameOID.LOCALITY_NAME, cert_info.locality),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, cert_info.organization),
            x509.NameAttribute(NameOID.COMMON_NAME, cert_info.common_name),
        ]
    )
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.utcnow())
        .not_valid_after(
            # Our certificate will be valid for 10 days
            datetime.datetime.utcnow()
            + datetime.timedelta(days=10)
        )
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName(dns_name) for dns_name in cert_info.dns_names or ["localhost"]]),
            critical=False,
            # Sign our certificate with our private key
        )
        .sign(key, hashes.SHA256())
    )

    # Write our certificate out to disk.
    print("Writing certificate to file ...")
    with open(cert_filename, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))

    # Generate PKCS12 to import to browser.
    print("Generate PKCS12 ...")
    pkcs12 = serialization.pkcs12.serialize_key_and_certificates(b"", key, cert, None, serialization.NoEncryption())

    # Write PKCS12 file to import to browser.
    print("Writing PKCS12 to file ...")
    with open(pkcs12_filename, "wb") as f:
        f.write(pkcs12)

    print("Done.")
