package net.corda.nodeapi.internal.crypto

import net.corda.core.CordaOID
import net.corda.core.crypto.Crypto
import net.corda.core.crypto.newSecureRandom
import net.corda.core.internal.*
import net.corda.core.utilities.days
import net.corda.core.utilities.millis
import net.i2p.crypto.eddsa.EdDSAPublicKey
import org.bouncycastle.asn1.*
import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.asn1.x500.style.BCStyle
import org.bouncycastle.asn1.x509.*
import org.bouncycastle.asn1.x509.Extension
import org.bouncycastle.cert.X509CertificateHolder
import org.bouncycastle.cert.X509v3CertificateBuilder
import org.bouncycastle.cert.bc.BcX509ExtensionUtils
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.openssl.jcajce.JcaPEMWriter
import org.bouncycastle.operator.ContentSigner
import org.bouncycastle.operator.jcajce.JcaContentVerifierProviderBuilder
import org.bouncycastle.pkcs.PKCS10CertificationRequest
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequestBuilder
import org.bouncycastle.util.io.pem.PemReader
import java.io.InputStream
import java.math.BigInteger
import java.nio.file.Path
import java.security.KeyFactory
import java.security.KeyPair
import java.security.PublicKey
import java.security.SignatureException
import java.security.cert.*
import java.security.cert.Certificate
import java.security.spec.X509EncodedKeySpec
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.*
import javax.security.auth.x500.X500Principal
import kotlin.experimental.and
import kotlin.experimental.or

object X509Utilities {
    // Note that this default value only applies to BCCryptoService. Other implementations of CryptoService may have to use different
    // schemes (for instance `UtimacoCryptoService.DEFAULT_IDENTITY_SIGNATURE_SCHEME`).
    val DEFAULT_IDENTITY_SIGNATURE_SCHEME = Crypto.EDDSA_ED25519_SHA512
    val DEFAULT_TLS_SIGNATURE_SCHEME = Crypto.ECDSA_SECP256R1_SHA256

    // TODO This class is more of a general purpose utility class and as such these constants belong elsewhere.
    // Aliases for private keys and certificates.
    const val CORDA_ROOT_CA = "cordarootca"
    const val CORDA_INTERMEDIATE_CA = "cordaintermediateca"
    const val CORDA_CLIENT_TLS = "cordaclienttls"
    const val CORDA_CLIENT_CA = "cordaclientca"

    // TODO Hyphen (-) seems to be supported by the major HSM vendors, but we should consider remove it in the
    //      future and stick to [a-z0-9].
    const val NODE_IDENTITY_KEY_ALIAS = "identity-private-key"
    const val DISTRIBUTED_NOTARY_KEY_ALIAS = "distributed-notary-private-key"
    const val DISTRIBUTED_NOTARY_COMPOSITE_KEY_ALIAS = "distributed-notary-composite-key"

    const val TLS_CERTIFICATE_DAYS_TO_EXPIRY_WARNING_THRESHOLD = 30
    private const val KEY_ALIAS_REGEX = "[a-z0-9-]+"
    private const val KEY_ALIAS_MAX_LENGTH = 100
    private const val CERTIFICATE_SERIAL_NUMBER_LENGTH = 16
    private val edDSAPublicKeyConverterKeyFactory = KeyFactory.getInstance("1.3.101.112", BouncyCastleProvider())

    /**
     * Checks if the provided key alias does not exceed maximum length and
     * only contains alphanumeric characters.
     */
    fun isKeyAliasValid(alias: String): Boolean {
        if (alias.length > KEY_ALIAS_MAX_LENGTH) return false
        return KEY_ALIAS_REGEX.toRegex().matches(alias)
    }

    /**
     * The error message to be displayed to the user when the alias validation fails.
     */
    fun invalidKeyAliasErrorMessage(alias: String): String {
        return "Alias '$alias' must contain only lowercase alphanumeric characters and not exceed 100 characters length."
    }

    val DEFAULT_VALIDITY_WINDOW = Pair(0.millis, 3650.days)

    /**
     * Helper function to return the latest out of an instant and an optional date.
     */
    private fun max(first: Instant, second: Date?): Date {
        return if (second != null && second.time > first.toEpochMilli())
            second
        else
            Date(first.toEpochMilli())
    }

    /**
     * Helper function to return the earliest out of an instant and an optional date.
     */
    private fun min(first: Instant, second: Date?): Date {
        return if (second != null && second.time < first.toEpochMilli())
            second
        else
            Date(first.toEpochMilli())
    }

    /**
     * Helper method to get a notBefore and notAfter pair from current day bounded by parent certificate validity range.
     * @param before duration to roll back returned start date relative to current date.
     * @param after duration to roll forward returned end date relative to current date.
     * @param parent if provided certificate whose validity should bound the date interval returned.
     */
    fun getCertificateValidityWindow(before: Duration, after: Duration, parent: X509Certificate? = null): Pair<Date, Date> {
        val startOfDayUTC = Instant.now().truncatedTo(ChronoUnit.DAYS)
        val notBefore = max(startOfDayUTC - before, parent?.notBefore)
        val notAfter = min(startOfDayUTC + after, parent?.notAfter)
        return Pair(notBefore, notAfter)
    }

    /*
     * Create a de novo root self-signed X509 v3 CA cert.
     */
    @JvmStatic
    fun createSelfSignedCACertificate(subject: X500Principal,
                                      keyPair: KeyPair,
                                      validityWindow: Pair<Duration, Duration> = DEFAULT_VALIDITY_WINDOW): X509Certificate {
        val window = getCertificateValidityWindow(validityWindow.first, validityWindow.second)
        return createCertificate(CertificateType.ROOT_CA, subject, keyPair, subject, keyPair.public, window)
    }

    fun validateCertificateChain(trustedRoot: X509Certificate, vararg certificates: X509Certificate) {
        validateCertificateChain(trustedRoot, certificates.asList())
    }

    fun validateCertificateChain(trustedRoot: X509Certificate, certificates: List<X509Certificate>) {
        require(certificates.isNotEmpty()) { "Certificate path must contain at least one certificate" }
        validateCertPath(trustedRoot, buildCertPath(certificates))
    }

    fun validateCertPath(trustedRoot: X509Certificate, certPath: CertPath) {
        certPath.validate(TrustAnchor(trustedRoot, null))
    }

    /**
     * Helper method to store a .pem/.cer format file copy of a certificate if required for import into a PC/Mac, or for inspection.
     * @param certificate certificate to save.
     * @param file Target file.
     */
    @JvmStatic
    fun saveCertificateAsPEMFile(certificate: X509Certificate, file: Path) {
        JcaPEMWriter(file.writer()).use {
            it.writeObject(certificate)
        }
    }

    /**
     * Helper method to load back a .pem/.cer format file copy of a certificate.
     * @param file Source file.
     * @return The X509Certificate that was encoded in the file.
     */
    @JvmStatic
    fun loadCertificateFromPEMFile(file: Path): X509Certificate {
        return file.reader().use {
            val pemObject = PemReader(it).readPemObject()
            X509CertificateHolder(pemObject.content).run {
                isValidOn(Date())
                toJca()
            }
        }
    }

    /**
     * Build a partial X.509 certificate ready for signing.
     *
     * @param certificateType type of the certificate.
     * @param issuer name of the issuing entity.
     * @param issuerPublicKey public key of the issuing entity.
     * @param subject name of the certificate subject.
     * @param subjectPublicKey public key of the certificate subject.
     * @param validityWindow the time period the certificate is valid for.
     * @param nameConstraints any name constraints to impose on certificates signed by the generated certificate.
     * @param crlDistPoint CRL distribution point.
     * @param crlIssuer X500Name of the CRL issuer.
     */
    fun createPartialCertificate(certificateType: CertificateType,
                                 issuer: X500Principal,
                                 issuerPublicKey: PublicKey,
                                 subject: X500Principal,
                                 subjectPublicKey: PublicKey,
                                 validityWindow: Pair<Date, Date>,
                                 nameConstraints: NameConstraints? = null,
                                 crlDistPoint: String? = null,
                                 crlIssuer: X500Name? = null): X509v3CertificateBuilder {
        val serial = generateCertificateSerialNumber()
        val keyPurposes = DERSequence(ASN1EncodableVector().apply { certificateType.purposes.forEach { add(it) } })
        val subjectPublicKeyInfo = SubjectPublicKeyInfo.getInstance(ASN1Sequence.getInstance(subjectPublicKey.encoded))
        val role = certificateType.role

        val builder = JcaX509v3CertificateBuilder(issuer, serial, validityWindow.first, validityWindow.second, subject, subjectPublicKey)
                .addExtension(Extension.subjectKeyIdentifier, false, BcX509ExtensionUtils().createSubjectKeyIdentifier(subjectPublicKeyInfo))
                .addExtension(Extension.basicConstraints, true, BasicConstraints(certificateType.isCA))
                .addExtension(Extension.keyUsage, false, certificateType.keyUsage)
                .addExtension(Extension.extendedKeyUsage, false, keyPurposes)
                .addExtension(Extension.authorityKeyIdentifier, false, JcaX509ExtensionUtils().createAuthorityKeyIdentifier(issuerPublicKey))

        if (role != null) {
            builder.addExtension(ASN1ObjectIdentifier(CordaOID.X509_EXTENSION_CORDA_ROLE), false, role)
        }
        addCrlInfo(builder, crlDistPoint, crlIssuer)
        if (nameConstraints != null) {
            builder.addExtension(Extension.nameConstraints, true, nameConstraints)
        }

        return builder
    }

    /**
     * Create a X509 v3 certificate using the given issuer certificate and key pair.
     *
     * @param certificateType type of the certificate.
     * @param issuerCertificate The Public certificate of the root CA above this used to sign it.
     * @param issuerKeyPair The KeyPair of the root CA above this used to sign it.
     * @param subject subject of the generated certificate.
     * @param subjectPublicKey subject's public key.
     * @param validityWindow The certificate's validity window. Default to [DEFAULT_VALIDITY_WINDOW] if not provided.
     * @param nameConstraints any name constraints to impose on certificates signed by the generated certificate.
     * @param crlDistPoint CRL distribution point.
     * @param crlIssuer X500Name of the CRL issuer.
     * @return A data class is returned containing the new intermediate CA Cert and its KeyPair for signing downstream certificates.
     * Note the generated certificate tree is capped at max depth of 1 below this to be in line with commercially available certificates.
     */
    @JvmStatic
    fun createCertificate(certificateType: CertificateType,
                          issuerCertificate: X509Certificate,
                          issuerKeyPair: KeyPair,
                          subject: X500Principal,
                          subjectPublicKey: PublicKey,
                          validityWindow: Pair<Duration, Duration> = DEFAULT_VALIDITY_WINDOW,
                          nameConstraints: NameConstraints? = null,
                          crlDistPoint: String? = null,
                          crlIssuer: X500Name? = null): X509Certificate {
        val window = getCertificateValidityWindow(validityWindow.first, validityWindow.second, issuerCertificate)
        return createCertificate(
                certificateType,
                issuerCertificate.subjectX500Principal,
                issuerKeyPair,
                subject,
                subjectPublicKey,
                window,
                nameConstraints,
                crlDistPoint,
                crlIssuer
        )
    }

    /**
     * Build and sign an X.509 certificate with the given signer.
     *
     * @param certificateType type of the certificate.
     * @param issuer name of the issuing entity.
     * @param issuerSigner content signer to sign the certificate with.
     * @param subject name of the certificate subject.
     * @param subjectPublicKey public key of the certificate subject.
     * @param validityWindow the time period the certificate is valid for.
     * @param nameConstraints any name constraints to impose on certificates signed by the generated certificate.
     * @param crlDistPoint CRL distribution point.
     * @param crlIssuer X500Name of the CRL issuer.
     */
    fun createCertificate(certificateType: CertificateType,
                          issuer: X500Principal,
                          issuerPublicKey: PublicKey,
                          issuerSigner: ContentSigner,
                          subject: X500Principal,
                          subjectPublicKey: PublicKey,
                          validityWindow: Pair<Date, Date>,
                          nameConstraints: NameConstraints? = null,
                          crlDistPoint: String? = null,
                          crlIssuer: X500Name? = null): X509Certificate {
        val builder = createPartialCertificate(certificateType, issuer, issuerPublicKey, subject, subjectPublicKey, validityWindow, nameConstraints, crlDistPoint, crlIssuer)
        return builder.build(issuerSigner).run {
            require(isValidOn(Date())){"Certificate is not valid at instant now"}
            toJca()
        }
    }

    /**
     * Build and sign an X.509 certificate with CA cert private key.
     *
     * @param certificateType type of the certificate.
     * @param issuer name of the issuing entity.
     * @param issuerKeyPair the public & private key to sign the certificate with.
     * @param subject name of the certificate subject.
     * @param subjectPublicKey public key of the certificate subject.
     * @param validityWindow the time period the certificate is valid for.
     * @param nameConstraints any name constraints to impose on certificates signed by the generated certificate.
     */
    fun createCertificate(certificateType: CertificateType,
                          issuer: X500Principal,
                          issuerKeyPair: KeyPair,
                          subject: X500Principal,
                          subjectPublicKey: PublicKey,
                          validityWindow: Pair<Date, Date>,
                          nameConstraints: NameConstraints? = null,
                          crlDistPoint: String? = null,
                          crlIssuer: X500Name? = null): X509Certificate {
        val signatureScheme = Crypto.findSignatureScheme(issuerKeyPair.private)
        val provider = Crypto.findProvider(signatureScheme.providerName)
        val signer = ContentSignerBuilder.build(signatureScheme, issuerKeyPair.private, provider)
        val builder = createPartialCertificate(
                certificateType,
                issuer,
                issuerKeyPair.public,
                subject, subjectPublicKey,
                validityWindow,
                nameConstraints,
                crlDistPoint,
                crlIssuer)
        return builder.build(signer).run {
            require(isValidOn(Date())){"Certificate is not valid at instant now"}
            var usePublicKey: PublicKey = issuerKeyPair.public
            if (issuerKeyPair.public is EdDSAPublicKey) {
                usePublicKey = edDSAPublicKeyConverterKeyFactory.generatePublic(X509EncodedKeySpec(issuerKeyPair.public.encoded))
            }

            require(isSignatureValid(JcaContentVerifierProviderBuilder().build(usePublicKey))){"Invalid signature"}
            toJca()
        }
    }

    /**
     * Create certificate signing request using provided information.
     */
    fun createCertificateSigningRequest(subject: X500Principal, email: String, publicKey: PublicKey, contentSigner: ContentSigner, certRole: CertRole = CertRole.NODE_CA): PKCS10CertificationRequest {
        return JcaPKCS10CertificationRequestBuilder(subject, publicKey)
                .addAttribute(BCStyle.E, DERUTF8String(email))
                .addAttribute(ASN1ObjectIdentifier(CordaOID.X509_EXTENSION_CORDA_ROLE), certRole)
                .build(contentSigner).apply {
                    if (!isSignatureValid()) {
                        throw SignatureException("The certificate signing request signature validation failed.")
                    }
                }
    }

    fun createCertificateSigningRequest(subject: X500Principal, email: String, keyPair: KeyPair, certRole: CertRole = CertRole.NODE_CA): PKCS10CertificationRequest {
        val signatureScheme = Crypto.findSignatureScheme(keyPair.public)
        val signer = ContentSignerBuilder.build(signatureScheme, keyPair.private, Crypto.findProvider(signatureScheme.providerName))
        return createCertificateSigningRequest(subject, email, keyPair.public, signer, certRole)
    }

    fun buildCertPath(first: X509Certificate, remaining: List<X509Certificate>): CertPath {
        val certificates = ArrayList<X509Certificate>(1 + remaining.size)
        certificates += first
        certificates += remaining
        return buildCertPath(certificates)
    }

    fun buildCertPath(vararg certificates: X509Certificate): CertPath {
        return X509CertificateFactory().generateCertPath(*certificates)
    }

    fun buildCertPath(certificates: List<X509Certificate>): CertPath {
        return X509CertificateFactory().generateCertPath(certificates)
    }

    private fun addCrlInfo(builder: X509v3CertificateBuilder, crlDistPoint: String?, crlIssuer: X500Name?) {
        if (crlDistPoint != null) {
            val distPointName = DistributionPointName(GeneralNames(GeneralName(GeneralName.uniformResourceIdentifier, crlDistPoint)))
            val crlIssuerGeneralNames = crlIssuer?.let {
                GeneralNames(GeneralName(crlIssuer))
            }
            // The second argument is flag that allows you to define what reason of certificate revocation is served by this distribution point see [ReasonFlags].
            // The idea is that you have different revocation per revocation reason. Since we won't go into such a granularity, we can skip that parameter.
            // The third argument allows you to specify the name of the CRL issuer, it needs to be consistent with the crl (IssuingDistributionPoint) extension and the idp argument.
            // If idp == true, set it, if idp == false, leave it null as done here.
            val distPoint = DistributionPoint(distPointName, null, crlIssuerGeneralNames)
            builder.addExtension(Extension.cRLDistributionPoints, false, CRLDistPoint(arrayOf(distPoint)))
        }
    }

    @Suppress("MagicNumber")
    private fun generateCertificateSerialNumber(): BigInteger {
        val bytes = ByteArray(CERTIFICATE_SERIAL_NUMBER_LENGTH)
        newSecureRandom().nextBytes(bytes)
        // Set highest byte to 01xxxxxx to ensure positive sign and constant bit length.
        bytes[0] = bytes[0].and(0x3F).or(0x40)
        return BigInteger(bytes)
    }
}

// Assuming cert type to role is 1:1
val CertRole.certificateType: CertificateType get() = CertificateType.values().first { it.role == this }

/**
 * Convert a [X509Certificate] into BouncyCastle's [X509CertificateHolder].
 *
 * NOTE: To avoid unnecessary copying use [X509Certificate] where possible.
 */
fun X509Certificate.toBc() = X509CertificateHolder(encoded)

fun X509CertificateHolder.toJca(): X509Certificate = X509CertificateFactory().generateCertificate(encoded.inputStream())

val CertPath.x509Certificates: List<X509Certificate>
    get() {
        require(type == "X.509") { "Not an X.509 cert path: $this" }
        // We're not mapping the list to avoid creating a new one.
        return uncheckedCast(certificates)
    }

val Certificate.x509: X509Certificate get() = requireNotNull(this as? X509Certificate) { "Not an X.509 certificate: $this" }

val Array<Certificate>.x509: List<X509Certificate> get() = map { it.x509 }

/**
 * Validates the signature of the CSR.
 */
fun PKCS10CertificationRequest.isSignatureValid(): Boolean {
    return this.isSignatureValid(JcaContentVerifierProviderBuilder().build(this.subjectPublicKeyInfo))
}

/**
 * Check certificate validity or print warning if expiry is within 30 days
 */
fun X509Certificate.checkValidity(errorMessage: () -> Any, warningBlock: (daysToExpiry: Int) -> Unit, date: Date = Date()) {
    try {
        checkValidity(date)
    }
    catch (e: CertificateException) {
        throw IllegalArgumentException(errorMessage().toString(), e)
    }
    // Number of full days until midnight of expiry date: today is not included
    val daysToExpiry = ChronoUnit.DAYS.between(date.toInstant(), notAfter.toInstant()).toInt()
    if (daysToExpiry < X509Utilities.TLS_CERTIFICATE_DAYS_TO_EXPIRY_WARNING_THRESHOLD) {
        // Also include today, e.g. return 1 for tomorrow expiry
        warningBlock(daysToExpiry + 1)
    }
}

/**
 * Wraps a [CertificateFactory] to remove boilerplate. It's unclear whether [CertificateFactory] is threadsafe so best
 * so assume this class is not.
 */
class X509CertificateFactory {
    val delegate: CertificateFactory = CertificateFactory.getInstance("X.509")

    fun generateCertificate(input: InputStream): X509Certificate = delegate.generateCertificate(input).x509

    fun generateCertPath(vararg certificates: X509Certificate): CertPath = generateCertPath(certificates.asList())

    fun generateCertPath(certificates: List<X509Certificate>): CertPath = delegate.generateCertPath(certificates)
}

enum class CertificateType(val keyUsage: KeyUsage, vararg val purposes: KeyPurposeId, val isCA: Boolean, val role: CertRole?) {
    ROOT_CA(
            KeyUsage(KeyUsage.digitalSignature or KeyUsage.keyCertSign or KeyUsage.cRLSign),
            isCA = true,
            role = null
    ),

    INTERMEDIATE_CA(
            KeyUsage(KeyUsage.digitalSignature or KeyUsage.keyCertSign or KeyUsage.cRLSign),
            KeyPurposeId.id_kp_clientAuth,
            isCA = true,
            role = CertRole.DOORMAN_CA
    ),

    NETWORK_MAP(
            KeyUsage(KeyUsage.digitalSignature),
            KeyPurposeId.id_kp_clientAuth,
            isCA = false,
            role = CertRole.NETWORK_MAP
    ),

    SERVICE_IDENTITY(
            KeyUsage(KeyUsage.digitalSignature),
            KeyPurposeId.id_kp_clientAuth,
            isCA = false,
            role = CertRole.SERVICE_IDENTITY
    ),

    NODE_CA(
            KeyUsage(KeyUsage.digitalSignature or KeyUsage.keyCertSign or KeyUsage.cRLSign),
            KeyPurposeId.id_kp_clientAuth,
            isCA = true,
            role = CertRole.NODE_CA
    ),

    TLS(
            KeyUsage(KeyUsage.digitalSignature or KeyUsage.keyEncipherment or KeyUsage.keyAgreement),
            KeyPurposeId.id_kp_serverAuth,
            KeyPurposeId.id_kp_clientAuth,
            isCA = false,
            role = CertRole.TLS
    ),

    // TODO: Identity certs should have tight name constraints on child certificates
    LEGAL_IDENTITY(
            KeyUsage(KeyUsage.digitalSignature or KeyUsage.keyCertSign),
            KeyPurposeId.id_kp_clientAuth,
            isCA = true,
            role = CertRole.LEGAL_IDENTITY
    ),

    CONFIDENTIAL_LEGAL_IDENTITY(
            KeyUsage(KeyUsage.digitalSignature),
            KeyPurposeId.id_kp_clientAuth,
            isCA = false,
            role = CertRole.CONFIDENTIAL_LEGAL_IDENTITY
    ),

    NETWORK_PARAMETERS(
            KeyUsage(KeyUsage.digitalSignature),
            KeyPurposeId.id_kp_clientAuth,
            isCA = false,
            role = CertRole.NETWORK_PARAMETERS
    )
}

data class CertificateAndKeyPair(val certificate: X509Certificate, val keyPair: KeyPair) {
    fun <T : Any> sign(obj: T): SignedDataWithCert<T> = obj.signWithCert(keyPair.private, certificate)
}
