package net.corda.client.jackson

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.*
import com.fasterxml.jackson.databind.*
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier
import com.fasterxml.jackson.databind.deser.std.NumberDeserializers
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import net.corda.client.jackson.internal.CordaModule
import net.corda.client.jackson.internal.ToStringSerialize
import net.corda.client.jackson.internal.jsonObject
import net.corda.client.jackson.internal.readValueAs
import net.corda.core.CordaInternal
import net.corda.core.CordaOID
import net.corda.core.DoNotImplement
import net.corda.core.contracts.Amount
import net.corda.core.contracts.ContractState
import net.corda.core.contracts.StateRef
import net.corda.core.crypto.*
import net.corda.core.identity.*
import net.corda.core.internal.*
import net.corda.core.messaging.CordaRPCOps
import net.corda.core.node.NodeInfo
import net.corda.core.node.services.IdentityService
import net.corda.core.serialization.SerializedBytes
import net.corda.core.serialization.serialize
import net.corda.core.transactions.CoreTransaction
import net.corda.core.transactions.NotaryChangeWireTransaction
import net.corda.core.transactions.WireTransaction
import net.corda.core.utilities.OpaqueBytes
import net.corda.core.utilities.parsePublicKeyBase58
import net.corda.core.utilities.toBase58String
import org.bouncycastle.asn1.x509.KeyPurposeId
import java.math.BigDecimal
import java.nio.charset.StandardCharsets.UTF_8
import java.security.PublicKey
import java.security.cert.CertPath
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.util.*
import javax.security.auth.x500.X500Principal

/**
 * Utilities and serialisers for working with JSON representations of basic types. This adds Jackson support for
 * the java.time API, some core types, and Kotlin data classes.
 *
 * Note that Jackson can also be used to serialise/deserialise other formats such as Yaml and XML.
 */
@Suppress("DEPRECATION", "MemberVisibilityCanBePrivate")
object JacksonSupport {
    // If you change this API please update the docs in the docsite (json.rst)

    @DoNotImplement
    interface PartyObjectMapper {
        val isFullParties: Boolean
        fun wellKnownPartyFromX500Name(name: CordaX500Name): Party?
        fun partyFromKey(owningKey: PublicKey): Party?
        fun partiesFromName(query: String): Set<Party>
        fun nodeInfoFromParty(party: AbstractParty): NodeInfo?
    }

    @Deprecated("This is an internal class, do not use", replaceWith = ReplaceWith("JacksonSupport.createDefaultMapper"))
    class RpcObjectMapper
    @JvmOverloads constructor(val rpc: CordaRPCOps,
                              factory: JsonFactory,
                              val fuzzyIdentityMatch: Boolean,
                              override val isFullParties: Boolean = false) : PartyObjectMapper, ObjectMapper(factory) {
        override fun wellKnownPartyFromX500Name(name: CordaX500Name): Party? = rpc.wellKnownPartyFromX500Name(name)
        override fun partyFromKey(owningKey: PublicKey): Party? = rpc.partyFromKey(owningKey)
        // Second parameter is exactMatch, so we have to invert the meaning here.
        override fun partiesFromName(query: String) = rpc.partiesFromName(query, !fuzzyIdentityMatch)
        override fun nodeInfoFromParty(party: AbstractParty): NodeInfo? = rpc.nodeInfoFromParty(party)
    }

    @Deprecated("This is an internal class, do not use")
    class IdentityObjectMapper
    @JvmOverloads constructor(val identityService: IdentityService,
                              factory: JsonFactory,
                              val fuzzyIdentityMatch: Boolean,
                              override val isFullParties: Boolean = false) : PartyObjectMapper, ObjectMapper(factory) {
        override fun wellKnownPartyFromX500Name(name: CordaX500Name): Party? = identityService.wellKnownPartyFromX500Name(name)
        override fun partyFromKey(owningKey: PublicKey): Party? = identityService.partyFromKey(owningKey)
        override fun partiesFromName(query: String) = identityService.partiesFromName(query, fuzzyIdentityMatch)
        override fun nodeInfoFromParty(party: AbstractParty): NodeInfo? = null
    }

    @Deprecated("This is an internal class, do not use", replaceWith = ReplaceWith("JacksonSupport.createNonRpcMapper"))
    class NoPartyObjectMapper
    @JvmOverloads constructor(factory: JsonFactory,
                              override val isFullParties: Boolean = false) : PartyObjectMapper, ObjectMapper(factory) {
        override fun wellKnownPartyFromX500Name(name: CordaX500Name): Party? = null
        override fun partyFromKey(owningKey: PublicKey): Party? = null
        override fun partiesFromName(query: String): Set<Party> = emptySet()
        override fun nodeInfoFromParty(party: AbstractParty): NodeInfo? = null
    }

    @Suppress("unused")
    @Deprecated("Do not use this as it's not thread safe. Instead get a ObjectMapper instance with one of the create*Mapper methods.")
    val cordaModule: Module by lazy(::CordaModule)

    /**
     * Creates a Jackson ObjectMapper that uses RPC to deserialise parties from string names.
     *
     * @param fuzzyIdentityMatch If false, fields mapped to [Party] objects must be in X.500 name form and precisely
     * match an identity known from the network map. If true, the name is matched more leniently but if the match
     * is ambiguous a [JsonParseException] is thrown.
     *
     * @param fullParties If true then [Party] objects will be serialised as JSON objects, with the owning key serialised
     * in addition to the name. For [PartyAndCertificate] objects the cert path will be included.
     */
    @JvmStatic
    @JvmOverloads
    fun createDefaultMapper(rpc: CordaRPCOps,
                            factory: JsonFactory = JsonFactory(),
                            fuzzyIdentityMatch: Boolean = false,
                            fullParties: Boolean = false): ObjectMapper {
        return configureMapper(RpcObjectMapper(rpc, factory, fuzzyIdentityMatch, fullParties))
    }

    /**
     * For testing or situations where deserialising parties is not required
     *
     * @param fullParties If true then [Party] objects will be serialised as JSON objects, with the owning key serialised
     * in addition to the name. For [PartyAndCertificate] objects the cert path will be included.
     */
    @JvmStatic
    @JvmOverloads
    fun createNonRpcMapper(factory: JsonFactory = JsonFactory(), fullParties: Boolean = false): ObjectMapper {
        return configureMapper(NoPartyObjectMapper(factory, fullParties))
    }

    /**
     * Creates a Jackson ObjectMapper that uses an [IdentityService] directly inside the node to deserialise parties from string names.
     *
     * @param fuzzyIdentityMatch If false, fields mapped to [Party] objects must be in X.500 name form and precisely
     * match an identity known from the network map. If true, the name is matched more leniently but if the match
     * is ambiguous a [JsonParseException] is thrown.
     *
     * @param fullParties If true then [Party] objects will be serialised as JSON objects, with the owning key serialised
     * in addition to the name. For [PartyAndCertificate] objects the cert path will be included.
     */
    @JvmStatic
    @JvmOverloads
    fun createInMemoryMapper(identityService: IdentityService,
                             factory: JsonFactory = JsonFactory(),
                             fuzzyIdentityMatch: Boolean = false,
                             fullParties: Boolean = false): ObjectMapper {
        return configureMapper(IdentityObjectMapper(identityService, factory, fuzzyIdentityMatch, fullParties))
    }

    @CordaInternal
    @VisibleForTesting
    internal fun createPartyObjectMapper(partyObjectMapper: PartyObjectMapper, factory: JsonFactory = JsonFactory()): ObjectMapper {
        val mapper = object : ObjectMapper(factory), PartyObjectMapper by partyObjectMapper {}
        return configureMapper(mapper)
    }

    private fun configureMapper(mapper: ObjectMapper): ObjectMapper {
        return mapper.apply {
            enable(SerializationFeature.INDENT_OUTPUT)
            enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
            enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

            registerModule(JavaTimeModule().apply {
                addSerializer(Date::class.java, DateSerializer)
            })
            registerModule(CordaModule())
            registerModule(KotlinModule().apply {
                setDeserializerModifier(KotlinObjectDeserializerModifier)
            })

            addMixIn(BigDecimal::class.java, BigDecimalMixin::class.java)
            addMixIn(X500Principal::class.java, X500PrincipalMixin::class.java)
            addMixIn(X509Certificate::class.java, X509CertificateMixin::class.java)
            addMixIn(CertPath::class.java, CertPathMixin::class.java)
        }
    }

    private object KotlinObjectDeserializerModifier : BeanDeserializerModifier() {
        override fun modifyDeserializer(config: DeserializationConfig,
                                        beanDesc: BeanDescription,
                                        deserializer: JsonDeserializer<*>): JsonDeserializer<*> {
            val objectInstance = beanDesc.beanClass.kotlinObjectInstance
            return if (objectInstance != null) KotlinObjectDeserializer(objectInstance) else deserializer
        }
    }

    private class KotlinObjectDeserializer<T>(private val objectInstance: T) : JsonDeserializer<T>() {
        override fun deserialize(parser: JsonParser, ctxt: DeserializationContext): T = objectInstance
    }

    @ToStringSerialize
    @JsonDeserialize(using = NumberDeserializers.BigDecimalDeserializer::class)
    private interface BigDecimalMixin

    private object DateSerializer : JsonSerializer<Date>() {
        override fun serialize(value: Date, gen: JsonGenerator, serializers: SerializerProvider) {
            gen.writeObject(value.toInstant())
        }
    }

    @ToStringSerialize
    private interface X500PrincipalMixin

    @JsonSerialize(using = X509CertificateSerializer::class)
    @JsonDeserialize(using = X509CertificateDeserializer::class)
    private interface X509CertificateMixin

    private object X509CertificateSerializer : JsonSerializer<X509Certificate>() {
        val keyUsages = arrayOf(
                "digitalSignature",
                "nonRepudiation",
                "keyEncipherment",
                "dataEncipherment",
                "keyAgreement",
                "keyCertSign",
                "cRLSign",
                "encipherOnly",
                "decipherOnly"
        )

        val keyPurposeIds = KeyPurposeId::class.java
                .fields
                .filter { it.isStatic && it.type == KeyPurposeId::class.java }
                .associateBy({ (it.get(null) as KeyPurposeId).id }, { it.name })

        val knownExtensions = setOf(
                "2.5.29.15",
                "2.5.29.17",
                "2.5.29.18",
                "2.5.29.19",
                "2.5.29.37",
                CordaOID.X509_EXTENSION_CORDA_ROLE
        )

        override fun serialize(value: X509Certificate, gen: JsonGenerator, serializers: SerializerProvider) {
            gen.jsonObject {
                writeNumberField("version", value.version)
                writeObjectField("serialNumber", value.serialNumber)
                writeObjectField("subject", value.subjectX500Principal)
                writeObjectField("publicKey", value.publicKey)
                writeObjectField("issuer", value.issuerX500Principal)
                writeObjectField("notBefore", value.notBefore)
                writeObjectField("notAfter", value.notAfter)
                writeObjectField("cordaCertRole", CertRole.extract(value))
                writeObjectField("issuerUniqueID", value.issuerUniqueID)
                writeObjectField("subjectUniqueID", value.subjectUniqueID)
                writeObjectField("keyUsage", value.keyUsage?.asList()?.mapIndexedNotNull { i, flag -> if (flag) keyUsages[i] else null })
                writeObjectField("extendedKeyUsage", value.extendedKeyUsage?.map { keyPurposeIds[it] ?: it })
                jsonObject("basicConstraints") {
                    val isCa = value.basicConstraints != -1
                    writeBooleanField("isCA", isCa)
                    if (isCa) {
                        writeObjectField("pathLength", value.basicConstraints.let { if (it != Int.MAX_VALUE) it else null })
                    }
                }
                writeObjectField("subjectAlternativeNames", value.subjectAlternativeNames)
                writeObjectField("issuerAlternativeNames", value.issuerAlternativeNames)
                writeObjectField("otherCriticalExtensions", value.criticalExtensionOIDs - knownExtensions)
                writeObjectField("otherNonCriticalExtensions", value.nonCriticalExtensionOIDs - knownExtensions)
                writeBinaryField("encoded", value.encoded)
            }
        }
    }

    private class X509CertificateDeserializer : JsonDeserializer<X509Certificate>() {
        private val certFactory = CertificateFactory.getInstance("X.509")
        override fun deserialize(parser: JsonParser, ctxt: DeserializationContext): X509Certificate {
            val encoded = if (parser.currentToken == JsonToken.START_OBJECT) {
                parser.readValueAsTree<ObjectNode>()["encoded"].binaryValue()
            } else {
                parser.binaryValue
            }
            return certFactory.generateCertificate(encoded.inputStream()) as X509Certificate
        }
    }

    @JsonSerialize(using = CertPathSerializer::class)
    @JsonDeserialize(using = CertPathDeserializer::class)
    private interface CertPathMixin

    private class CertPathSerializer : JsonSerializer<CertPath>() {
        override fun serialize(value: CertPath, gen: JsonGenerator, serializers: SerializerProvider) {
            gen.writeObject(CertPathWrapper(value.type, uncheckedCast(value.certificates)))
        }
    }

    private class CertPathDeserializer : JsonDeserializer<CertPath>() {
        private val certFactory = CertificateFactory.getInstance("X.509")
        override fun deserialize(parser: JsonParser, ctxt: DeserializationContext): CertPath {
            val wrapper = parser.readValueAs<CertPathWrapper>()
            return certFactory.generateCertPath(wrapper.certificates)
        }
    }

    private data class CertPathWrapper(val type: String, val certificates: List<X509Certificate>) {
        init {
            require(type == "X.509") { "Only X.509 cert paths are supported" }
        }
    }

    @Deprecated("This is an internal class, do not use")
    object AnonymousPartySerializer : JsonSerializer<AnonymousParty>() {
        override fun serialize(value: AnonymousParty, generator: JsonGenerator, provider: SerializerProvider) {
            generator.writeObject(value.owningKey)
        }
    }

    @Deprecated("This is an internal class, do not use")
    object AnonymousPartyDeserializer : JsonDeserializer<AnonymousParty>() {
        override fun deserialize(parser: JsonParser, context: DeserializationContext): AnonymousParty {
            return AnonymousParty(parser.readValueAs(PublicKey::class.java))
        }
    }

    @Deprecated("This is an internal class, do not use")
    object PartySerializer : JsonSerializer<Party>() {
        override fun serialize(value: Party, gen: JsonGenerator, provider: SerializerProvider) {
            val mapper = gen.codec as PartyObjectMapper
            if (mapper.isFullParties) {
                gen.writeObject(PartyAnalogue(value.name, value.owningKey))
            } else {
                gen.writeObject(value.name)
            }
        }
    }

    @Deprecated("This is an internal class, do not use")
    object PartyDeserializer : JsonDeserializer<Party>() {
        override fun deserialize(parser: JsonParser, context: DeserializationContext): Party {
            val mapper = parser.codec as PartyObjectMapper
            return if (parser.currentToken == JsonToken.START_OBJECT) {
                val analogue = parser.readValueAs<PartyAnalogue>()
                Party(analogue.name, analogue.owningKey)
            } else {
                // The comma character is invalid in Base58, and required as a separator for X.500 names. As Corda
                // X.500 names all involve at least three attributes (organisation, locality, country), they must
                // include a comma. As such we can use it as a distinguisher between the two types.
                if ("," in parser.text) {
                    val principal = CordaX500Name.parse(parser.text)
                    mapper.wellKnownPartyFromX500Name(principal) ?: throw JsonParseException(parser, "Could not find a Party with name $principal")
                } else {
                    lookupByNameSegment(mapper, parser)
                }
            }
        }

        private fun lookupByNameSegment(mapper: PartyObjectMapper, parser: JsonParser): Party {
            val nameMatches = mapper.partiesFromName(parser.text)
            return when {
                nameMatches.isEmpty() -> {
                    val publicKey = try {
                        parser.readValueAs<PublicKey>()
                    } catch (e: Exception) {
                        throw JsonParseException(parser, "No matching Party found, then tried to directly deserialise ${parser.text} as a PublicKey with no success", e)
                    }
                    mapper.partyFromKey(publicKey)
                            ?: throw JsonParseException(parser, "Could not find a Party with key ${publicKey.toStringShort()}")
                }
                nameMatches.size == 1 -> nameMatches.first()
                else -> throw JsonParseException(parser, "Ambiguous name match '${parser.text}': could be any of " +
                        nameMatches.map { it.name }.joinToString(" ... or ... "))
            }
        }
    }

    private class PartyAnalogue(val name: CordaX500Name, val owningKey: PublicKey)

    @Deprecated("This is an internal class, do not use")
    object CordaX500NameDeserializer : JsonDeserializer<CordaX500Name>() {
        override fun deserialize(parser: JsonParser, context: DeserializationContext): CordaX500Name {
            return try {
                CordaX500Name.parse(parser.text)
            } catch (e: IllegalArgumentException) {
                throw JsonParseException(parser, "Invalid Corda X.500 name ${parser.text}: ${e.message}", e)
            }
        }
    }

    @Deprecated("This is an internal class, do not use")
    object NodeInfoDeserializer : JsonDeserializer<NodeInfo>() {
        override fun deserialize(parser: JsonParser, context: DeserializationContext): NodeInfo {
            val mapper = parser.codec as PartyObjectMapper
            val party = parser.readValueAs<AbstractParty>()
            return mapper.nodeInfoFromParty(party) ?: throw JsonParseException(parser, "Cannot find node with $party")
        }
    }

    @Deprecated("This is an internal class, do not use")
    class SecureHashDeserializer<T : SecureHash> : JsonDeserializer<T>() {
        override fun deserialize(parser: JsonParser, context: DeserializationContext): T {
            try {
                return uncheckedCast(SecureHash.create(parser.text))
            } catch (e: Exception) {
                throw JsonParseException(parser, "Invalid hash ${parser.text}: ${e.message}")
            }
        }
    }

    @Deprecated("This is an internal class, do not use")
    object PublicKeySerializer : JsonSerializer<PublicKey>() {
        override fun serialize(value: PublicKey, generator: JsonGenerator, provider: SerializerProvider) {
            generator.writeString(value.toBase58String())
        }
    }

    @Deprecated("This is an internal class, do not use")
    object PublicKeyDeserializer : JsonDeserializer<PublicKey>() {
        override fun deserialize(parser: JsonParser, context: DeserializationContext): PublicKey {
            return try {
                parsePublicKeyBase58(parser.text)
            } catch (e: Exception) {
                throw JsonParseException(parser, "Invalid public key ${parser.text}: ${e.message}")
            }
        }
    }

    @Deprecated("Do not use - Replaced by Corda's internal AmountDeserializer and TokenDeserializer classes")
    object AmountDeserializer : JsonDeserializer<Amount<*>>() {
        override fun deserialize(parser: JsonParser, context: DeserializationContext): Amount<*> {
            return if (parser.currentToken() == JsonToken.VALUE_STRING) {
                Amount.parseCurrency(parser.text)
            } else {
                val wrapper = parser.readValueAs<CurrencyAmountWrapper>()
                Amount(wrapper.quantity, wrapper.token)
            }
        }
    }

    private data class CurrencyAmountWrapper(val quantity: Long, val token: Currency)

    @Deprecated("This is an internal class, do not use")
    object OpaqueBytesDeserializer : JsonDeserializer<OpaqueBytes>() {
        override fun deserialize(parser: JsonParser, ctxt: DeserializationContext): OpaqueBytes {
            return OpaqueBytes(parser.text?.toByteArray(UTF_8) ?: parser.binaryValue)
        }
    }


    //
    // Everything below this point is no longer used but can't be deleted as they leaked into the public API
    //

    @Deprecated("No longer used as jackson already has a toString serializer",
            replaceWith = ReplaceWith("com.fasterxml.jackson.databind.ser.std.ToStringSerializer.instance"))
    object ToStringSerializer : JsonSerializer<Any>() {
        override fun serialize(obj: Any, generator: JsonGenerator, provider: SerializerProvider) {
            generator.writeString(obj.toString())
        }
    }

    @Deprecated("This is an internal class, do not use")
    object CordaX500NameSerializer : JsonSerializer<CordaX500Name>() {
        override fun serialize(obj: CordaX500Name, generator: JsonGenerator, provider: SerializerProvider) {
            generator.writeString(obj.toString())
        }
    }

    @Deprecated("This is an internal class, do not use")
    object NodeInfoSerializer : JsonSerializer<NodeInfo>() {
        override fun serialize(value: NodeInfo, gen: JsonGenerator, serializers: SerializerProvider) {
            gen.writeString(Base58.encode(value.serialize().bytes))
        }
    }

    @Deprecated("This is an internal class, do not use")
    object SecureHashSerializer : JsonSerializer<SecureHash>() {
        override fun serialize(obj: SecureHash, generator: JsonGenerator, provider: SerializerProvider) {
            generator.writeString(obj.toString())
        }
    }

    @Deprecated("This is an internal class, do not use")
    object AmountSerializer : JsonSerializer<Amount<*>>() {
        override fun serialize(value: Amount<*>, gen: JsonGenerator, serializers: SerializerProvider) {
            gen.writeString(value.toString())
        }
    }

    @Deprecated("This is an internal class, do not use")
    object OpaqueBytesSerializer : JsonSerializer<OpaqueBytes>() {
        override fun serialize(value: OpaqueBytes, gen: JsonGenerator, serializers: SerializerProvider) {
            gen.writeBinary(value.bytes)
        }
    }

    @Deprecated("This is an internal class, do not use")
    @Suppress("unused")
    abstract class SignedTransactionMixin {
        @JsonIgnore abstract fun getTxBits(): SerializedBytes<CoreTransaction>
        @JsonProperty("signatures") protected abstract fun getSigs(): List<TransactionSignature>
        @JsonProperty protected abstract fun getTransaction(): CoreTransaction
        @JsonIgnore abstract fun getTx(): WireTransaction
        @JsonIgnore abstract fun getNotaryChangeTx(): NotaryChangeWireTransaction
        @JsonIgnore abstract fun getInputs(): List<StateRef>
        @JsonIgnore abstract fun getNotary(): Party?
        @JsonIgnore abstract fun getId(): SecureHash
        @JsonIgnore abstract fun getRequiredSigningKeys(): Set<PublicKey>
    }

    @Deprecated("This is an internal class, do not use")
    @Suppress("unused")
    abstract class WireTransactionMixin {
        @JsonIgnore abstract fun getMerkleTree(): MerkleTree
        @JsonIgnore abstract fun getAvailableComponents(): List<Any>
        @JsonIgnore abstract fun getAvailableComponentHashes(): List<SecureHash>
        @JsonIgnore abstract fun getOutputStates(): List<ContractState>
    }
}
