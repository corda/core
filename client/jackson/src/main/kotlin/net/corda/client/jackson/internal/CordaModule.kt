@file:Suppress("DEPRECATION")

package net.corda.client.jackson.internal

import com.fasterxml.jackson.annotation.*
import com.fasterxml.jackson.annotation.JsonAutoDetect.Value
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.JsonCreator.Mode.DISABLED
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.*
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.databind.cfg.MapperConfig
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier
import com.fasterxml.jackson.databind.deser.ContextualDeserializer
import com.fasterxml.jackson.databind.deser.std.DelegatingDeserializer
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer
import com.fasterxml.jackson.databind.introspect.AnnotatedClass
import com.fasterxml.jackson.databind.introspect.BasicClassIntrospector
import com.fasterxml.jackson.databind.introspect.POJOPropertiesCollector
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.node.IntNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.ser.BeanPropertyWriter
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier
import com.fasterxml.jackson.databind.ser.std.StdScalarSerializer
import com.fasterxml.jackson.databind.ser.std.UUIDSerializer
import com.google.common.primitives.Booleans
import net.corda.client.jackson.JacksonSupport
import net.corda.core.contracts.*
import net.corda.core.crypto.*
import net.corda.core.crypto.PartialMerkleTree.PartialTree
import net.corda.core.crypto.SecureHash.Companion.SHA2_256
import net.corda.core.flows.StateMachineRunId
import net.corda.core.identity.*
import net.corda.core.internal.DigitalSignatureWithCert
import net.corda.core.internal.createComponentGroups
import net.corda.core.node.NodeInfo
import net.corda.core.serialization.SerializeAsToken
import net.corda.core.serialization.SerializedBytes
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.serialize
import net.corda.core.transactions.*
import net.corda.core.utilities.ByteSequence
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.core.utilities.parseAsHex
import net.corda.core.utilities.toHexString
import net.corda.serialization.internal.AllWhitelist
import net.corda.serialization.internal.amqp.SerializerFactoryBuilder
import net.corda.serialization.internal.amqp.hasCordaSerializable
import net.corda.serialization.internal.amqp.registerCustomSerializers
import java.math.BigDecimal
import java.security.PublicKey
import java.security.cert.CertPath
import java.time.Instant
import java.util.*

class CordaModule : SimpleModule("corda-core") {
    override fun setupModule(context: SetupContext) {
        super.setupModule(context)

        // For classes which are annotated with CordaSerializable we want to use the same set of properties as the Corda serilasation scheme.
        // To do that we use CordaSerializableClassIntrospector to first turn on field visibility for these classes (the Jackson default is
        // private fields are not included) and then we use CordaSerializableBeanSerializerModifier to remove any extra properties that Jackson
        // might pick up.
        context.setClassIntrospector(CordaSerializableClassIntrospector(context))
        context.addBeanSerializerModifier(CordaSerializableBeanSerializerModifier())

        context.addBeanDeserializerModifier(AmountBeanDeserializerModifier())

        context.setMixInAnnotations(PartyAndCertificate::class.java, PartyAndCertificateMixin::class.java)
        context.setMixInAnnotations(NetworkHostAndPort::class.java, NetworkHostAndPortMixin::class.java)
        context.setMixInAnnotations(CordaX500Name::class.java, CordaX500NameMixin::class.java)
        context.setMixInAnnotations(Amount::class.java, AmountMixin::class.java)
        context.setMixInAnnotations(AbstractParty::class.java, AbstractPartyMixin::class.java)
        context.setMixInAnnotations(AnonymousParty::class.java, AnonymousPartyMixin::class.java)
        context.setMixInAnnotations(Party::class.java, PartyMixin::class.java)
        context.setMixInAnnotations(PublicKey::class.java, PublicKeyMixin::class.java)
        context.setMixInAnnotations(ByteSequence::class.java, ByteSequenceMixin::class.java)
        context.setMixInAnnotations(SecureHash.SHA256::class.java, SecureHashMixin::class.java)
        context.setMixInAnnotations(SecureHash.HASH::class.java, SecureHashMixin::class.java)
        context.setMixInAnnotations(SecureHash::class.java, SecureHashMixin::class.java)
        context.setMixInAnnotations(SerializedBytes::class.java, SerializedBytesMixin::class.java)
        context.setMixInAnnotations(DigitalSignature.WithKey::class.java, ByteSequenceWithPropertiesMixin::class.java)
        context.setMixInAnnotations(DigitalSignatureWithCert::class.java, ByteSequenceWithPropertiesMixin::class.java)
        context.setMixInAnnotations(TransactionSignature::class.java, ByteSequenceWithPropertiesMixin::class.java)
        context.setMixInAnnotations(SignedTransaction::class.java, SignedTransactionMixin::class.java)
        context.setMixInAnnotations(WireTransaction::class.java, WireTransactionMixin::class.java)
        context.setMixInAnnotations(TransactionState::class.java, TransactionStateMixin::class.java)
        context.setMixInAnnotations(Command::class.java, CommandMixin::class.java)
        context.setMixInAnnotations(TimeWindow::class.java, TimeWindowMixin::class.java)
        context.setMixInAnnotations(PrivacySalt::class.java, PrivacySaltMixin::class.java)
        context.setMixInAnnotations(SignatureScheme::class.java, SignatureSchemeMixin::class.java)
        context.setMixInAnnotations(SignatureMetadata::class.java, SignatureMetadataMixin::class.java)
        context.setMixInAnnotations(PartialTree::class.java, PartialTreeMixin::class.java)
        context.setMixInAnnotations(NodeInfo::class.java, NodeInfoMixin::class.java)
        context.setMixInAnnotations(StateMachineRunId::class.java, StateMachineRunIdMixin::class.java)
    }
}

private class CordaSerializableClassIntrospector(private val context: Module.SetupContext) : BasicClassIntrospector() {
    override fun constructPropertyCollector(
            config: MapperConfig<*>?,
            ac: AnnotatedClass?,
            type: JavaType,
            forSerialization: Boolean,
            mutatorPrefix: String?
    ): POJOPropertiesCollector {
        if (hasCordaSerializable(type.rawClass)) {
            // Adjust the field visibility of CordaSerializable classes on the fly as they are encountered.
            context.configOverride(type.rawClass).visibility = Value.defaultVisibility().withFieldVisibility(Visibility.ANY)
        }
        return super.constructPropertyCollector(config, ac, type, forSerialization, mutatorPrefix)
    }
}

private class CordaSerializableBeanSerializerModifier : BeanSerializerModifier() {
        // We need to pass in a SerializerFactory when scanning for properties, but don't actually do any serialisation so any will do.
    private val serializerFactory = SerializerFactoryBuilder.build(AllWhitelist, javaClass.classLoader).also {
            registerCustomSerializers(it)
    }

    override fun changeProperties(config: SerializationConfig,
                                  beanDesc: BeanDescription,
                                  beanProperties: MutableList<BeanPropertyWriter>): MutableList<BeanPropertyWriter> {
        val beanClass = beanDesc.beanClass
        if (hasCordaSerializable(beanClass) && !SerializeAsToken::class.java.isAssignableFrom(beanClass)) {
            val typeInformation = serializerFactory.getTypeInformation(beanClass)
            val propertyNames = typeInformation.propertiesOrEmptyMap.mapNotNull {
                if (it.value.isCalculated) null else it.key
            }
            beanProperties.removeIf { it.name !in propertyNames }
        }
        return beanProperties
    }
}

@ToStringSerialize
@JsonDeserialize(using = NetworkHostAndPortDeserializer::class)
private interface NetworkHostAndPortMixin

private class NetworkHostAndPortDeserializer : SimpleDeserializer<NetworkHostAndPort>({ NetworkHostAndPort.parse(text) })

@JsonSerialize(using = PartyAndCertificateSerializer::class)
// TODO Add deserialization which follows the same lookup logic as Party
private interface PartyAndCertificateMixin

private class PartyAndCertificateSerializer : JsonSerializer<PartyAndCertificate>() {
    override fun serialize(value: PartyAndCertificate, gen: JsonGenerator, serializers: SerializerProvider) {
        val mapper = gen.codec as JacksonSupport.PartyObjectMapper
        if (mapper.isFullParties) {
            gen.writeObject(PartyAndCertificateJson(value.name, value.certPath))
        } else {
            gen.writeObject(value.party)
        }
    }
}

private class PartyAndCertificateJson(val name: CordaX500Name, val certPath: CertPath)

@JsonSerialize(using = SignedTransactionSerializer::class)
@JsonDeserialize(using = SignedTransactionDeserializer::class)
private interface SignedTransactionMixin

private class SignedTransactionSerializer : JsonSerializer<SignedTransaction>() {
    override fun serialize(value: SignedTransaction, gen: JsonGenerator, serializers: SerializerProvider) {
        val core = value.coreTransaction
        val stxJson = when (core) {
            is WireTransaction -> StxJson(wire = core, signatures = value.sigs)
            is FilteredTransaction -> StxJson(filtered = core, signatures = value.sigs)
            is NotaryChangeWireTransaction -> StxJson(notaryChangeWire = core, signatures = value.sigs)
            is ContractUpgradeWireTransaction -> StxJson(contractUpgradeWire = core, signatures = value.sigs)
            is ContractUpgradeFilteredTransaction -> StxJson(contractUpgradeFiltered = core, signatures = value.sigs)
            else -> throw IllegalArgumentException("Don't know about ${core.javaClass}")
        }
        gen.writeObject(stxJson)
    }
}

private class SignedTransactionDeserializer : JsonDeserializer<SignedTransaction>() {
    override fun deserialize(parser: JsonParser, ctxt: DeserializationContext): SignedTransaction {
        val wrapper = parser.readValueAs<StxJson>()
        val core = wrapper.run { wire ?: filtered ?: notaryChangeWire ?: contractUpgradeWire ?: contractUpgradeFiltered!! }
        return SignedTransaction(core, wrapper.signatures)
    }
}

@JsonInclude(Include.NON_NULL)
private data class StxJson(
        val wire: WireTransaction? = null,
        val filtered: FilteredTransaction? = null,
        val notaryChangeWire: NotaryChangeWireTransaction? = null,
        val contractUpgradeWire: ContractUpgradeWireTransaction? = null,
        val contractUpgradeFiltered: ContractUpgradeFilteredTransaction? = null,
        val signatures: List<TransactionSignature>
) {
    init {
        val count = Booleans.countTrue(wire != null, filtered != null, notaryChangeWire != null, contractUpgradeWire != null, contractUpgradeFiltered != null)
        require(count == 1) { this }
    }
}

@JsonSerialize(using = WireTransactionSerializer::class)
@JsonDeserialize(using = WireTransactionDeserializer::class)
private interface WireTransactionMixin

private class WireTransactionSerializer : JsonSerializer<WireTransaction>() {
    override fun serialize(value: WireTransaction, gen: JsonGenerator, serializers: SerializerProvider) {
        gen.writeObject(WireTransactionJson(
                value.hashAlgorithm,
                value.id,
                value.notary,
                value.inputs,
                value.outputs,
                value.commands,
                value.timeWindow,
                value.attachments,
                value.references,
                value.privacySalt,
                value.networkParametersHash
        ))
    }
}

private class WireTransactionDeserializer : JsonDeserializer<WireTransaction>() {
    override fun deserialize(parser: JsonParser, ctxt: DeserializationContext): WireTransaction {
        val wrapper = parser.readValueAs<WireTransactionJson>()
        val componentGroups = createComponentGroups(
                wrapper.inputs,
                wrapper.outputs,
                wrapper.commands,
                wrapper.attachments,
                wrapper.notary,
                wrapper.timeWindow,
                wrapper.references,
                wrapper.networkParametersHash
        )
        return WireTransaction(componentGroups, wrapper.privacySalt, wrapper.hashAlgorithm ?: SHA2_256)
    }
}

private class WireTransactionJson(@get:JsonInclude(Include.NON_NULL) val hashAlgorithm: String?,
                                  val id: SecureHash,
                                  val notary: Party?,
                                  val inputs: List<StateRef>,
                                  val outputs: List<TransactionState<*>>,
                                  val commands: List<Command<*>>,
                                  val timeWindow: TimeWindow?,
                                  val attachments: List<SecureHash>,
                                  val references: List<StateRef>,
                                  val privacySalt: PrivacySalt,
                                  val networkParametersHash: SecureHash?)

private interface TransactionStateMixin {
    @get:JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
    val data: ContractState
    @get:JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
    val constraint: AttachmentConstraint
}

private interface CommandMixin {
    @get:JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
    val value: CommandData
}

@JsonDeserialize(using = TimeWindowDeserializer::class)
private interface TimeWindowMixin

private class TimeWindowDeserializer : JsonDeserializer<TimeWindow>() {
    override fun deserialize(parser: JsonParser, ctxt: DeserializationContext): TimeWindow {
        return parser.readValueAs<TimeWindowJson>().run {
            when {
                fromTime != null && untilTime != null -> TimeWindow.between(fromTime, untilTime)
                fromTime != null -> TimeWindow.fromOnly(fromTime)
                untilTime != null -> TimeWindow.untilOnly(untilTime)
                else -> throw JsonParseException(parser, "Neither fromTime nor untilTime exists for TimeWindow")
            }
        }
    }
}

private data class TimeWindowJson(val fromTime: Instant?, val untilTime: Instant?)

@JsonSerialize(using = PrivacySaltSerializer::class)
@JsonDeserialize(using = PrivacySaltDeserializer::class)
private interface PrivacySaltMixin

private class PrivacySaltSerializer : JsonSerializer<PrivacySalt>() {
    override fun serialize(value: PrivacySalt, gen: JsonGenerator, serializers: SerializerProvider) {
        gen.writeString(value.bytes.toHexString())
    }
}

private class PrivacySaltDeserializer : SimpleDeserializer<PrivacySalt>({ PrivacySalt(text.parseAsHex()) })

// TODO Add a lookup function by number ID in Crypto
private val signatureSchemesByNumberID = Crypto.supportedSignatureSchemes().associateBy { it.schemeNumberID }

@JsonSerialize(using = SignatureMetadataSerializer::class)
@JsonDeserialize(using = SignatureMetadataDeserializer::class)
private interface SignatureMetadataMixin

private class SignatureMetadataSerializer : JsonSerializer<SignatureMetadata>() {
    override fun serialize(value: SignatureMetadata, gen: JsonGenerator, serializers: SerializerProvider) {
        gen.jsonObject {
            writeNumberField("platformVersion", value.platformVersion)
            writeObjectField("scheme", value.schemeNumberID.let { signatureSchemesByNumberID[it] ?: it })
        }
    }
}

private class SignatureMetadataDeserializer : JsonDeserializer<SignatureMetadata>() {
    override fun deserialize(parser: JsonParser, ctxt: DeserializationContext): SignatureMetadata {
        val json = parser.readValueAsTree<ObjectNode>()
        val scheme = json["scheme"]
        val schemeNumberID = if (scheme is IntNode) {
            scheme.intValue()
        } else {
            Crypto.findSignatureScheme(scheme.textValue()).schemeNumberID
        }
        return SignatureMetadata(json["platformVersion"].intValue(), schemeNumberID)
    }
}

@JsonSerialize(using = PartialTreeSerializer::class)
@JsonDeserialize(using = PartialTreeDeserializer::class)
private interface PartialTreeMixin

private class PartialTreeSerializer : JsonSerializer<PartialTree>() {
    override fun serialize(value: PartialTree, gen: JsonGenerator, serializers: SerializerProvider) {
        gen.writeObject(convert(value))
    }

    private fun convert(tree: PartialTree): PartialTreeJson {
        return when (tree) {
            is PartialTree.IncludedLeaf -> PartialTreeJson(includedLeaf = tree.hash)
            is PartialTree.Leaf -> PartialTreeJson(leaf = tree.hash)
            is PartialTree.Node -> PartialTreeJson(left = convert(tree.left), right = convert(tree.right))
            else -> throw IllegalArgumentException("Don't know how to serialize $tree")
        }
    }
}

private class PartialTreeDeserializer : JsonDeserializer<PartialTree>() {
    override fun deserialize(parser: JsonParser, ctxt: DeserializationContext): PartialTree {
        return convert(parser.readValueAs(PartialTreeJson::class.java))
    }

    private fun convert(wrapper: PartialTreeJson): PartialTree {
        return wrapper.run {
            when {
                includedLeaf != null -> PartialTree.IncludedLeaf(includedLeaf)
                leaf != null -> PartialTree.Leaf(leaf)
                else -> PartialTree.Node(convert(left!!), convert(right!!))
            }
        }
    }
}

@JsonInclude(Include.NON_NULL)
private class PartialTreeJson(val includedLeaf: SecureHash? = null,
                              val leaf: SecureHash? = null,
                              val left: PartialTreeJson? = null,
                              val right: PartialTreeJson? = null) {
    init {
        if (includedLeaf != null) {
            require(leaf == null && left == null && right == null) { "Invalid JSON structure" }
        } else if (leaf != null) {
            require(left == null && right == null) { "Invalid JSON structure" }
        } else {
            require(left != null && right != null) { "Invalid JSON structure" }
        }
    }
}

@JsonSerialize(using = SignatureSchemeSerializer::class)
@JsonDeserialize(using = SignatureSchemeDeserializer::class)
private interface SignatureSchemeMixin

private class SignatureSchemeSerializer : JsonSerializer<SignatureScheme>() {
    override fun serialize(value: SignatureScheme, gen: JsonGenerator, serializers: SerializerProvider) {
        gen.writeString(value.schemeCodeName)
    }
}

private class SignatureSchemeDeserializer : JsonDeserializer<SignatureScheme>() {
    override fun deserialize(parser: JsonParser, ctxt: DeserializationContext): SignatureScheme {
        return if (parser.currentToken == JsonToken.VALUE_NUMBER_INT) {
            signatureSchemesByNumberID[parser.intValue] ?: throw JsonParseException(parser, "Unable to find SignatureScheme ${parser.text}")
        } else {
            Crypto.findSignatureScheme(parser.text)
        }
    }
}

@JsonSerialize(using = SerializedBytesSerializer::class)
@JsonDeserialize(using = SerializedBytesDeserializer::class)
private class SerializedBytesMixin

private class SerializedBytesSerializer : JsonSerializer<SerializedBytes<*>>() {
    override fun serialize(value: SerializedBytes<*>, gen: JsonGenerator, serializers: SerializerProvider) {
        val deserialized = value.deserialize<Any>()
        gen.jsonObject {
            writeStringField("class", deserialized.javaClass.name)
            writeObjectField("deserialized", deserialized)
        }
    }
}

private class SerializedBytesDeserializer : JsonDeserializer<SerializedBytes<*>>() {
    override fun deserialize(parser: JsonParser, context: DeserializationContext): SerializedBytes<Any> {
        return if (parser.currentToken == JsonToken.START_OBJECT) {
            val mapper = parser.codec as ObjectMapper
            val json = parser.readValueAsTree<ObjectNode>()
            val clazz = context.findClass(json["class"].textValue())
            val pojo = mapper.convertValue(json["deserialized"], clazz)
            pojo.serialize()
        } else {
            SerializedBytes(parser.binaryValue)
        }
    }
}

@JsonDeserialize(using = JacksonSupport.PartyDeserializer::class)
private interface AbstractPartyMixin

@JsonSerialize(using = JacksonSupport.AnonymousPartySerializer::class)
@JsonDeserialize(using = JacksonSupport.AnonymousPartyDeserializer::class)
private interface AnonymousPartyMixin

@JsonSerialize(using = JacksonSupport.PartySerializer::class)
private interface PartyMixin

@ToStringSerialize
@JsonDeserialize(using = JacksonSupport.CordaX500NameDeserializer::class)
private interface CordaX500NameMixin

@JsonDeserialize(using = JacksonSupport.NodeInfoDeserializer::class)
private interface NodeInfoMixin

@ToStringSerialize
@JsonDeserialize(using = JacksonSupport.SecureHashDeserializer::class)
private interface SecureHashMixin

@JsonSerialize(using = JacksonSupport.PublicKeySerializer::class)
@JsonDeserialize(using = JacksonSupport.PublicKeyDeserializer::class)
private interface PublicKeyMixin

@JsonSerialize(using = StateMachineRunIdSerializer::class)
@JsonDeserialize(using = StateMachineRunIdDeserializer::class)
private interface StateMachineRunIdMixin

private class StateMachineRunIdSerializer : StdScalarSerializer<StateMachineRunId>(StateMachineRunId::class.java) {
    private val uuidSerializer = UUIDSerializer()

    override fun isEmpty(provider: SerializerProvider?, value: StateMachineRunId): Boolean {
        return uuidSerializer.isEmpty(provider, value.uuid)
    }

    override fun serialize(value: StateMachineRunId, gen: JsonGenerator?, provider: SerializerProvider?) {
        uuidSerializer.serialize(value.uuid, gen, provider)
    }
}

private class StateMachineRunIdDeserializer : FromStringDeserializer<StateMachineRunId>(StateMachineRunId::class.java) {
    override fun _deserialize(value: String, ctxt: DeserializationContext?): StateMachineRunId {
        return StateMachineRunId(UUID.fromString(value))
    }
}

@Suppress("unused_parameter")
@ToStringSerialize
private abstract class AmountMixin @JsonCreator(mode = DISABLED) constructor(
    quantity: Long,
    displayTokenSize: BigDecimal,
    token: Any
) {
    /**
     * This mirrors the [Amount] constructor that we want Jackson to use, and
     * requires that we also tell Jackson NOT to use [Amount]'s primary constructor.
     */
    @JsonCreator constructor(
        @JsonProperty("quantity")
        quantity: Long,

        @JsonDeserialize(using = TokenDeserializer::class)
        @JsonProperty("token")
        token: Any
    ) : this(quantity, Amount.getDisplayTokenSize(token), token)
}

/**
 * Implements polymorphic deserialization for [Amount.token]. Kotlin must
 * be able to determine the concrete [Amount] type at runtime, or it will
 * fall back to using [Currency].
 */
private class TokenDeserializer(private val tokenType: Class<*>) : JsonDeserializer<Any>(), ContextualDeserializer {
    @Suppress("unused")
    constructor() : this(Currency::class.java)

    override fun deserialize(parser: JsonParser, ctxt: DeserializationContext): Any = parser.readValueAs(tokenType)

    override fun createContextual(ctxt: DeserializationContext, property: BeanProperty?): TokenDeserializer {
        if (property == null) return this
        return TokenDeserializer(property.type.rawClass.let { type ->
            if (type == Any::class.java) Currency::class.java else type
        })
    }
}

/**
 * Intercepts bean-based deserialization for the generic [Amount] type.
 */
private class AmountBeanDeserializerModifier : BeanDeserializerModifier() {
    override fun modifyDeserializer(config: DeserializationConfig, description: BeanDescription, deserializer: JsonDeserializer<*>): JsonDeserializer<*> {
        val modified = super.modifyDeserializer(config, description, deserializer)
        return if (Amount::class.java.isAssignableFrom(description.beanClass)) {
            AmountDeserializer(modified)
        } else {
            modified
        }
    }
}

private class AmountDeserializer(delegate: JsonDeserializer<*>) : DelegatingDeserializer(delegate) {
    override fun newDelegatingInstance(newDelegatee: JsonDeserializer<*>) = AmountDeserializer(newDelegatee)

    override fun deserialize(parser: JsonParser, context: DeserializationContext?): Any {
        return if (parser.currentToken() == JsonToken.VALUE_STRING) {
            /*
             * This is obviously specific to Amount<Currency>, and is here to
             * preserve the original deserializing behaviour for this case.
             */
            Amount.parseCurrency(parser.text)
        } else {
            /*
             * Otherwise continue deserializing our Bean as usual.
             */
            _delegatee.deserialize(parser, context)
        }
    }
}

@JsonDeserialize(using = JacksonSupport.OpaqueBytesDeserializer::class)
private interface ByteSequenceMixin {
    @Suppress("unused")
    @JsonValue
    fun copyBytes(): ByteArray
}

@JsonSerialize
@JsonDeserialize
private interface ByteSequenceWithPropertiesMixin {
    @Suppress("unused")
    @JsonValue(false)
    fun copyBytes(): ByteArray
}
