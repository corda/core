@file:Suppress("ThrowsCount", "ComplexMethod")
package net.corda.core.transactions

import co.paralleluniverse.strands.Strand
import net.corda.core.CordaInternal
import net.corda.core.DeleteForDJVM
import net.corda.core.contracts.*
import net.corda.core.crypto.CompositeKey
import net.corda.core.crypto.SecureHash.Companion.SHA2_256
import net.corda.core.crypto.SignableData
import net.corda.core.crypto.SignatureMetadata
import net.corda.core.identity.Party
import net.corda.core.internal.*
import net.corda.core.node.NetworkParameters
import net.corda.core.node.ServiceHub
import net.corda.core.node.ServicesForResolution
import net.corda.core.node.ZoneVersionTooLowException
import net.corda.core.node.services.AttachmentId
import net.corda.core.node.services.KeyManagementService
import net.corda.core.serialization.SerializationContext
import net.corda.core.serialization.SerializationFactory
import net.corda.core.utilities.contextLogger
import java.security.PublicKey
import java.time.Duration
import java.time.Instant
import java.util.ArrayDeque
import java.util.UUID
import java.util.regex.Pattern
import kotlin.collections.ArrayList
import kotlin.collections.component1
import kotlin.collections.component2
import kotlin.reflect.KClass

/**
 * A TransactionBuilder is a transaction class that's mutable (unlike the others which are all immutable). It is
 * intended to be passed around contracts that may edit it by adding new states/commands. Then once the states
 * and commands are right, this class can be used as a holding bucket to gather signatures from multiple parties.
 *
 * The builder can be customised for specific transaction types, e.g. where additional processing is needed
 * before adding a state/command.
 *
 * @param notary Notary used for the transaction. If null, this indicates the transaction DOES NOT have a notary.
 * When this is set to a non-null value, an output state can be added by just passing in a [ContractState] – a
 * [TransactionState] with this notary specified will be generated automatically.
 */
@DeleteForDJVM
open class TransactionBuilder(
        var notary: Party? = null,
        var lockId: UUID = defaultLockId(),
        protected val inputs: MutableList<StateRef> = arrayListOf(),
        protected val attachments: MutableList<AttachmentId> = arrayListOf(),
        protected val outputs: MutableList<TransactionState<ContractState>> = arrayListOf(),
        protected val commands: MutableList<Command<*>> = arrayListOf(),
        protected var window: TimeWindow? = null,
        protected var privacySalt: PrivacySalt = PrivacySalt(),
        protected val references: MutableList<StateRef> = arrayListOf(),
        protected val serviceHub: ServiceHub? = (Strand.currentStrand() as? FlowStateMachine<*>)?.serviceHub
) {
    constructor(notary: Party? = null,
                lockId: UUID = defaultLockId(),
                inputs: MutableList<StateRef> = arrayListOf(),
                attachments: MutableList<AttachmentId> = arrayListOf(),
                outputs: MutableList<TransactionState<ContractState>> = arrayListOf(),
                commands: MutableList<Command<*>> = arrayListOf(),
                window: TimeWindow? = null,
                privacySalt: PrivacySalt = PrivacySalt()
    ) : this(notary, lockId, inputs, attachments, outputs, commands, window, privacySalt, arrayListOf())

    constructor(notary: Party) : this(notary, window = null)

    private companion object {
        private fun defaultLockId() = (Strand.currentStrand() as? FlowStateMachine<*>)?.id?.uuid ?: UUID.randomUUID()
        private val log = contextLogger()
        private val MISSING_CLASS_DISABLED = java.lang.Boolean.getBoolean("net.corda.transactionbuilder.missingclass.disabled")

        private const val ID_PATTERN = "\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*"
        private val FQCP: Pattern = Pattern.compile("$ID_PATTERN(/$ID_PATTERN)+")
        private fun isValidJavaClass(identifier: String) = FQCP.matcher(identifier).matches()
        private fun Collection<*>.deepEquals(other: Collection<*>): Boolean {
            return (size == other.size) && containsAll(other) && other.containsAll(this)
        }
        private fun Collection<AttachmentId>.toPrettyString(): String = sorted().joinToString(
            separator = System.lineSeparator(),
            prefix = System.lineSeparator()
        )
    }

    protected var hashAlgorithm = SHA2_256
        set(value) {
            field = value.toUpperCase()
        }

    private val inputsWithTransactionState = arrayListOf<StateAndRef<ContractState>>()
    private val referencesWithTransactionState = arrayListOf<TransactionState<ContractState>>()
    private val excludedAttachments = arrayListOf<AttachmentId>()

    /**
     * Creates a copy of the builder.
     */
    fun copy(): TransactionBuilder {
        val t = TransactionBuilder(
                notary = notary,
                inputs = ArrayList(inputs),
                attachments = ArrayList(attachments),
                outputs = ArrayList(outputs),
                commands = ArrayList(commands),
                window = window,
                privacySalt = privacySalt,
                references = ArrayList(references),
                serviceHub = serviceHub
        )
        t.hashAlgorithm = hashAlgorithm
        t.inputsWithTransactionState.addAll(this.inputsWithTransactionState)
        t.referencesWithTransactionState.addAll(this.referencesWithTransactionState)
        return t
    }

    // DOCSTART 1
    /** A more convenient way to add items to this transaction that calls the add* methods for you based on type */
    fun withItems(vararg items: Any) = apply {
        for (t in items) {
            when (t) {
                is StateAndRef<*> -> addInputState(t)
                is ReferencedStateAndRef<*> -> addReferenceState(t)
                is AttachmentId -> addAttachment(t)
                is TransactionState<*> -> addOutputState(t)
                is StateAndContract -> addOutputState(t.state, t.contract)
                is ContractState -> throw UnsupportedOperationException("Removed as of V1: please use a StateAndContract instead")
                is Command<*> -> addCommand(t)
                is CommandData -> throw IllegalArgumentException("You passed an instance of CommandData, but that lacks the pubkey. You need to wrap it in a Command object first.")
                is TimeWindow -> setTimeWindow(t)
                is PrivacySalt -> setPrivacySalt(t)
                else -> throw IllegalArgumentException("Wrong argument type: ${t.javaClass}")
            }
        }
    }
    // DOCEND 1

    /**
     * Generates a [WireTransaction] from this builder, resolves any [AutomaticPlaceholderConstraint], and selects the attachments to use for this transaction.
     *
     * @returns A new [WireTransaction] that will be unaffected by further changes to this [TransactionBuilder].
     *
     * @throws [ZoneVersionTooLowException] if there are reference states and the zone minimum platform version is less than 4.
     */
    @Throws(MissingContractAttachments::class)
    fun toWireTransaction(services: ServicesForResolution): WireTransaction = toWireTransactionWithContext(services, null)

    @CordaInternal
    internal fun toWireTransactionWithContext(
        services: ServicesForResolution,
        serializationContext: SerializationContext?
    ) : WireTransaction = toWireTransactionWithContext(services, serializationContext, 0)

    private tailrec fun toWireTransactionWithContext(
        services: ServicesForResolution,
        serializationContext: SerializationContext?,
        tryCount: Int
    ): WireTransaction {
        val referenceStates = referenceStates()
        if (referenceStates.isNotEmpty()) {
            services.ensureMinimumPlatformVersion(4, "Reference states")
        }

        val (allContractAttachments: Collection<AttachmentId>, resolvedOutputs: List<TransactionState<ContractState>>)
                = selectContractAttachmentsAndOutputStateConstraints(services, serializationContext)

        // Final sanity check that all states have the correct constraints.
        for (state in (inputsWithTransactionState.map { it.state } + resolvedOutputs)) {
            checkConstraintValidity(state)
        }

        val wireTx = SerializationFactory.defaultFactory.withCurrentContext(serializationContext) {
            WireTransaction(
                    createComponentGroups(
                            inputStates(),
                            resolvedOutputs,
                            commands(),
                            // Sort the attachments to ensure transaction builds are stable.
                            ((allContractAttachments + attachments).toSortedSet() - excludedAttachments).toList(),
                            notary,
                            window,
                            referenceStates,
                            services.networkParametersService.currentHash),
                    privacySalt,
                    hashAlgorithm
            )
        }

        // Check the transaction for missing dependencies, and attempt to add them.
        // This is a workaround as the current version of Corda does not support cordapp dependencies.
        // It works by running transaction validation and then scan the attachment storage for missing classes.
        // TODO - remove once proper support for cordapp dependencies is added.
        val addedDependency = addMissingDependency(services, wireTx, tryCount)

        return if (addedDependency)
            toWireTransactionWithContext(services, serializationContext, tryCount + 1)
        else
            wireTx
    }

    // Returns the first exception in the hierarchy that matches one of the [types].
    private tailrec fun Throwable.rootClassNotFoundCause(vararg types: KClass<*>): Throwable = when {
        this::class in types -> this
        this.cause == null -> this
        else -> this.cause!!.rootClassNotFoundCause(*types)
    }

    /**
     * @return true if a new dependency was successfully added.
     */
    private fun addMissingDependency(services: ServicesForResolution, wireTx: WireTransaction, tryCount: Int): Boolean {
        return try {
            wireTx.toLedgerTransaction(services).verify()
            // The transaction verified successfully without adding any extra dependency.
            false
        } catch (e: Throwable) {
            val rootError = e.rootClassNotFoundCause(ClassNotFoundException::class, NoClassDefFoundError::class)

            when {
                // Handle various exceptions that can be thrown during verification and drill down the wrappings.
                // Note: this is a best effort to preserve backwards compatibility.
                rootError is ClassNotFoundException -> {
                    ((tryCount == 0) && fixupAttachments(wireTx.attachments, services, e))
                        || addMissingAttachment((rootError.message ?: throw e).replace('.', '/'), services, e)
                }
                rootError is NoClassDefFoundError -> {
                    ((tryCount == 0) && fixupAttachments(wireTx.attachments, services, e))
                        || addMissingAttachment(rootError.message ?: throw e, services, e)
                }

                // Ignore these exceptions as they will break unit tests.
                // The point here is only to detect missing dependencies. The other exceptions are irrelevant.
                e is TransactionVerificationException -> false
                e is TransactionResolutionException -> false
                e is IllegalStateException -> false
                e is IllegalArgumentException -> false

                // Fail early if none of the expected scenarios were hit.
                else -> {
                    log.error("""The transaction currently built will not validate because of an unknown error most likely caused by a
                        missing dependency in the transaction attachments.
                        Please contact the developer of the CorDapp for further instructions.
                    """.trimIndent(), e)
                    throw e
                }
            }
        }
    }

    private fun fixupAttachments(
        txAttachments: List<AttachmentId>,
        services: ServicesForResolution,
        originalException: Throwable
    ): Boolean {
        val replacementAttachments = services.cordappProvider.internalFixupAttachmentIds(txAttachments)
        if (replacementAttachments.deepEquals(txAttachments)) {
            return false
        }

        val extraAttachments = replacementAttachments - txAttachments
        extraAttachments.forEach { id ->
            val attachment = services.attachments.openAttachment(id)
            if (attachment == null || !attachment.isUploaderTrusted()) {
                log.warn("""The node's fix-up rules suggest including attachment {}, which cannot be found either.
                    |Please contact the developer of the CorDapp for further instructions.
                    |""".trimMargin(), id)
                throw originalException
            }
        }

        attachments.addAll(extraAttachments)
        with(excludedAttachments) {
            clear()
            addAll(txAttachments - replacementAttachments)
        }

        log.warn("Attempting to rebuild transaction with these extra attachments:{}{}and these attachments removed:{}",
            extraAttachments.toPrettyString(),
            System.lineSeparator(),
            excludedAttachments.toPrettyString()
        )
        return true
    }

    private fun addMissingAttachment(missingClass: String, services: ServicesForResolution, originalException: Throwable): Boolean {
        if (!isValidJavaClass(missingClass)) {
            log.warn("Could not autodetect a valid attachment for the transaction being built.")
            throw originalException
        } else if (MISSING_CLASS_DISABLED) {
            log.warn("BROKEN TRANSACTION, BUT AUTOMATIC DETECTION OF {} IS DISABLED!", missingClass)
            throw originalException
        }

        val attachment = services.attachments.internalFindTrustedAttachmentForClass(missingClass)

        if (attachment == null) {
            log.error("""The transaction currently built is missing an attachment for class: $missingClass.
                        Attempted to find a suitable attachment but could not find any in the storage.
                        Please contact the developer of the CorDapp for further instructions.
                    """.trimIndent())
            throw originalException
        }

        log.warnOnce("""The transaction currently built is missing an attachment for class: $missingClass.
                        Automatically attaching contract dependency $attachment.
                        Please contact the developer of the CorDapp and install the latest version, as this approach might be insecure.
                    """.trimIndent())

        addAttachment(attachment.id)
        return true
    }

    /**
     * This method is responsible for selecting the contract versions to be used for the current transaction and resolve the output state
     * [AutomaticPlaceholderConstraint]s. The contract attachments are used to create a deterministic Classloader to deserialise the
     * transaction and to run the contract verification.
     *
     * The selection logic depends on the Attachment Constraints of the input, output and reference states, also on the explicitly
     * set attachments.
     * TODO also on the versions of the attachments of the transactions generating the input states. ( after we add versioning)
     */
    private fun selectContractAttachmentsAndOutputStateConstraints(
            services: ServicesForResolution,
            @Suppress("UNUSED_PARAMETER") serializationContext: SerializationContext?
    ): Pair<Collection<AttachmentId>, List<TransactionState<ContractState>>> {

        // Determine the explicitly set contract attachments.
        val explicitAttachmentContracts: List<Pair<ContractClassName, AttachmentId>> = this.attachments
                .map(services.attachments::openAttachment)
                .mapNotNull { it as? ContractAttachment }
                .flatMap { attch ->
                    attch.allContracts.map { it to attch.id }
                }

        // And fail early if there's more than 1 for a contract.
        require(explicitAttachmentContracts.isEmpty()
                  || explicitAttachmentContracts.groupBy { (ctr, _) -> ctr }.all { (_, groups) -> groups.size == 1 }) {
            "Multiple attachments set for the same contract."
        }

        val explicitAttachmentContractsMap: Map<ContractClassName, AttachmentId> = explicitAttachmentContracts.toMap()

        val inputContractGroups: Map<ContractClassName, List<TransactionState<ContractState>>> = inputsWithTransactionState.map { it.state }
                .groupBy { it.contract }
        val outputContractGroups: Map<ContractClassName, List<TransactionState<ContractState>>> = outputs.groupBy { it.contract }

        val allContracts: Set<ContractClassName> = inputContractGroups.keys + outputContractGroups.keys

        // Handle reference states.
        // Filter out all contracts that might have been already used by 'normal' input or output states.
        val referenceStateGroups: Map<ContractClassName, List<TransactionState<ContractState>>>
                = referencesWithTransactionState.groupBy { it.contract }
        val refStateContractAttachments: List<AttachmentId> = referenceStateGroups
                .filterNot { it.key in allContracts }
                .map { refStateEntry ->
                    getInstalledContractAttachmentId(
                            refStateEntry.key,
                            refStateEntry.value,
                            services
                    )
                }

        // For each contract, resolve the AutomaticPlaceholderConstraint, and select the attachment.
        val contractAttachmentsAndResolvedOutputStates: List<Pair<AttachmentId, List<TransactionState<ContractState>>?>> = allContracts.toSet()
                .map { ctr ->
                    handleContract(ctr, inputContractGroups[ctr], outputContractGroups[ctr], explicitAttachmentContractsMap[ctr], services)
                }

        val resolvedStates: List<TransactionState<ContractState>> = contractAttachmentsAndResolvedOutputStates.mapNotNull { it.second }
                .flatten()

        // The output states need to preserve the order in which they were added.
        val resolvedOutputStatesInTheOriginalOrder: List<TransactionState<ContractState>> = outputStates().map { os -> resolvedStates.find { rs -> rs.data == os.data && rs.encumbrance == os.encumbrance }!! }

        val attachments: Collection<AttachmentId> = contractAttachmentsAndResolvedOutputStates.map { it.first } + refStateContractAttachments

        return Pair(attachments, resolvedOutputStatesInTheOriginalOrder)
    }

    private val automaticConstraints = setOf(
            AutomaticPlaceholderConstraint,
            @Suppress("DEPRECATION") AutomaticHashConstraint
    )

    /**
     * Selects an attachment and resolves the constraints for the output states with [AutomaticPlaceholderConstraint].
     *
     * This is the place where the complex logic of the upgradability of contracts and constraint propagation is handled.
     *
     * * For contracts that *are not* annotated with @[NoConstraintPropagation], this will attempt to determine a constraint for the output states
     * that is a valid transition from all the constraints of the input states.
     *
     * * For contracts that *are* annotated with @[NoConstraintPropagation], this enforces setting an explicit output constraint.
     *
     * * For states with the [HashAttachmentConstraint], if an attachment with that hash is installed on the current node, then it will be inherited by the output states and selected for the transaction.
     * Otherwise a [MissingContractAttachments] is thrown.
     *
     * * For input states with [WhitelistedByZoneAttachmentConstraint] or a [AlwaysAcceptAttachmentConstraint] implementations, then the currently installed cordapp version is used.
     */
    private fun handleContract(
            contractClassName: ContractClassName,
            inputStates: List<TransactionState<ContractState>>?,
            outputStates: List<TransactionState<ContractState>>?,
            explicitContractAttachment: AttachmentId?,
            services: ServicesForResolution
    ): Pair<AttachmentId, List<TransactionState<ContractState>>?> {
        val inputsAndOutputs = (inputStates ?: emptyList()) + (outputStates ?: emptyList())

        fun selectAttachment() = getInstalledContractAttachmentId(
                contractClassName,
                inputsAndOutputs.filterNot { it.constraint in automaticConstraints },
                services
        )

        /*
        This block handles the very specific code path where a [HashAttachmentConstraint] can
        migrate to a [SignatureAttachmentConstraint]. If all the criteria is met, this function
        will return early as the rest of the logic is no longer required.

        This can only happen in a private network where all nodes have started with
        a system parameter that disables the hash constraint check.
        */
        if (canMigrateFromHashToSignatureConstraint(inputStates, outputStates, services)) {
            val attachmentId = selectAttachment()
            val attachment = services.attachments.openAttachment(attachmentId)
            require(attachment != null) { "Contract attachment $attachmentId for $contractClassName is missing." }
            if ((attachment as ContractAttachment).isSigned && (explicitContractAttachment == null || explicitContractAttachment == attachment.id)) {
                val signatureConstraint =
                        makeSignatureAttachmentConstraint(attachment.signerKeys)
                require(signatureConstraint.isSatisfiedBy(attachment)) { "Selected output constraint: $signatureConstraint not satisfying ${attachment.id}" }
                val resolvedOutputStates = outputStates?.map {
                    if (it.constraint in automaticConstraints) {
                        it.copy(constraint = signatureConstraint)
                    } else {
                        it
                    }
                }
                return attachment.id to resolvedOutputStates
            }
        }

        // Determine if there are any HashConstraints that pin the version of a contract. If there are, check if we trust them.
        val hashAttachments = inputsAndOutputs
                .filter { it.constraint is HashAttachmentConstraint }
                .map { state ->
                    val attachment = services.attachments.openAttachment((state.constraint as HashAttachmentConstraint).attachmentId)
                    if (attachment == null || attachment !is ContractAttachment || !isUploaderTrusted(attachment.uploader)) {
                        // This should never happen because these are input states that should have been validated already.
                        throw MissingContractAttachments(listOf(state))
                    }
                    attachment
                }.toSet()

        // Check that states with the HashConstraint don't conflict between themselves or with an explicitly set attachment.
        require(hashAttachments.size <= 1) {
            "Transaction was built with $contractClassName states with multiple HashConstraints. This is illegal, because it makes it impossible to validate with a single version of the contract code."
        }

        if (explicitContractAttachment != null && hashAttachments.singleOrNull() != null) {
            require(explicitContractAttachment == (hashAttachments.single() as ContractAttachment).attachment.id) {
                "An attachment has been explicitly set for contract $contractClassName in the transaction builder which conflicts with the HashConstraint of a state."
            }
        }

        // This will contain the hash of the JAR that *has* to be used by this Transaction, because it is explicit. Or null if none.
        val forcedAttachmentId = explicitContractAttachment ?: hashAttachments.singleOrNull()?.id

        // This will contain the hash of the JAR that will be used by this Transaction.
        val selectedAttachmentId = forcedAttachmentId ?: selectAttachment()

        val attachmentToUse = services.attachments.openAttachment(selectedAttachmentId)?.let { it as ContractAttachment }
                ?: throw IllegalArgumentException("Contract attachment $selectedAttachmentId for $contractClassName is missing.")

        // For Exit transactions (no output states) there is no need to resolve the output constraints.
        if (outputStates == null) {
            return Pair(selectedAttachmentId, null)
        }

        // If there are no automatic constraints, there is nothing to resolve.
        if (outputStates.none { it.constraint in automaticConstraints }) {
            return Pair(selectedAttachmentId, outputStates)
        }

        // The final step is to resolve AutomaticPlaceholderConstraint.
        val automaticConstraintPropagation = contractClassName.contractHasAutomaticConstraintPropagation(inputsAndOutputs.first().data::class.java.classLoader)

        // When automaticConstraintPropagation is disabled for a contract, output states must an explicit Constraint.
        require(automaticConstraintPropagation) { "Contract $contractClassName was marked with @NoConstraintPropagation, which means the constraint of the output states has to be set explicitly." }

        // This is the logic to determine the constraint which will replace the AutomaticPlaceholderConstraint.
        val defaultOutputConstraint = selectAttachmentConstraint(contractClassName, inputStates, attachmentToUse, services)

        // Sanity check that the selected attachment actually passes.
        val constraintAttachment = AttachmentWithContext(attachmentToUse, contractClassName, services.networkParameters.whitelistedContractImplementations)
        require(defaultOutputConstraint.isSatisfiedBy(constraintAttachment)) { "Selected output constraint: $defaultOutputConstraint not satisfying $selectedAttachmentId" }

        val resolvedOutputStates = outputStates.map {
            val outputConstraint = it.constraint
            if (outputConstraint in automaticConstraints) {
                it.copy(constraint = defaultOutputConstraint)
            } else {
                // If the constraint on the output state is already set, and is not a valid transition or can't be transitioned, then fail early.
                inputStates?.forEach { input ->
                    require(outputConstraint.canBeTransitionedFrom(input.constraint, attachmentToUse)) { "Output state constraint $outputConstraint cannot be transitioned from ${input.constraint}" }
                }
                require(outputConstraint.isSatisfiedBy(constraintAttachment)) { "Output state constraint check fails. $outputConstraint" }
                it
            }
        }

        return Pair(selectedAttachmentId, resolvedOutputStates)
    }

    /**
     * Checks whether the current transaction can migrate from a [HashAttachmentConstraint] to a
     * [SignatureAttachmentConstraint]. This is only possible in very specific scenarios. Most
     * importantly, [HashAttachmentConstraint.disableHashConstraints] must be set to `true` for
     * any possibility of transition off of existing [HashAttachmentConstraint]s.
     */
    private fun canMigrateFromHashToSignatureConstraint(
            inputStates: List<TransactionState<ContractState>>?,
            outputStates: List<TransactionState<ContractState>>?,
            services: ServicesForResolution
    ): Boolean {
        return HashAttachmentConstraint.disableHashConstraints
                && services.networkParameters.minimumPlatformVersion >= PlatformVersionSwitches.MIGRATE_HASH_TO_SIGNATURE_CONSTRAINTS
                // `disableHashConstraints == true` therefore it does not matter if there are
                // multiple input states with different hash constraints
                && inputStates?.any { it.constraint is HashAttachmentConstraint } == true
                && outputStates?.none { it.constraint is HashAttachmentConstraint } == true
    }

    /**
     * If there are multiple input states with different constraints then run the constraint intersection logic to determine the resulting output constraint.
     * For issuing transactions where the attachmentToUse is JarSigned, then default to the SignatureConstraint with all the signatures.
     * TODO - in the future this step can actually create a new ContractAttachment by merging 2 signed jars of the same version.
     */
    private fun selectAttachmentConstraint(
            contractClassName: ContractClassName,
            inputStates: List<TransactionState<ContractState>>?,
            attachmentToUse: ContractAttachment,
            services: ServicesForResolution): AttachmentConstraint = when {
        inputStates != null -> attachmentConstraintsTransition(inputStates.groupBy { it.constraint }.keys, attachmentToUse, services)
        attachmentToUse.signerKeys.isNotEmpty() && services.networkParameters.minimumPlatformVersion < PlatformVersionSwitches.MIGRATE_ATTACHMENT_TO_SIGNATURE_CONSTRAINTS -> {
            log.warnOnce("Signature constraints not available on network requiring a minimum platform version of ${PlatformVersionSwitches.MIGRATE_ATTACHMENT_TO_SIGNATURE_CONSTRAINTS}. Current is: ${services.networkParameters.minimumPlatformVersion}.")
            if (useWhitelistedByZoneAttachmentConstraint(contractClassName, services.networkParameters)) {
                log.warnOnce("Reverting back to using whitelisted zone constraints for contract $contractClassName")
                WhitelistedByZoneAttachmentConstraint
            } else {
                log.warnOnce("Reverting back to using hash constraints for contract $contractClassName")
                HashAttachmentConstraint(attachmentToUse.id)
            }
        }
        attachmentToUse.signerKeys.isNotEmpty() -> makeSignatureAttachmentConstraint(attachmentToUse.signerKeys)
        useWhitelistedByZoneAttachmentConstraint(contractClassName, services.networkParameters) -> WhitelistedByZoneAttachmentConstraint
        else -> HashAttachmentConstraint(attachmentToUse.id)
    }

    /**
     * Given a set of [AttachmentConstraint]s, this function implements the rules on how constraints can evolve.
     *
     * This should be an exhaustive check, and should mirror [AttachmentConstraint.canBeTransitionedFrom].
     *
     * TODO - once support for third party signing is added, it should be implemented here. ( a constraint with 2 signatures is less restrictive than a constraint with 1 more signature)
     */
    private fun attachmentConstraintsTransition(
            constraints: Set<AttachmentConstraint>,
            attachmentToUse: ContractAttachment,
            services: ServicesForResolution
    ): AttachmentConstraint = when {

        // Sanity check.
        constraints.isEmpty() -> throw IllegalArgumentException("Cannot transition from no constraints.")

        // Fail when combining the insecure AlwaysAcceptAttachmentConstraint with something else.
        constraints.size > 1 && constraints.any { it is AlwaysAcceptAttachmentConstraint } ->
            throw IllegalArgumentException("Can't mix the AlwaysAcceptAttachmentConstraint with a secure constraint in the same transaction. This can be used to hide insecure transitions.")

        // Multiple states with Hash constraints with different hashes. This should not happen as we checked already.
        constraints.size > 1 && constraints.all { it is HashAttachmentConstraint } ->
            throw IllegalArgumentException("Cannot mix HashConstraints with different hashes in the same transaction.")

        // The HashAttachmentConstraint is the strongest constraint, so it wins when mixed with anything. As long as the actual constraints pass.
        // Migration from HashAttachmentConstraint to SignatureAttachmentConstraint is handled in [TransactionBuilder.handleContract]
        // If we have reached this point, then no migration is possible and the existing HashAttachmentConstraint must be used
        constraints.any { it is HashAttachmentConstraint } -> constraints.find { it is HashAttachmentConstraint }!!

        // TODO, we don't currently support mixing signature constraints with different signers. This will change once we introduce third party signers.
        constraints.count { it is SignatureAttachmentConstraint } > 1 ->
            throw IllegalArgumentException("Cannot mix SignatureAttachmentConstraints signed by different parties in the same transaction.")

        // This ensures a smooth migration from a Whitelist Constraint to a Signature Constraint
        constraints.any { it is WhitelistedByZoneAttachmentConstraint } &&
                attachmentToUse.isSigned &&
                services.networkParameters.minimumPlatformVersion >= PlatformVersionSwitches.MIGRATE_ATTACHMENT_TO_SIGNATURE_CONSTRAINTS ->
            transitionToSignatureConstraint(constraints, attachmentToUse)

        // This condition is hit when the current node has not installed the latest signed version but has already received states that have been migrated
        constraints.any { it is SignatureAttachmentConstraint } && !attachmentToUse.isSigned ->
            throw IllegalArgumentException("Attempting to create an illegal transaction. Please install the latest signed version for the $attachmentToUse Cordapp.")

        // When all input states have the same constraint.
        constraints.size == 1 -> constraints.single()

        else -> throw IllegalArgumentException("Unexpected constraints $constraints.")
    }

    private fun transitionToSignatureConstraint(constraints: Set<AttachmentConstraint>, attachmentToUse: ContractAttachment): SignatureAttachmentConstraint {
        val signatureConstraint = constraints.singleOrNull { it is SignatureAttachmentConstraint } as? SignatureAttachmentConstraint
        // If there were states transitioned already used in the current transaction use that signature constraint, otherwise create a new one.
        return when {
            signatureConstraint != null -> signatureConstraint
            else -> makeSignatureAttachmentConstraint(attachmentToUse.signerKeys)
        }
    }

    private fun makeSignatureAttachmentConstraint(attachmentSigners: List<PublicKey>) =
            SignatureAttachmentConstraint(CompositeKey.Builder().addKeys(attachmentSigners).build())

    private fun getInstalledContractAttachmentId(
            contractClassName: String,
            states: List<TransactionState<ContractState>>,
            services: ServicesForResolution
    ): AttachmentId {
        return services.cordappProvider.getContractAttachmentID(contractClassName)
                ?: throw MissingContractAttachments(states, contractClassName)
    }

    private fun useWhitelistedByZoneAttachmentConstraint(contractClassName: ContractClassName, networkParameters: NetworkParameters) = contractClassName in networkParameters.whitelistedContractImplementations.keys

    @Throws(AttachmentResolutionException::class, TransactionResolutionException::class)
    fun toLedgerTransaction(services: ServiceHub) = toWireTransaction(services).toLedgerTransaction(services)

    internal fun toLedgerTransactionWithContext(services: ServicesForResolution, serializationContext: SerializationContext): LedgerTransaction {
        return toWireTransactionWithContext(services, serializationContext).toLedgerTransaction(services)
    }

    @Throws(AttachmentResolutionException::class, TransactionResolutionException::class, TransactionVerificationException::class)
    fun verify(services: ServiceHub) {
        toLedgerTransaction(services).verify()
    }

    private fun checkNotary(stateAndRef: StateAndRef<*>) {
        val notary = stateAndRef.state.notary
        require(notary == this.notary) {
            "Input state requires notary \"$notary\" which does not match the transaction notary \"${this.notary}\"."
        }
    }

    // This check is performed here as well as in BaseTransaction.
    private fun checkForInputsAndReferencesOverlap() {
        val intersection = inputs intersect references
        require(intersection.isEmpty()) {
            "A StateRef cannot be both an input and a reference input in the same transaction."
        }
    }

    private fun checkReferencesUseSameNotary() = referencesWithTransactionState.map { it.notary }.toSet().size == 1

    /**
     * If any inputs or outputs added to the [TransactionBuilder] contain [StatePointer]s, then this method is used
     * to resolve those [StatePointer]s to [StateAndRef]s. The [StateAndRef]s are then added as reference states to
     * the transaction. The effect is that the referenced data is carried along with the transaction.
     *
     * If this method is called outside the context of a flow, a [ServiceHub] instance must be passed to this method
     * for it to be able to resolve [StatePointer]s. Usually for a unit test, this will be an instance of mock services.
     *
     * @param serviceHub a [ServiceHub] instance needed for performing vault queries.
     *
     * @throws IllegalStateException if no [ServiceHub] is provided and no flow context is available.
     */
    private fun resolveStatePointers(transactionState: TransactionState<*>) {
        val contractState = transactionState.data
        // Find pointers in all inputs and outputs.
        val inputAndOutputPointers = StatePointerSearch(contractState).search()
        // Queue up the pointers to resolve.
        val statePointerQueue = ArrayDeque<StatePointer<*>>().apply { addAll(inputAndOutputPointers) }
        // Recursively resolve all pointers.
        while (statePointerQueue.isNotEmpty()) {
            val nextStatePointer = statePointerQueue.pop()
            val hub = serviceHub
            if (hub != null && nextStatePointer.isResolved) {
                val resolvedStateAndRef = nextStatePointer.resolve(hub)
                // Don't add dupe reference states because CoreTransaction doesn't allow it.
                if (resolvedStateAndRef.ref !in referenceStates()) {
                    addReferenceState(resolvedStateAndRef.referenced())
                }
            } else {
                log.warn("WARNING: You must pass in a ServiceHub reference to TransactionBuilder to resolve " +
                        "state pointers outside of flows. If you are writing a unit test then pass in a " +
                        "MockServices instance.")
                return
            }
        }
    }

    /**
     * Adds a reference input [StateRef] to the transaction.
     *
     * Note: Reference states are only supported on Corda networks running a minimum platform version of 4.
     * [toWireTransaction] will throw an [IllegalStateException] if called in such an environment.
     */
    open fun addReferenceState(referencedStateAndRef: ReferencedStateAndRef<*>) = apply {
        val stateAndRef = referencedStateAndRef.stateAndRef
        referencesWithTransactionState.add(stateAndRef.state)

        // It is likely the case that users of reference states do not have permission to change the notary assigned
        // to a reference state. Even if users _did_ have this permission the result would likely be a bunch of
        // notary change races. As such, if a reference state is added to a transaction which is assigned to a
        // different notary to the input and output states then all those inputs and outputs must be moved to the
        // notary which the reference state uses.
        //
        // If two or more reference states assigned to different notaries are added to a transaction then it follows
        // that this transaction likely _cannot_ be committed to the ledger as it unlikely that the party using the
        // reference state can change the assigned notary for one of the reference states.
        //
        // As such, if reference states assigned to multiple different notaries are added to a transaction builder
        // then the check below will fail.
        check(checkReferencesUseSameNotary()) {
            "Transactions with reference states using multiple different notaries are currently unsupported."
        }

        // State Pointers are recursively resolved. NOTE: That this might be expensive.
        // TODO: Add support for making recursive resolution optional if it becomes an issue.
        resolveStatePointers(stateAndRef.state)

        checkNotary(stateAndRef)
        references.add(stateAndRef.ref)
        checkForInputsAndReferencesOverlap()
    }

    /** Adds an input [StateRef] to the transaction. */
    open fun addInputState(stateAndRef: StateAndRef<*>) = apply {
        checkNotary(stateAndRef)
        inputs.add(stateAndRef.ref)
        inputsWithTransactionState.add(stateAndRef)
        resolveStatePointers(stateAndRef.state)
        return this
    }

    /** Adds an attachment with the specified hash to the TransactionBuilder. */
    fun addAttachment(attachmentId: AttachmentId) = apply {
        attachments.add(attachmentId)
    }

    /** Adds an output state to the transaction. */
    fun addOutputState(state: TransactionState<*>) = apply {
        outputs.add(state)
        resolveStatePointers(state)
        return this
    }

    /** Adds an output state, with associated contract code (and constraints), and notary, to the transaction. */
    @JvmOverloads
    fun addOutputState(
        state: ContractState,
        contract: ContractClassName = requireNotNullContractClassName(state),
        notary: Party, encumbrance: Int? = null,
        constraint: AttachmentConstraint = AutomaticPlaceholderConstraint
    ): TransactionBuilder {
        return addOutputState(TransactionState(state, contract, notary, encumbrance, constraint))
    }

    /** Adds an output state. A default notary must be specified during builder construction to use this method */
    @JvmOverloads
    fun addOutputState(
        state: ContractState,
        contract: ContractClassName = requireNotNullContractClassName(state),
        constraint: AttachmentConstraint = AutomaticPlaceholderConstraint
    ): TransactionBuilder {
        checkNotNull(notary) { "Need to specify a notary for the state, or set a default one on TransactionBuilder initialisation" }
        addOutputState(state, contract, notary!!, constraint = constraint)
        return this
    }

    /** Adds an output state with the specified constraint. */
    fun addOutputState(state: ContractState, constraint: AttachmentConstraint): TransactionBuilder {
        return addOutputState(state, requireNotNullContractClassName(state), constraint)
    }

    private fun requireNotNullContractClassName(state: ContractState) = requireNotNull(state.requiredContractClassName) {
        //TODO: add link to docsite page, when there is one.
        """
        Unable to infer Contract class name because state class ${state::class.java.name} is not annotated with
        @BelongsToContract, and does not have an enclosing class which implements Contract. Either annotate ${state::class.java.name}
        with @BelongsToContract, or supply an explicit contract parameter to addOutputState().
        """.trimIndent().replace('\n', ' ')
    }

    /** Adds a [Command] to the transaction. */
    fun addCommand(arg: Command<*>) = apply {
        commands.add(arg)
    }

    /**
     * Adds a [Command] to the transaction, specified by the encapsulated [CommandData] object and required list of
     * signing [PublicKey]s.
     */
    fun addCommand(data: CommandData, vararg keys: PublicKey) = addCommand(Command(data, listOf(*keys)))

    fun addCommand(data: CommandData, keys: List<PublicKey>) = addCommand(Command(data, keys))

    /**
     * Sets the [TimeWindow] for this transaction, replacing the existing [TimeWindow] if there is one. To be valid, the
     * transaction must then be signed by the notary service within this window of time. In this way, the notary acts as
     * the Timestamp Authority.
     */
    fun setTimeWindow(timeWindow: TimeWindow) = apply {
        check(notary != null) { "Only notarised transactions can have a time-window" }
        window = timeWindow
    }

    /**
     * The [TimeWindow] for the transaction can also be defined as [time] +/- [timeTolerance]. The tolerance should be
     * chosen such that your code can finish building the transaction and sending it to the Timestamp Authority within
     * that window of time, taking into account factors such as network latency. Transactions being built by a group of
     * collaborating parties may therefore require a higher time tolerance than a transaction being built by a single
     * node.
     */
    fun setTimeWindow(time: Instant, timeTolerance: Duration) = setTimeWindow(TimeWindow.withTolerance(time, timeTolerance))

    fun setPrivacySalt(privacySalt: PrivacySalt) = apply {
        this.privacySalt = privacySalt
    }

    fun setHashAlgorithm(hashAlgorithm: String) = apply {
        this.hashAlgorithm = hashAlgorithm
    }

    fun resalt() = apply {
        privacySalt = PrivacySalt.createFor(hashAlgorithm)
    }

    /** Returns an immutable list of input [StateRef]s. */
    fun inputStates(): List<StateRef> = ArrayList(inputs)

    /** Returns an immutable list of reference input [StateRef]s. */
    fun referenceStates(): List<StateRef> = ArrayList(references)

    /** Returns an immutable list of attachment hashes. */
    fun attachments(): List<AttachmentId> = ArrayList(attachments)

    /** Returns an immutable list of output [TransactionState]s. */
    fun outputStates(): List<TransactionState<*>> = ArrayList(outputs)

    /** Returns an immutable list of [Command]s. */
    fun commands(): List<Command<*>> = ArrayList(commands)

    /**
     * Sign the built transaction and return it. This is an internal function for use by the service hub, please use
     * [ServiceHub.signInitialTransaction] instead.
     */
    fun toSignedTransaction(keyManagementService: KeyManagementService,
                            publicKey: PublicKey,
                            signatureMetadata: SignatureMetadata,
                            services: ServicesForResolution): SignedTransaction {
        val wtx = toWireTransaction(services)
        val signableData = SignableData(wtx.id, signatureMetadata)
        val sig = keyManagementService.sign(signableData, publicKey)
        return SignedTransaction(wtx, listOf(sig))
    }
}
