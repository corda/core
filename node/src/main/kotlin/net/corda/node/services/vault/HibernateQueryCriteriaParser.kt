package net.corda.node.services.vault

import net.corda.core.contracts.ContractClassName
import net.corda.core.contracts.ContractState
import net.corda.core.contracts.StateRef
import net.corda.core.identity.AbstractParty
import net.corda.core.internal.uncheckedCast
import net.corda.core.node.services.Vault
import net.corda.core.node.services.VaultQueryException
import net.corda.core.node.services.vault.*
import net.corda.core.node.services.vault.BinaryComparisonOperator.*
import net.corda.core.node.services.vault.CollectionOperator.*
import net.corda.core.node.services.vault.ColumnPredicate.*
import net.corda.core.node.services.vault.EqualityComparisonOperator.*
import net.corda.core.node.services.vault.LikenessOperator.*
import net.corda.core.node.services.vault.NullOperator.IS_NULL
import net.corda.core.node.services.vault.NullOperator.NOT_NULL
import net.corda.core.node.services.vault.QueryCriteria.CommonQueryCriteria
import net.corda.core.schemas.IndirectStatePersistable
import net.corda.core.schemas.PersistentState
import net.corda.core.schemas.PersistentStateRef
import net.corda.core.schemas.StatePersistable
import net.corda.core.utilities.OpaqueBytes
import net.corda.core.utilities.contextLogger
import net.corda.core.utilities.trace
import net.corda.node.services.persistence.NodeAttachmentService
import org.hibernate.query.criteria.internal.expression.LiteralExpression
import org.hibernate.query.criteria.internal.path.SingularAttributePath
import org.hibernate.query.criteria.internal.predicate.ComparisonPredicate
import org.hibernate.query.criteria.internal.predicate.CompoundPredicate
import org.hibernate.query.criteria.internal.predicate.InPredicate
import java.security.PublicKey
import java.time.Instant
import java.util.*
import javax.persistence.Tuple
import javax.persistence.criteria.*


abstract class AbstractQueryCriteriaParser<Q : GenericQueryCriteria<Q,P>, in P: BaseQueryCriteriaParser<Q, P, S>, in S: BaseSort> : BaseQueryCriteriaParser<Q, P, S> {

    abstract val criteriaBuilder: CriteriaBuilder

    override fun parseOr(left: Q, right: Q): Collection<Predicate> {
        val predicateSet = mutableSetOf<Predicate>()
        val leftPredicates = parse(left)
        val rightPredicates = parse(right)

        val leftAnd = criteriaBuilder.and(*leftPredicates.toTypedArray())
        val rightAnd = criteriaBuilder.and(*rightPredicates.toTypedArray())
        val orPredicate = criteriaBuilder.or(leftAnd,rightAnd)
        predicateSet.add(orPredicate)

        return predicateSet
    }

    override fun parseAnd(left: Q, right: Q): Collection<Predicate> {
        val predicateSet = mutableSetOf<Predicate>()
        val leftPredicates = parse(left)
        val rightPredicates = parse(right)

        val andPredicate = criteriaBuilder.and(*leftPredicates.toTypedArray(), *rightPredicates.toTypedArray())
        predicateSet.add(andPredicate)

        return predicateSet
    }

    protected fun columnPredicateToPredicate(column: Path<out Any?>, columnPredicate: ColumnPredicate<*>): Predicate {
        return when (columnPredicate) {
            is EqualityComparison -> equalityComparisonToPredicate(column, columnPredicate)
            is BinaryComparison -> binaryComparisonToPredicate(column, columnPredicate)
            is Likeness -> likeComparisonToPredicate(column, columnPredicate)
            is CollectionExpression -> collectionComparisonToPredicate(column, columnPredicate)
            is Between -> betweenComparisonToPredicate(column, columnPredicate)
            is NullExpression -> nullComparisonToPredicate(column, columnPredicate)
            else -> throw VaultQueryException("Not expecting $columnPredicate")
        }
    }

    private fun equalityComparisonToPredicate(column: Path<out Any?>, columnPredicate: EqualityComparison<*>): Predicate {
        val literal = columnPredicate.rightLiteral
        return if (literal is String) {
            @Suppress("UNCHECKED_CAST")
            column as Path<String?>
            when (columnPredicate.operator) {
                EQUAL -> criteriaBuilder.equal(column, literal)
                EQUAL_IGNORE_CASE -> criteriaBuilder.equal(criteriaBuilder.upper(column), literal.toUpperCase())
                NOT_EQUAL -> criteriaBuilder.notEqual(column, literal)
                NOT_EQUAL_IGNORE_CASE -> criteriaBuilder.notEqual(criteriaBuilder.upper(column), literal.toUpperCase())
            }
        } else {
            when (columnPredicate.operator) {
                EQUAL, EQUAL_IGNORE_CASE -> criteriaBuilder.equal(column, literal)
                NOT_EQUAL, NOT_EQUAL_IGNORE_CASE -> criteriaBuilder.notEqual(column, literal)
            }
        }
    }

    private fun binaryComparisonToPredicate(column: Path<out Any?>, columnPredicate: BinaryComparison<*>): Predicate {
        val literal: Comparable<Any?>? = uncheckedCast(columnPredicate.rightLiteral)
        @Suppress("UNCHECKED_CAST")
        column as Path<Comparable<Any?>?>
        return when (columnPredicate.operator) {
            GREATER_THAN -> criteriaBuilder.greaterThan(column, literal)
            GREATER_THAN_OR_EQUAL -> criteriaBuilder.greaterThanOrEqualTo(column, literal)
            LESS_THAN -> criteriaBuilder.lessThan(column, literal)
            LESS_THAN_OR_EQUAL -> criteriaBuilder.lessThanOrEqualTo(column, literal)
        }
    }

    private fun likeComparisonToPredicate(column: Path<out Any?>, columnPredicate: Likeness): Predicate {
        @Suppress("UNCHECKED_CAST")
        column as Path<String?>
        return when (columnPredicate.operator) {
            LIKE -> criteriaBuilder.like(column, columnPredicate.rightLiteral)
            LIKE_IGNORE_CASE -> criteriaBuilder.like(criteriaBuilder.upper(column), columnPredicate.rightLiteral.toUpperCase())
            NOT_LIKE -> criteriaBuilder.notLike(column, columnPredicate.rightLiteral)
            NOT_LIKE_IGNORE_CASE -> criteriaBuilder.notLike(criteriaBuilder.upper(column), columnPredicate.rightLiteral.toUpperCase())
        }
    }

    private fun stringCollectionComparisonToPredicate(
            columnPredicate: CollectionExpression<*>,
            column: Path<String?>,
            literal: Collection<String>
    ): Predicate {
        return when (columnPredicate.operator) {
            IN -> column.`in`(literal)
            IN_IGNORE_CASE -> criteriaBuilder.upper(column).`in`(literal.map { it.toUpperCase() })
            NOT_IN -> criteriaBuilder.not(column.`in`(literal))
            NOT_IN_IGNORE_CASE -> criteriaBuilder.not(criteriaBuilder.upper(column).`in`(literal.map { it.toUpperCase() }))
        }
    }

    private fun collectionComparisonToPredicate(column: Path<out Any?>, columnPredicate: CollectionExpression<*>): Predicate {
        val literal = if (columnPredicate.rightLiteral.isEmpty()) {
            // This is needed because MS SQL does not allow writing IN statement with empty list,
            // e.g this is invalid: "select 1 where 'a' in ()"
            // To fix it we just insert a null into the list and the query becomes: "select 1 where 'a' in (null)"
            listOf(null)
        } else {
            columnPredicate.rightLiteral
        }
        return if (literal.any { it is String }) {
            @Suppress("UNCHECKED_CAST")
            stringCollectionComparisonToPredicate(columnPredicate, column as Path<String?>, literal as Collection<String>)
        } else {
            when (columnPredicate.operator) {
                IN, IN_IGNORE_CASE -> column.`in`(literal)
                NOT_IN, NOT_IN_IGNORE_CASE -> criteriaBuilder.not(column.`in`(literal))
            }
        }
    }

    private fun betweenComparisonToPredicate(column: Path<out Any?>, columnPredicate: Between<*>): Predicate {
        @Suppress("UNCHECKED_CAST")
        column as Path<Comparable<Any?>?>
        val fromLiteral: Comparable<Any?>? = uncheckedCast(columnPredicate.rightFromLiteral)
        val toLiteral: Comparable<Any?>? = uncheckedCast(columnPredicate.rightToLiteral)
        return criteriaBuilder.between(column, fromLiteral, toLiteral)
    }

    private fun nullComparisonToPredicate(column: Path<out Any?>, columnPredicate: NullExpression<*>): Predicate {
        return when (columnPredicate.operator) {
            IS_NULL -> criteriaBuilder.isNull(column)
            NOT_NULL -> criteriaBuilder.isNotNull(column)
        }
    }
}

class HibernateAttachmentQueryCriteriaParser(override val criteriaBuilder: CriteriaBuilder,
                                             private val criteriaQuery: CriteriaQuery<NodeAttachmentService.DBAttachment>, val root: Root<NodeAttachmentService.DBAttachment>) :
        AbstractQueryCriteriaParser<AttachmentQueryCriteria, AttachmentsQueryCriteriaParser, AttachmentSort>(), AttachmentsQueryCriteriaParser {

    private companion object {
        private val log = contextLogger()
    }

    init {
        criteriaQuery.select(root)
    }

    override fun parse(criteria: AttachmentQueryCriteria, sorting: AttachmentSort?): Collection<Predicate> {
        val predicateSet = criteria.visit(this)

        sorting?.let {
            if (sorting.columns.isNotEmpty())
                parse(sorting)
        }

        criteriaQuery.where(*predicateSet.toTypedArray())

        return predicateSet
    }

    private fun parse(sorting: AttachmentSort) {
        log.trace { "Parsing sorting specification: $sorting" }

        val orderCriteria = mutableListOf<Order>()

        sorting.columns.map { (sortAttribute, direction) ->
            when (direction) {
                Sort.Direction.ASC -> orderCriteria.add(criteriaBuilder.asc(root.get<String>(sortAttribute.columnName)))
                Sort.Direction.DESC -> orderCriteria.add(criteriaBuilder.desc(root.get<String>(sortAttribute.columnName)))
            }
        }
        if (orderCriteria.isNotEmpty()) {
            criteriaQuery.orderBy(orderCriteria)
        }
    }

    override fun parseCriteria(criteria: AttachmentQueryCriteria.AttachmentsQueryCriteria): Collection<Predicate> {
        log.trace { "Parsing AttachmentsQueryCriteria: $criteria" }

        val predicateSet = mutableSetOf<Predicate>()

        criteria.filenameCondition?.let {
            predicateSet.add(columnPredicateToPredicate(root.get<String>("filename"), it))
        }

        criteria.uploaderCondition?.let {
            predicateSet.add(columnPredicateToPredicate(root.get<String>("uploader"), it))
        }

        criteria.uploadDateCondition?.let {
            predicateSet.add(columnPredicateToPredicate(root.get<Instant>("insertionDate"), it))
        }

        criteria.contractClassNamesCondition?.let {
            val contractClassNames =
                if (criteria.contractClassNamesCondition is EqualityComparison)
                    (criteria.contractClassNamesCondition as EqualityComparison<List<ContractClassName>>).rightLiteral
                else emptyList()
            val joinDBAttachmentToContractClassNames = root.joinList<NodeAttachmentService.DBAttachment, ContractClassName>("contractClassNames")
            predicateSet.add(criteriaBuilder.and(joinDBAttachmentToContractClassNames.`in`(contractClassNames)))
        }

        criteria.signersCondition?.let {
            val signers =
                    if (criteria.signersCondition is EqualityComparison)
                        (criteria.signersCondition as EqualityComparison<List<PublicKey>>).rightLiteral
                    else emptyList()
            val joinDBAttachmentToSigners = root.joinList<NodeAttachmentService.DBAttachment, PublicKey>("signers")
            predicateSet.add(criteriaBuilder.and(joinDBAttachmentToSigners.`in`(signers)))
        }

        criteria.isSignedCondition?.let { isSigned ->
            if (isSigned == Builder.equal(true)) {
                val joinDBAttachmentToSigners = root.joinList<NodeAttachmentService.DBAttachment, PublicKey>("signers")
                predicateSet.add(criteriaBuilder.and(joinDBAttachmentToSigners.isNotNull))
            } else {
                predicateSet.add(criteriaBuilder.equal(criteriaBuilder.size(root.get<List<PublicKey>?>("signers")),0))
            }
        }

        criteria.versionCondition?.let {
            predicateSet.add(columnPredicateToPredicate(root.get<String>("version"), it))
        }

        return predicateSet
    }
}

class HibernateQueryCriteriaParser(val contractStateType: Class<out ContractState>,
                                   val contractStateTypeMappings: Map<String, Set<String>>,
                                   override val criteriaBuilder: CriteriaBuilder,
                                   val criteriaQuery: CriteriaQuery<Tuple>,
                                   val vaultStates: Root<VaultSchemaV1.VaultStates>) : AbstractQueryCriteriaParser<QueryCriteria, IQueryCriteriaParser, Sort>(), IQueryCriteriaParser {
    private companion object {
        private val log = contextLogger()
    }

    // incrementally build list of join predicates
    private val joinPredicates = mutableListOf<Predicate>()
    // incrementally build list of root entities (for later use in Sort parsing)
    private val rootEntities = mutableMapOf<Class<out StatePersistable>, Root<*>>(Pair(VaultSchemaV1.VaultStates::class.java, vaultStates))
    private val aggregateExpressions = mutableListOf<Expression<*>>()
    private val commonPredicates = mutableMapOf<Pair<String, Operator>, Predicate>()   // schema attribute Name, operator -> predicate
    private val constraintPredicates = mutableSetOf<Predicate>()

    var stateTypes: Vault.StateStatus = Vault.StateStatus.UNCONSUMED

    override fun parseCriteria(criteria: QueryCriteria.VaultQueryCriteria): Collection<Predicate> {
        log.trace { "Parsing VaultQueryCriteria: $criteria" }
        val predicateSet = mutableSetOf<Predicate>()

        // soft locking
        criteria.softLockingCondition?.let {
            val softLocking = criteria.softLockingCondition
            val type = softLocking!!.type
            when (type) {
                QueryCriteria.SoftLockingType.UNLOCKED_ONLY ->
                    predicateSet.add(criteriaBuilder.and(vaultStates.get<String>("lockId").isNull))
                QueryCriteria.SoftLockingType.LOCKED_ONLY ->
                    predicateSet.add(criteriaBuilder.and(vaultStates.get<String>("lockId").isNotNull))
                QueryCriteria.SoftLockingType.UNLOCKED_AND_SPECIFIED -> {
                    require(softLocking.lockIds.isNotEmpty()) { "Must specify one or more lockIds" }
                    predicateSet.add(criteriaBuilder.or(vaultStates.get<String>("lockId").isNull,
                            vaultStates.get<String>("lockId").`in`(softLocking.lockIds.map { it.toString() })))
                }
                QueryCriteria.SoftLockingType.SPECIFIED -> {
                    require(softLocking.lockIds.isNotEmpty()) { "Must specify one or more lockIds" }
                    predicateSet.add(criteriaBuilder.and(vaultStates.get<String>("lockId").`in`(softLocking.lockIds.map { it.toString() })))
                }
            }
        }

        // notary names
        criteria.notary?.let {
            predicateSet.add(criteriaBuilder.and(vaultStates.get<AbstractParty>("notary").`in`(criteria.notary)))
        }

        // state references
        criteria.stateRefs?.let {
            val persistentStateRefs = (criteria.stateRefs as List<StateRef>).map(::PersistentStateRef)
            val compositeKey = vaultStates.get<PersistentStateRef>("stateRef")
            predicateSet.add(criteriaBuilder.and(compositeKey.`in`(persistentStateRefs)))
        }

        // time constraints (recorded, consumed)
        criteria.timeCondition?.let {
            val timeCondition = criteria.timeCondition
            val timeInstantType = timeCondition!!.type
            val timeColumn = when (timeInstantType) {
                QueryCriteria.TimeInstantType.RECORDED -> Column(VaultSchemaV1.VaultStates::recordedTime)
                QueryCriteria.TimeInstantType.CONSUMED -> Column(VaultSchemaV1.VaultStates::consumedTime)
            }
            val expression = CriteriaExpression.ColumnPredicateExpression(timeColumn, timeCondition.predicate)
            predicateSet.add(parseExpression(vaultStates, expression) as Predicate)
        }
        return predicateSet
    }

    private fun deriveContractStateTypes(contractStateTypes: Set<Class<out ContractState>>? = null): Set<String> {
        log.trace { "Contract types to be derived: primary ($contractStateType), additional ($contractStateTypes)" }
        val combinedContractStateTypes = contractStateTypes?.plus(contractStateType) ?: setOf(contractStateType)
        combinedContractStateTypes.filter { it.name != ContractState::class.java.name }.let {
            val interfaces = it.flatMap { contractStateTypeMappings[it.name] ?: setOf(it.name) }
            val concrete = it.filter { !it.isInterface }.map { it.name }
            log.trace { "Derived contract types: ${interfaces.union(concrete)}" }
            return interfaces.union(concrete)
        }
    }


    private fun <O> parseExpression(entityRoot: Root<O>, expression: CriteriaExpression<O, Boolean>, predicateSet: MutableSet<Predicate>) {
        if (expression is CriteriaExpression.AggregateFunctionExpression<O, *>) {
            parseAggregateFunction(entityRoot, expression)
        } else {
            predicateSet.add(parseExpression(entityRoot, expression) as Predicate)
        }
    }

    private fun <O, R> parseExpression(root: Root<O>, expression: CriteriaExpression<O, R>): Expression<Boolean> {
        return when (expression) {
            is CriteriaExpression.BinaryLogical -> {
                val leftPredicate = parseExpression(root, expression.left)
                val rightPredicate = parseExpression(root, expression.right)
                when (expression.operator) {
                    BinaryLogicalOperator.AND -> criteriaBuilder.and(leftPredicate, rightPredicate)
                    BinaryLogicalOperator.OR -> criteriaBuilder.or(leftPredicate, rightPredicate)
                }
            }
            is CriteriaExpression.Not -> criteriaBuilder.not(parseExpression(root, expression.expression))
            is CriteriaExpression.ColumnPredicateExpression<O, *> -> {
                val column = root.get<Any?>(getColumnName(expression.column))
                columnPredicateToPredicate(column, expression.predicate)
            }
            else -> throw VaultQueryException("Unexpected expression: $expression")
        }
    }

    private fun <O, R> parseAggregateFunction(root: Root<O>, expression: CriteriaExpression.AggregateFunctionExpression<O, R>): Expression<out Any?>? {
        val column = root.get<Any?>(getColumnName(expression.column))
        val columnPredicate = expression.predicate
        when (columnPredicate) {
            is ColumnPredicate.AggregateFunction -> {
                @Suppress("UNCHECKED_CAST")
                column as Path<Long?>?
                val aggregateExpression =
                        when (columnPredicate.type) {
                            AggregateFunctionType.SUM -> criteriaBuilder.sum(column)
                            AggregateFunctionType.AVG -> criteriaBuilder.avg(column)
                            AggregateFunctionType.COUNT -> criteriaBuilder.count(column)
                            AggregateFunctionType.MAX -> criteriaBuilder.max(column)
                            AggregateFunctionType.MIN -> criteriaBuilder.min(column)
                        }
                //TODO investigate possibility to avoid producing redundant joins in SQL for multiple aggregate functions against the same table
                aggregateExpressions.add(aggregateExpression)
                // Some databases may not support aggregate expression in 'group by' clause e.g. 'group by sum(col)',
                // Hibernate Criteria Builder can't produce alias 'group by col_alias', and the only solution is to use a positional parameter 'group by 1'
                val orderByColumnPosition = aggregateExpressions.size
                var shiftLeft = 0
                // add optional group by clauses
                expression.groupByColumns?.let { columns ->
                    val groupByExpressions =
                            columns.map { _column ->
                                val path = root.get<Any?>(getColumnName(_column))
                                val columnNumberBeforeRemoval = aggregateExpressions.size
                                if (path is SingularAttributePath) //remove the same columns from different joins to match the single column in 'group by' only (from the last join)
                                    aggregateExpressions.removeAll {
                                        elem -> if (elem is SingularAttributePath) elem.attribute.javaMember == path.attribute.javaMember else false
                                    }
                                shiftLeft += columnNumberBeforeRemoval - aggregateExpressions.size //record how many times a duplicated column was removed (from the previous 'parseAggregateFunction' run)
                                aggregateExpressions.add(path)
                                path
                            }
                    criteriaQuery.groupBy(groupByExpressions)
                }
                // optionally order by this aggregate function
                expression.orderBy?.let {
                    val orderCriteria =
                            when (expression.orderBy!!) {
                                // when adding column position of 'group by' shift in case columns were removed
                                Sort.Direction.ASC -> criteriaBuilder.asc(criteriaBuilder.literal<Int>(orderByColumnPosition - shiftLeft))
                                Sort.Direction.DESC -> criteriaBuilder.desc(criteriaBuilder.literal<Int>(orderByColumnPosition - shiftLeft))
                            }
                    criteriaQuery.orderBy(orderCriteria)
                }
                return aggregateExpression
            }
            else -> throw VaultQueryException("Not expecting $columnPredicate")
        }
    }

    private fun getVaultFungibleStateRoot(): Root<out Any> {
        val entityStateClass = VaultSchemaV1.VaultFungibleStates::class.java
        return rootEntities.getOrElse(entityStateClass) {
            val entityRoot = criteriaQuery.from(entityStateClass)
            rootEntities[entityStateClass] = entityRoot
            entityRoot
        }
    }

    private fun getVaultLinearStatesRoot(): Root<out Any> {
        val entityStateClass = VaultSchemaV1.VaultLinearStates::class.java
        return rootEntities.getOrElse(entityStateClass) {
            val entityRoot = criteriaQuery.from(entityStateClass)
            rootEntities[entityStateClass] = entityRoot
            entityRoot
        }
    }

    private fun getPersistentPartyRoot(): Root<out Any> {
        val persistentPartyEntity = VaultSchemaV1.PersistentParty::class.java
        return rootEntities.getOrElse(persistentPartyEntity) {
            val entityRoot = criteriaQuery.from(persistentPartyEntity)
            rootEntities[persistentPartyEntity] = entityRoot
            entityRoot
        }
    }

    override fun parseCriteria(criteria: QueryCriteria.FungibleAssetQueryCriteria): Collection<Predicate> {
        log.trace { "Parsing FungibleAssetQueryCriteria: $criteria" }

        val predicateSet = mutableSetOf<Predicate>()

        // ensure we re-use any existing instance of the same root entity
        val vaultFungibleStatesRoot = getVaultFungibleStateRoot()
        val joinPredicate = criteriaBuilder.equal(vaultStates.get<PersistentStateRef>("stateRef"),
                vaultFungibleStatesRoot.get<PersistentStateRef>("stateRef"))
        predicateSet.add(joinPredicate)

        // owner
        criteria.owner?.let {
            val owners = criteria.owner as List<AbstractParty>
            predicateSet.add(criteriaBuilder.and(vaultFungibleStatesRoot.get<AbstractParty>("owner").`in`(owners)))
        }

        // quantity
        criteria.quantity?.let {
            predicateSet.add(columnPredicateToPredicate(vaultFungibleStatesRoot.get<Long>("quantity"), it))
        }

        // issuer party
        criteria.issuer?.let {
            val issuerParties = criteria.issuer as List<AbstractParty>
            predicateSet.add(criteriaBuilder.and(vaultFungibleStatesRoot.get<AbstractParty>("issuer").`in`(issuerParties)))
        }

        // issuer reference
        criteria.issuerRef?.let {
            val issuerRefs = (criteria.issuerRef as List<OpaqueBytes>).map { it.bytes }
            predicateSet.add(criteriaBuilder.and(vaultFungibleStatesRoot.get<ByteArray>("issuerRef").`in`(issuerRefs)))
        }

        if (criteria.participants != null && criteria.exactParticipants != null)
            throw VaultQueryException("Cannot specify both participants (${criteria.participants}) and exactParticipants " +
                    "(${criteria.exactParticipants}).")

        // Participants.
        if (criteria.participants != null || criteria.exactParticipants != null) {
            // Join VaultFungibleState and PersistentParty tables (participant values are added to the common query criteria predicate)
            val statePartyToFungibleStatesJoin = criteriaBuilder.and(
                criteriaBuilder.equal(vaultFungibleStatesRoot.get<VaultSchemaV1.VaultFungibleStates>("stateRef"),
                    getPersistentPartyRoot().get<VaultSchemaV1.PersistentParty>("compositeKey").get<PersistentStateRef>("stateRef")))
            predicateSet.add(statePartyToFungibleStatesJoin)
        }

        return predicateSet
    }

    override fun parseCriteria(criteria: QueryCriteria.LinearStateQueryCriteria): Collection<Predicate> {
        log.trace { "Parsing LinearStateQueryCriteria: $criteria" }

        val predicateSet = mutableSetOf<Predicate>()

        // ensure we re-use any existing instance of the same root entity
        val vaultLinearStatesRoot = getVaultLinearStatesRoot()

        val joinPredicate = criteriaBuilder.equal(vaultStates.get<PersistentStateRef>("stateRef"),
                vaultLinearStatesRoot.get<PersistentStateRef>("stateRef"))
        predicateSet.add(joinPredicate)

        // linear ids UUID
        criteria.uuid?.let {
            val uuids = criteria.uuid as List<UUID>
            predicateSet.add(criteriaBuilder.and(vaultLinearStatesRoot.get<UUID>("uuid").`in`(uuids)))
        }

        // linear ids externalId
        criteria.externalId?.let {
            val externalIds = criteria.externalId as List<String>
            if (externalIds.isNotEmpty())
                predicateSet.add(criteriaBuilder.and(vaultLinearStatesRoot.get<String>("externalId").`in`(externalIds)))
        }

        if (criteria.participants != null && criteria.exactParticipants != null)
            throw VaultQueryException("Cannot specify both participants (${criteria.participants}) " +
                    "and exactParticipants (${criteria.exactParticipants}).")

        // Participants.
        if (criteria.participants != null || criteria.exactParticipants != null) {
            // Join VaultLinearState and PersistentParty tables (participant values are added to the common query criteria predicate)
            val statePartyToLinearStatesJoin = criteriaBuilder.and(
                criteriaBuilder.equal(vaultLinearStatesRoot.get<VaultSchemaV1.VaultLinearStates>("stateRef"),
                        getPersistentPartyRoot().get<VaultSchemaV1.PersistentParty>("compositeKey").get<PersistentStateRef>("stateRef")))
            predicateSet.add(statePartyToLinearStatesJoin)
        }

        return predicateSet
    }

    override fun <L : StatePersistable> parseCriteria(criteria: QueryCriteria.VaultCustomQueryCriteria<L>): Collection<Predicate> {
        log.trace { "Parsing VaultCustomQueryCriteria: $criteria" }

        val predicateSet = mutableSetOf<Predicate>()
        val entityStateClass = resolveEnclosingObjectFromExpression(criteria.expression)

        try {
            // ensure we re-use any existing instance of the same root entity
            val entityRoot =
                    rootEntities.getOrElse(entityStateClass) {
                        val entityRoot = criteriaQuery.from(entityStateClass)
                        rootEntities[entityStateClass] = entityRoot
                        entityRoot
                    }

            val joinPredicate = if(IndirectStatePersistable::class.java.isAssignableFrom(entityRoot.javaType)) {
                        criteriaBuilder.equal(vaultStates.get<PersistentStateRef>("stateRef"), entityRoot.get<IndirectStatePersistable<*>>("compositeKey").get<PersistentStateRef>("stateRef"))
                    } else {
                criteriaBuilder.equal(vaultStates.get<PersistentStateRef>("stateRef"), entityRoot.get<PersistentStateRef>("stateRef"))
            }
            predicateSet.add(joinPredicate)

            // resolve general criteria expressions
            @Suppress("UNCHECKED_CAST")
            parseExpression(entityRoot as Root<L>, criteria.expression, predicateSet)
        } catch (e: Exception) {
            e.message?.let { message ->
                if (message.contains("Not an entity"))
                    throw VaultQueryException("""
                    Please register the entity '${entityStateClass.name}'
                    See https://docs.corda.net/api-persistence.html#custom-schema-registration for more information""")
            }
            throw VaultQueryException("Parsing error: ${e.message}")
        }
        return predicateSet
    }

    override fun parse(criteria: QueryCriteria, sorting: Sort?): Collection<Predicate> {
        val predicateSet = criteria.visit(this)

        sorting?.let {
            if (sorting.columns.isNotEmpty())
                parse(sorting)
        }

        val selections =
                if (aggregateExpressions.isEmpty())
                    rootEntities.map { it.value }
                else
                    aggregateExpressions
        criteriaQuery.multiselect(selections)
        val combinedPredicates = commonPredicates.values.plus(predicateSet).plus(constraintPredicates).plus(joinPredicates)
        criteriaQuery.where(*combinedPredicates.toTypedArray())

        return predicateSet
    }

    override fun parseCriteria(criteria: CommonQueryCriteria): Collection<Predicate> {
        log.trace { "Parsing CommonQueryCriteria: $criteria" }

        // state status
        stateTypes = criteria.status
        if (criteria.status != Vault.StateStatus.ALL) {
            val predicateID = Pair(VaultSchemaV1.VaultStates::stateStatus.name, EQUAL)
            if (commonPredicates.containsKey(predicateID)) {
                val existingStatus = ((commonPredicates[predicateID] as ComparisonPredicate).rightHandOperand as LiteralExpression).literal
                if (existingStatus != criteria.status) {
                    log.warn("Overriding previous attribute [${VaultSchemaV1.VaultStates::stateStatus.name}] value $existingStatus with ${criteria.status}")
                    commonPredicates.replace(predicateID, criteriaBuilder.equal(vaultStates.get<Vault.StateStatus>(VaultSchemaV1.VaultStates::stateStatus.name), criteria.status))
                }
            } else {
                commonPredicates[predicateID] = criteriaBuilder.equal(vaultStates.get<Vault.StateStatus>(VaultSchemaV1.VaultStates::stateStatus.name), criteria.status)
            }
        }

        // state relevance.
        if (criteria.relevancyStatus != Vault.RelevancyStatus.ALL) {
            val predicateID = Pair(VaultSchemaV1.VaultStates::relevancyStatus.name, EQUAL)
            if (commonPredicates.containsKey(predicateID)) {
                val existingStatus = ((commonPredicates[predicateID] as ComparisonPredicate).rightHandOperand as LiteralExpression).literal
                if (existingStatus != criteria.relevancyStatus) {
                    log.warn("Overriding previous attribute [${VaultSchemaV1.VaultStates::relevancyStatus.name}] value $existingStatus with ${criteria.status}")
                    commonPredicates.replace(predicateID, criteriaBuilder.equal(vaultStates.get<Vault.RelevancyStatus>(VaultSchemaV1.VaultStates::relevancyStatus.name), criteria.relevancyStatus))
                }
            } else {
                commonPredicates[predicateID] = criteriaBuilder.equal(vaultStates.get<Vault.RelevancyStatus>(VaultSchemaV1.VaultStates::relevancyStatus.name), criteria.relevancyStatus)
            }
        }

        // contract state types
        val contractStateTypes = deriveContractStateTypes(criteria.contractStateTypes)
        if (contractStateTypes.isNotEmpty()) {
            val predicateID = Pair(VaultSchemaV1.VaultStates::contractStateClassName.name, IN)
            if (commonPredicates.containsKey(predicateID)) {
                val existingTypes = (commonPredicates[predicateID]!!.expressions[0] as InPredicate<*>).values.map { (it as LiteralExpression).literal }.toSet()
                if (existingTypes != contractStateTypes) {
                    log.warn("Enriching previous attribute [${VaultSchemaV1.VaultStates::contractStateClassName.name}] values [$existingTypes] with [$contractStateTypes]")
                    commonPredicates.replace(predicateID, criteriaBuilder.and(vaultStates.get<String>(VaultSchemaV1.VaultStates::contractStateClassName.name).`in`(contractStateTypes.plus(existingTypes))))
                }
            } else {
                commonPredicates[predicateID] = criteriaBuilder.and(vaultStates.get<String>(VaultSchemaV1.VaultStates::contractStateClassName.name).`in`(contractStateTypes))
            }
        }

        // contract constraint types
        if (criteria.constraintTypes.isNotEmpty()) {
            val predicateID = Pair(VaultSchemaV1.VaultStates::constraintType.name, IN)
            if (commonPredicates.containsKey(predicateID)) {
                val existingTypes = (commonPredicates[predicateID]!!.expressions[0] as InPredicate<*>).values.map { (it as LiteralExpression).literal }.toSet()
                if (existingTypes != criteria.constraintTypes) {
                    log.warn("Enriching previous attribute [${VaultSchemaV1.VaultStates::constraintType.name}] values [$existingTypes] with [${criteria.constraintTypes}]")
                    commonPredicates.replace(predicateID, criteriaBuilder.and(vaultStates.get<Vault.ConstraintInfo.Type>(VaultSchemaV1.VaultStates::constraintType.name).`in`(criteria.constraintTypes.plus(existingTypes))))
                }
            } else {
                commonPredicates[predicateID] = criteriaBuilder.and(vaultStates.get<Vault.ConstraintInfo.Type>(VaultSchemaV1.VaultStates::constraintType.name).`in`(criteria.constraintTypes))
            }
        }

        // contract constraint information (type and data)
        if (criteria.constraints.isNotEmpty()) {
            criteria.constraints.forEach { constraint ->
                val predicateConstraintType = criteriaBuilder.equal(vaultStates.get<Vault.ConstraintInfo>(VaultSchemaV1.VaultStates::constraintType.name), constraint.type())
                if (constraint.data() != null) {
                    val predicateConstraintData = criteriaBuilder.equal(vaultStates.get<Vault.ConstraintInfo>(VaultSchemaV1.VaultStates::constraintData.name), constraint.data())
                    val compositePredicate = criteriaBuilder.and(predicateConstraintType, predicateConstraintData)
                    if (constraintPredicates.isNotEmpty()) {
                        val previousPredicate = constraintPredicates.last()
                        constraintPredicates.clear()
                        constraintPredicates.add(criteriaBuilder.or(previousPredicate, compositePredicate))
                    }
                    else constraintPredicates.add(compositePredicate)
                }
                else constraintPredicates.add(criteriaBuilder.or(predicateConstraintType))
            }
        }

        // External IDs.
        if (criteria.externalIds.isNotEmpty()) {
            val ids = criteria.externalIds

            // Get the state to external id entity.
            val persistentStateToExternalIdEntity = VaultSchemaV1.StateToExternalId::class.java
            val entityRoot = rootEntities.getOrElse(persistentStateToExternalIdEntity) {
                val entityRoot = criteriaQuery.from(persistentStateToExternalIdEntity)
                rootEntities[persistentStateToExternalIdEntity] = entityRoot
                entityRoot
            }

            // Add the join and external id predicates.
            val externalIdJoin = criteriaBuilder.equal(vaultStates.get<VaultSchemaV1.VaultStates>("stateRef"), entityRoot.get<VaultSchemaV1.StateToExternalId>("compositeKey").get<PersistentStateRef>("stateRef"))
            val externalIdPredicate = criteriaBuilder.and(entityRoot.get<VaultSchemaV1.StateToExternalId>("externalId").`in`(ids))
            constraintPredicates.add(externalIdJoin)
            constraintPredicates.add(externalIdPredicate)
        }

        // Participants.
        criteria.participants?.let {
            val participants = criteria.participants!!

            // use a single predicate for querying the persistent party table (incrementally add additional parties)
            val predicateID = Pair(VaultSchemaV1.PersistentParty::x500Name.name, EQUAL)
            if (commonPredicates.containsKey(predicateID)) {
                val existingParticipants = ((((commonPredicates[predicateID]) as CompoundPredicate).expressions[0]) as InPredicate<*>)
                        .values.map { participant -> (participant as LiteralExpression<*>).literal }
                log.warn("Adding new participants: $participants to existing participants: $existingParticipants")
                commonPredicates.replace(predicateID, criteriaBuilder.and(
                        getPersistentPartyRoot().get<VaultSchemaV1.PersistentParty>("x500Name").`in`(existingParticipants + participants)))
            }
            else {
                // Get the persistent party entity.
                commonPredicates[predicateID] = criteriaBuilder.and(
                        getPersistentPartyRoot().get<VaultSchemaV1.PersistentParty>("x500Name").`in`(participants))
            }

            // Add the join for vault states to persistent entities (if this is not a Fungible nor Linear criteria query)
            if (criteria !is QueryCriteria.FungibleAssetQueryCriteria && criteria !is QueryCriteria.LinearStateQueryCriteria ) {
                val statePartyJoin = criteriaBuilder.equal(vaultStates.get<VaultSchemaV1.VaultStates>("stateRef"),
                        getPersistentPartyRoot().get<VaultSchemaV1.PersistentParty>("compositeKey").get<PersistentStateRef>("stateRef"))
                constraintPredicates.add(statePartyJoin)
            }
        }

        // Exact participants
        // Requires a tricky SQL query to ensure *only* exact matches are selected (eg. a transaction cannot have more nor less than the
        // exact participants specified in the query criteria).
        criteria.exactParticipants?.let {
            val exactParticipants = criteria.exactParticipants!!

            // obtain all transactions where other participants are not present
            val subQueryNotExists = criteriaQuery.subquery(Tuple::class.java)
            val subRoot = subQueryNotExists.from(VaultSchemaV1.PersistentParty::class.java)
            subQueryNotExists.select(subRoot.get("x500Name"))
            subQueryNotExists.where(criteriaBuilder.and(
                    criteriaBuilder.equal(vaultStates.get<VaultSchemaV1.VaultStates>("stateRef"),
                            subRoot.get<VaultSchemaV1.PersistentParty>("compositeKey").get<PersistentStateRef>("stateRef"))),
                    criteriaBuilder.not(subRoot.get<VaultSchemaV1.PersistentParty>("x500Name").`in`(exactParticipants)))
            val subQueryNotExistsPredicate = criteriaBuilder.and(criteriaBuilder.not(criteriaBuilder.exists(subQueryNotExists)))
            constraintPredicates.add(subQueryNotExistsPredicate)

            // join with transactions for each matching participant (only required where more than one)
            if (exactParticipants.size > 1)
                exactParticipants.forEach { participant ->
                    val subQueryExists = criteriaQuery.subquery(Tuple::class.java)
                    val subRootExists = subQueryExists.from(VaultSchemaV1.PersistentParty::class.java)
                    subQueryExists.select(subRootExists.get("x500Name"))
                    subQueryExists.where(criteriaBuilder.and(
                            criteriaBuilder.equal(vaultStates.get<VaultSchemaV1.VaultStates>("stateRef"),
                                subRootExists.get<VaultSchemaV1.PersistentParty>("compositeKey").get<PersistentStateRef>("stateRef"))),
                            criteriaBuilder.equal(subRootExists.get<VaultSchemaV1.PersistentParty>("x500Name"), participant))
                    val subQueryExistsPredicate = criteriaBuilder.and(criteriaBuilder.exists(subQueryExists))
                    constraintPredicates.add(subQueryExistsPredicate)
                }
        }

        return emptySet()
    }

    private fun parse(sorting: Sort) {
        log.trace { "Parsing sorting specification: $sorting" }

        val orderCriteria = mutableListOf<Order>()

        val actualSorting = if (sorting.columns.none { it.sortAttribute == SortAttribute.Standard(Sort.CommonStateAttribute.STATE_REF) }) {
            sorting.copy(columns = sorting.columns + Sort.SortColumn(SortAttribute.Standard(Sort.CommonStateAttribute.STATE_REF), Sort.Direction.ASC))
        } else {
            sorting
        }
        actualSorting.columns.map { (sortAttribute, direction) ->
            val (entityStateClass, entityStateAttributeParent, entityStateAttributeChild) =
                    when (sortAttribute) {
                        is SortAttribute.Standard -> parse(sortAttribute.attribute)
                        is SortAttribute.Custom -> Triple(sortAttribute.entityStateClass, sortAttribute.entityStateColumnName, null)
                    }
            val sortEntityRoot =
                    rootEntities.getOrElse(entityStateClass) {
                        // scenario where sorting on attributes not parsed as criteria
                        val entityRoot = criteriaQuery.from(entityStateClass)
                        rootEntities[entityStateClass] = entityRoot
                        val joinPredicate = criteriaBuilder.equal(vaultStates.get<PersistentStateRef>("stateRef"), entityRoot.get<PersistentStateRef>("stateRef"))
                        joinPredicates.add(joinPredicate)
                        entityRoot
                    }
            when (direction) {
                Sort.Direction.ASC -> {
                    if (entityStateAttributeChild != null)
                        orderCriteria.add(criteriaBuilder.asc(sortEntityRoot.get<String>(entityStateAttributeParent).get<String>(entityStateAttributeChild)))
                    else
                        orderCriteria.add(criteriaBuilder.asc(sortEntityRoot.get<String>(entityStateAttributeParent)))
                }
                Sort.Direction.DESC ->
                    if (entityStateAttributeChild != null)
                        orderCriteria.add(criteriaBuilder.desc(sortEntityRoot.get<String>(entityStateAttributeParent).get<String>(entityStateAttributeChild)))
                    else
                        orderCriteria.add(criteriaBuilder.desc(sortEntityRoot.get<String>(entityStateAttributeParent)))
            }
        }
        if (orderCriteria.isNotEmpty()) {
            criteriaQuery.orderBy(orderCriteria)
            criteriaQuery.where(*joinPredicates.toTypedArray())
        }
    }

    private fun parse(sortAttribute: Sort.Attribute): Triple<Class<out PersistentState>, String, String?> {
        return when (sortAttribute) {
            is Sort.CommonStateAttribute -> {
                Triple(VaultSchemaV1.VaultStates::class.java, sortAttribute.attributeParent, sortAttribute.attributeChild)
            }
            is Sort.VaultStateAttribute -> {
                Triple(VaultSchemaV1.VaultStates::class.java, sortAttribute.attributeName, null)
            }
            is Sort.LinearStateAttribute -> {
                Triple(VaultSchemaV1.VaultLinearStates::class.java, sortAttribute.attributeName, null)
            }
            is Sort.FungibleStateAttribute -> {
                Triple(VaultSchemaV1.VaultFungibleStates::class.java, sortAttribute.attributeName, null)
            }
            else -> throw VaultQueryException("Invalid sort attribute: $sortAttribute")
        }
    }
}