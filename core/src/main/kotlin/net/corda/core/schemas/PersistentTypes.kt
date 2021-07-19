package net.corda.core.schemas

import net.corda.core.KeepForDJVM
import net.corda.core.contracts.ContractState
import net.corda.core.contracts.StateRef
import net.corda.core.serialization.CordaSerializable
import org.hibernate.annotations.Immutable
import java.io.Serializable
import javax.persistence.Column
import javax.persistence.Embeddable
import javax.persistence.EmbeddedId
import javax.persistence.MappedSuperclass

//DOCSTART QueryableState
/**
 * A contract state that may be mapped to database schemas configured for this node to support querying for,
 * or filtering of, states.
 */
@KeepForDJVM
interface QueryableState : ContractState {
    /**
     * Enumerate the schemas this state can export representations of itself as.
     */
    fun supportedSchemas(): Iterable<MappedSchema>

    /**
     * Export a representation for the given schema.
     */
    fun generateMappedObject(schema: MappedSchema): PersistentState
}
//DOCEND QueryableState

//DOCSTART MappedSchema
/**
 * A database schema that might be configured for this node.  As well as a name and version for identifying the schema,
 * also list the classes that may be used in the generated object graph in order to configure the ORM tool.
 *
 * @param schemaFamily A class to fully qualify the name of a schema family (i.e. excludes version)
 * @param version The version number of this instance within the family.
 * @param mappedTypes The JPA entity classes that the ORM layer needs to be configure with for this schema.
 */
@KeepForDJVM
open class MappedSchema(schemaFamily: Class<*>,
                        val version: Int,
                        val mappedTypes: Iterable<Class<*>>) {
    val name: String = schemaFamily.name

    /**
     * Optional classpath resource containing the database changes for the [mappedTypes]
     */
    open val migrationResource: String? = null

    override fun toString(): String = "${this.javaClass.simpleName}(name=$name, version=$version)"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as MappedSchema

        if (version != other.version) return false
        if (mappedTypes != other.mappedTypes) return false
        if (name != other.name) return false

        return true
    }

    override fun hashCode(): Int {
        var result = version
        result = 31 * result + mappedTypes.hashCode()
        result = 31 * result + name.hashCode()
        return result
    }
}
//DOCEND MappedSchema

/**
 * A super class for all mapped states exported to a schema that ensures the [StateRef] appears on the database row.  The
 * [StateRef] will be set to the correct value by the framework (there's no need to set during mapping generation by the state itself).
 */
@KeepForDJVM
@MappedSuperclass
@CordaSerializable
class PersistentState(@EmbeddedId override var stateRef: PersistentStateRef? = null) : DirectStatePersistable

/**
 * Embedded [StateRef] representation used in state mapping.
 */
@KeepForDJVM
@Embeddable
@Immutable
data class PersistentStateRef(
        @Suppress("MagicNumber") // column width
        @Column(name = "transaction_id", length = 80, nullable = false)
        var txId: String,

        @Column(name = "output_index", nullable = false)
        var index: Int
) : Serializable {
    constructor(stateRef: StateRef) : this(stateRef.txhash.toString(), stateRef.index)
}

/**
 * Marker interface to denote a persistable Corda state entity that will always have a transaction id and index
 */
@KeepForDJVM
interface StatePersistable

/**
 * Marker interface to denote a persistable Corda state entity that exposes the transaction id and index as composite key called [stateRef].
 */
interface DirectStatePersistable : StatePersistable {
    val stateRef: PersistentStateRef?
}

/**
 * Marker interface to denote a persistable Corda state entity that exposes the transaction id and index as a nested composite key called [compositeKey]
 * that is itself a [DirectStatePersistable].  i.e. exposes a [stateRef].
 */
interface IndirectStatePersistable<T : DirectStatePersistable> : StatePersistable {
    val compositeKey: T
}

object MappedSchemaValidator {
    fun fieldsFromOtherMappedSchema(schema: MappedSchema) : List<SchemaCrossReferenceReport> =
            schema.mappedTypes.map { entity ->
                entity.declaredFields.filter { field ->
                    field.type.enclosingClass != null
                            && MappedSchema::class.java.isAssignableFrom(field.type.enclosingClass)
                            && hasJpaAnnotation(field.declaredAnnotations)
                            && field.type.enclosingClass != schema.javaClass
                }.map { field -> SchemaCrossReferenceReport(schema.javaClass.name, entity.simpleName, field.type.enclosingClass.name, field.name, field.type.simpleName)}
            }.flatMap { it.toSet() }

    fun methodsFromOtherMappedSchema(schema: MappedSchema) : List<SchemaCrossReferenceReport> =
            schema.mappedTypes.map { entity ->
                entity.declaredMethods.filter { method ->
                    method.returnType.enclosingClass != null
                            && MappedSchema::class.java.isAssignableFrom(method.returnType.enclosingClass)
                            && method.returnType.enclosingClass != schema.javaClass
                            && hasJpaAnnotation(method.declaredAnnotations)
                }.map { method -> SchemaCrossReferenceReport(schema.javaClass.name, entity.simpleName, method.returnType.enclosingClass.name, method.name, method.returnType.simpleName)}
            }.flatMap { it.toSet() }

    fun crossReferencesToOtherMappedSchema(schema: MappedSchema) : List<SchemaCrossReferenceReport> =
         fieldsFromOtherMappedSchema(schema) + methodsFromOtherMappedSchema(schema)

    /** Returns true if [javax.persistence] annotation expect [javax.persistence.Transient] is found. */
    private fun hasJpaAnnotation(annotations: Array<Annotation>) =
            annotations.any { annotation -> annotation.toString().startsWith("@javax.persistence.") && annotation !is javax.persistence.Transient }

    class SchemaCrossReferenceReport(private val schema: String, private val entity: String, private val referencedSchema: String,
                                     private val fieldOrMethod: String, private val fieldOrMethodType: String) {

        override fun toString() = "Cross-reference between MappedSchemas '$schema' and '$referencedSchema'. " +
                "MappedSchema '${schema.substringAfterLast(".")}' entity '$entity' field '$fieldOrMethod' is of type '$fieldOrMethodType' " +
                "defined in another MappedSchema '${referencedSchema.substringAfterLast(".")}'."

        fun toWarning() = toString() + " This may cause issues when evolving MappedSchema or migrating its data, " +
                "ensure JPA entities are defined within the same enclosing MappedSchema."
    }
}
