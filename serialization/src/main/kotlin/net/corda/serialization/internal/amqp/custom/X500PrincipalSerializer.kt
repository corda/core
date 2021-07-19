package net.corda.serialization.internal.amqp.custom

import net.corda.serialization.internal.amqp.CustomSerializer
import net.corda.serialization.internal.amqp.SerializerFactory
import javax.security.auth.x500.X500Principal

class X500PrincipalSerializer(
        factory: SerializerFactory
) : CustomSerializer.Proxy<X500Principal, X500PrincipalSerializer.X500PrincipalProxy>(
        X500Principal::class.java,
        X500PrincipalProxy::class.java,
        factory
) {
    override fun toProxy(obj: X500Principal): X500PrincipalProxy = X500PrincipalProxy(name = obj.name)

    override fun fromProxy(proxy: X500PrincipalProxy): X500Principal = X500Principal(proxy.name)

    data class X500PrincipalProxy(val name: String)
}