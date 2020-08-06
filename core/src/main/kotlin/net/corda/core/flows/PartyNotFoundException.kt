package net.corda.core.flows

import net.corda.core.CordaRuntimeException
import net.corda.core.identity.CordaX500Name

class PartyNotFoundException(message: String, val party: CordaX500Name) : CordaRuntimeException(message)