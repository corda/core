Changelog
=========

Here's a summary of what's changed in each Corda release. For guidance on how to upgrade code from the previous
release, see :doc:`app-upgrade-notes`.


.. _changelog_v4.4:

Version 4.4
----------
* 

.. _changelog_v4.3:

Version 4.3
----------

* Added ability for ``CordaService`` to register for receiving node lifecycle events to perform initialisation and clean-up actions.

* Custom serializer classes implementing ``SerializationCustomSerializer`` should ideally be packaged in the same CorDapp as the
  contract classes. Corda 4 could therefore fail to verify transactions created with Corda 3 if their custom serializer classes
  had been packaged somewhere else. Add a "fallback mechanism" to Corda's transaction verification logic which will attempt to
  include any missing custom serializers from other CorDapps within ``AttachmentStorage``.
* ``AppServiceHub`` been extended to provide access to ``database`` which will enable the Service class to perform DB transactions
  from the threads managed by the custom Service.

* Moved and renamed the testing web server to the ``testing`` subproject. Also renamed the published artifact to ``corda-testserver.jar``.

* New Vault Query criteria to specify exact matches for specified participants.

* Support for Java 11 (compatibility mode). Please read https://github.com/corda/corda/pull/5356.

* Updating FinalityFlow with functionality to indicate the appropriate StatesToRecord. This allows the initiating party to record states
  from transactions which they are proposing, but are not necessarily participants of.

* Removed the RPC exception privacy feature. Previously, in production mode, the exceptions thrown on the node were stripped of all content
  when rethrown on the RPC client.

* Introduced a new parameter ``externalIds: List<UUID>`` to ``VaultQueryCriteria`` which allows CorDapp developers to constrain queries
  to a specified set of external IDs. This feature should be used when querying states for a particular "account" (see accounts CorDapp for
  further information).

* Introduced a new API on ``IdentityService`` called ``RegisterKey`` which maps a ``PublicKey`` to a specified ``CordaX500Name`` and to an
  optional ``UUID`` (external Id).

* Introduced a new API on ``IdentityService`` called ``publicKeyToExternalId` which facilitates lookups of ``PublicKey`` s to
  ``externalId`` s (Account IDs).

* Introduced a new API on ``IdentityService`` called ``publicKeysForExternalId`` which returns all the ``PublicKey`` s associated with a
  particular external ID.

* ``StatePointer`` has been marked as ```@DoNotImplement``, which was an omission in the original release.

* Introduced a new low level flow diagnostics tool: checkpoint agent (that can be used standalone or in conjunction with the ``checkpoints dump`` shell command).
  See :doc:`checkpoint-tooling` for more information.

* ``NotaryFlow.Client`` now performs transaction verification by default to prevent accidentally sending an invalid transaction to a
  non-validating notary. The behaviour can be controlled by passing a constructor parameter flag ``skipVerification``.
  Note: this only affects flows that invoke ``NotaryFlow.Client`` directly – no behavioural change if using ``FinalityFlow``.

* The MockNet now supports setting a custom Notary class name, as was already supported by normal node config. See :doc:`tutorial-custom-notary`.

* Introduced a new ``Destination`` abstraction for communicating with non-Party destinations using the new ``FlowLogic.initateFlow(Destination)``
  method. ``Party`` and ``AnonymousParty`` have been retrofitted to implement ``Destination``. Initiating a flow to an ``AnonymousParty``
  means resolving to the well-known identity ``Party`` and then communicating with that.

* Removed ``finance-workflows`` dependency on jackson library.  The functions that used jackson (e.g. ``FinanceJSONSupport``) have been moved
  into IRS Demo.
* The introductory and technical white papers have been refreshed. They have new content and a clearer organisation.

* Information about checkpointed flows can be retrieved from the shell. Calling ``checkpoints dump`` will create a zip file inside the node's
  ``log`` directory. This zip will contain a JSON representation of each checkpointed flow. This information can then be used to determine the
  state of stuck flows or flows that experienced internal errors and were kept in the node for manual intervention.

* It is now possible to re-record transactions if a node wishes to record as an observer a transaction it has participated in. If this is
  done, then the node may record new output states that are not relevant to the node.

  .. warning:: Nodes may re-record transactions if they have previously recorded them as a participant and wish to record them as an observer.
     However, the node cannot resolve the forward chain of transactions if this is done. This means that if you wish to re-record a chain of
     transactions and get the new output states to be correctly marked as consumed, the full chain must be sent to the node *in order*.

* Added ``nodeDiagnosticInfo`` to the RPC API. The new RPC is also available as the ``run nodeDiagnosticInfo`` command executable from
  the Corda shell. It retrieves version information about the Corda platform and the CorDapps installed on the node.

* ``CordaRPCClient.start`` has a new ``gracefulReconnect`` parameter. The class ``GracefulReconnect`` takes two lambdas - one for callbacks
  on disconnect, and one for callbacks on reconnection.  When provided (ie. the ``gracefulReconnect`` parameter is not null) the RPC client
  will to try to automatically reconnect to the node on disconnect. Further any ``Observable`` s previously created will continue to vend new
  events on reconnect.

  .. note:: This is only best-effort and there are no guarantees of reliability.

* Contract attachments are now automatically whitelisted by the node if another contract attachment is present with the same contract classes,
  signed by the same public keys, and uploaded by a trusted uploader. This allows the node to resolve transactions that use earlier versions
  of a contract without having to manually install that version, provided a newer version is installed. Similarly, non-contract attachments
  are whitelisted if another attachment is present on the node that is signed by the same public key.

* Package namespace ownership configurations can be now be set as described in
  :ref:`node_package_namespace_ownership`, when using the Cordformation plugin version 4.0.43.

* Wildcards can now be used when specifying RPC permissions, for example ``StartFlow.foo.bar.*`` will allow users to start any flow in the
  ``foo.bar`` package. See :ref:`rpcUsers <corda_configuration_file_rpc_users>` for more information.

* ``-XX:+HeapDumpOnOutOfMemoryError`` and ``-XX:+CrashOnOutOfMemoryError`` have been added to the default JVM options of the node.
  A node which is running out of memory is now expected to stop immediately to preserve ledger consistency and avoid flaws in operations.
  Note that it's a responsibility of a client application to handle RPC reconnection in case this happens.
  See :ref:`setting_jvm_args` and :ref:`memory_usage_and_tuning` for further details.

* :doc:`design/data-model-upgrades/package-namespace-ownership` configurations can be now be set as described in
  :ref:`node_package_namespace_ownership`, when using the Cordformation plugin version 4.0.43.

* Environment variables and system properties can now be provided with underscore separators instead of dots. Neither are case sensitive.
  See :ref:`overriding config values <corda_configuration_file_overriding_config>` for more information.

* SSH server in the :doc:`shell` has been updated to remove outdated weak ciphers and algorithms.

* Removed support for external SSH connections to the standalone shell. As a result, ``--sshd-port`` and ``--sshd-hostkey-directory``
  options, as well as ``extensions.sshd`` configuration entry, have been removed from the standalone shell.
  Available alternatives are either to use the standalone shell directly, or connect to the node embedded shell via SSH.

* Fixed the operator used by the ``notEqual`` predicate

.. _changelog_v4.1:

Version 4.1
-----------

* Fix a bug in Corda 4.0 that combined commands in ``TransactionBuilder`` if they only differed by the signers list.  The behaviour is now consistent with prior Corda releases.

* Disabled the default loading of ``hibernate-validator`` as a plugin by hibernate when a CorDapp depends on it. This change will in turn fix the
  (https://github.com/corda/corda/issues/4444) issue, because nodes will no longer need to add ``hibernate-validator`` to the ``\libs`` folder.
  For nodes that already did that, it can be safely removed when the latest Corda is installed.
  One thing to keep in mind is that if any CorDapp relied on hibernate-validator to validate Querayable JPA Entities via annotations, that will no longer happen.
  That was a bad practice anyway, because the ``ContractState`` should be validated in the Contract verify method.

.. _changelog_v4.0:

Version 4.0
-----------

* Fixed race condition between ``NodeVaultService.trackBy`` and ``NodeVaultService.notifyAll``, where there could be states that were not reflected
  in the data feed returned from ``trackBy`` (either in the query's result snapshot or the observable).

* TimedFlows (only used by the notary client flow) will never give up trying to reach the notary, as this would leave the states
  in the notarisation request in an undefined state (unknown whether the spend has been notarised, i.e. has happened, or not). Also,
  retries have been disabled for single node notaries since in this case they offer no potential benefits, unlike for a notary cluster with
  several members who might have different availability.

* New configuration property ``database.initialiseAppSchema`` with values ``UPDATE``, ``VALIDATE`` and ``NONE``.
  The property controls the behavior of the Hibernate DDL generation. ``UPDATE`` performs an update of CorDapp schemas, while
  ``VALIDATE`` only verifies their integrity.  The property does not affect the node-specific DDL handling and
  complements ``database.initialiseSchema`` to disable DDL handling altogether.

* ``JacksonSupport.createInMemoryMapper`` was incorrectly marked as deprecated and is no longer so.

* Standardised CorDapp version identifiers in jar manifests (aligned with associated cordapp Gradle plugin changes).
  Updated all samples to reflect new conventions.

* Introduction of unique CorDapp version identifiers in jar manifests for contract and flows/services CorDapps.
  Updated all sample CorDapps to reflect new conventions.
  See :ref:`CorDapp separation <cordapp_separation_ref>` for further information.

* Automatic Constraints propagation for hash-constrained states to signature-constrained states.
  This allows Corda 4 signed CorDapps using signature constraints to consume existing hash constrained states generated
  by unsigned CorDapps in previous versions of Corda.

* You can now load different CorDapps for different nodes in the node-driver and mock-network. This previously wasn't possible with the
  ``DriverParameters.extraCordappPackagesToScan`` and ``MockNetwork.cordappPackages`` parameters as all the nodes would get the same CorDapps.
  See ``TestCordapp``, ``NodeParameters.additionalCordapps`` and ``MockNodeParameters.additionalCordapps``.

* ``DriverParameters.extraCordappPackagesToScan`` and ``MockNetwork.cordappPackages`` have been deprecated as they do not support the new
  CorDapp versioning and MANIFEST metadata support that has been added. They create artificial CorDapp jars which do not preserve these
  settings and thus may produce incorrect results when testing. It is recommended ``DriverParameters.cordappsForAllNodes`` and
  ``MockNetworkParameters.cordappsForAllNodes`` be used instead.

* Fixed a problem with IRS demo not being able to simulate future dates as expected (https://github.com/corda/corda/issues/3851).

* Fixed a problem that was preventing ``Cash.generateSpend`` to be used more than once per transaction (https://github.com/corda/corda/issues/4110).

* Fixed a bug resulting in poor vault query performance and incorrect results when sorting.

* Improved exception thrown by ``AttachmentsClassLoader`` when an attachment cannot be used because its uploader is not trusted.

* Fixed deadlocks generated by starting flow from within CordaServices.

* Marked the ``Attachment`` interface as ``@DoNotImplement`` because it is not meant to be extended by CorDapp developers. If you have already
  done so, please get in contact on the usual communication channels.

* Added auto-acceptance of network parameters for network updates. This behaviour is available for a subset of the network parameters
  and is configurable via the node config. See :doc:`network-map` for more information.

* Deprecated ``SerializationContext.withAttachmentsClassLoader``. This functionality has always been disabled by flags
  and there is no reason for a CorDapp developer to use it. It is just an internal implementation detail of Corda.

* Deprecated all means to directly create a ``LedgerTransaction`` instance, as client code is only meant to get hold of a ``LedgerTransaction``
  via ``WireTransaction.toLedgerTransaction``.

* Introduced new optional network bootstrapper command line options (--register-package-owner, --unregister-package-owner)
  to register/unregister a java package namespace with an associated owner in the network parameter packageOwnership whitelist.

* BFT-Smart and Raft notary implementations have been moved to the ``net.corda.notary.experimental`` package to emphasise
  their experimental nature. Note that it is not possible to preserve the state for both types of notaries when upgrading from V3 or an earlier Corda version.

* New "validate-configuration" sub-command to ``corda.jar``, allowing to validate the actual node configuration without starting the node.

* CorDapps now have the ability to specify a minimum platform version in their MANIFEST.MF to prevent old nodes from loading them.

* CorDapps have the ability to specify a target platform version in their MANIFEST.MF as a means of indicating to the node
  the app was designed and tested on that version.

* Nodes will no longer automatically reject flow initiation requests for flows they don't know about. Instead the request will remain
  un-acknowledged in the message broker. This enables the recovery scenerio whereby any missing CorDapp can be installed and retried on node
  restart. As a consequence the initiating flow will be blocked until the receiving node has resolved the issue.

* ``FinalityFlow`` is now an inlined flow and requires ``FlowSession`` s to each party intended to receive the transaction. This is to fix the
  security problem with the old API that required every node to accept any transaction it received without any checks. Existing CorDapp
  binaries relying on this old behaviour will continue to function as previously. However, it is strongly recommended CorDapps switch to
  this new API. See :doc:`app-upgrade-notes` for further details.

* For similar reasons, ``SwapIdentitiesFlow``, from confidential-identities, is also now an inlined flow. The old API has been preserved but
  it is strongly recommended CorDapps switch to this new API. See :doc:`app-upgrade-notes` for further details.

* Introduced new optional network bootstrapper command line option (--minimum-platform-version) to set as a network parameter

* Vault storage of contract state constraints metadata and associated vault query functions to retrieve and sort by constraint type.

* New overload for ``CordaRPCClient.start()`` method allowing to specify target legal identity to use for RPC call.

* Case insensitive vault queries can be specified via a boolean on applicable SQL criteria builder operators. By default
  queries will be case sensitive.

* Getter added to ``CordaRPCOps`` for the node's network parameters.

* The RPC client library now checks at startup whether the server is of the client libraries major version or higher.
  Therefore to connect to a Corda 4 node you must use version 4 or lower of the library. This behaviour can be overridden
  by specifying a lower number in the ``CordaRPCClientConfiguration`` class.

* Removed experimental feature ``CordformDefinition``

* Added new overload of ``StartedMockNode.registerInitiatedFlow`` which allows registering custom initiating-responder flow pairs, which
  can be useful for testing error cases.

* "app", "rpc", "p2p" and "unknown" are no longer allowed as uploader values when importing attachments. These are used
  internally in security sensitive code.

* Change type of the ``checkpoint_value`` column. Please check the upgrade-notes on how to update your database.

* Removed buggy :serverNameTablePrefix: configuration.

* ``freeLocalHostAndPort``, ``freePort``, and ``getFreeLocalPorts`` from ``TestUtils`` have been deprecated as they
  don't provide any guarantee the returned port will be available which can result in flaky tests. Use ``PortAllocation.Incremental``
  instead.

* Docs for IdentityService. assertOwnership updated to correctly state that an UnknownAnonymousPartyException is thrown
  rather than IllegalStateException.

* The Corda JPA entities no longer implement java.io.Serializable, as this was causing persistence errors in obscure cases.
  Java serialization is disabled globally in the node, but in the unlikely event you were relying on these types being Java
  serializable please contact us.

* Remove all references to the out-of-process transaction verification.

* The class carpenter has a "lenient" mode where it will, during deserialisation, happily synthesis classes that implement
  interfaces that will have unimplemented methods. This is useful, for example, for object viewers. This can be turned on
  with ``SerializationContext.withLenientCarpenter``.

* Added a ``FlowMonitor`` to log information about flows that have been waiting for IO more than a configurable threshold.

* H2 database changes:
  * The node's H2 database now listens on ``localhost`` by default.
  * The database server address must also be enabled in the node configuration.
  * A new ``h2Settings`` configuration block supersedes the ``h2Port`` option.

* Improved documentation PDF quality. Building the documentation now requires ``LaTex`` to be installed on the OS.

* Add ``devModeOptions.allowCompatibilityZone`` to re-enable the use of a compatibility zone and ``devMode``

* Fixed an issue where ``trackBy`` was returning ``ContractStates`` from a transaction that were not being tracked. The
  unrelated ``ContractStates`` will now be filtered out from the returned ``Vault.Update``.

* Introducing the flow hospital - a component of the node that manages flows that have errored and whether they should
  be retried from their previous checkpoints or have their errors propagate. Currently it will respond to any error that
  occurs during the resolution of a received transaction as part of ``FinalityFlow``. In such a scenario the receiving
  flow will be parked and retried on node restart. This is to allow the node operator to rectify the situation as otherwise
  the node will have an incomplete view of the ledger.

* Fixed an issue preventing out of process nodes started by the ``Driver`` from logging to file.

* Fixed an issue with ``CashException`` not being able to deserialize after the introduction of AMQP for RPC.

* Removed -Xmx VM argument from Explorer's Capsule setup. This helps avoiding out of memory errors.

* New ``killFlow`` RPC for killing stuck flows.

* Shell now kills an ongoing flow when CTRL+C is pressed in the terminal.

* Add check at startup that all persisted Checkpoints are compatible with the current version of the code.

* ``ServiceHub`` and ``CordaRPCOps`` can now safely be used from multiple threads without incurring in database transaction problems.

* Doorman and NetworkMap url's can now be configured individually rather than being assumed to be
  the same server. Current ``compatibilityZoneURL`` configurations remain valid. See both :doc:`corda-configuration-file`
  and :doc:`permissioning` for details.

* Improved audit trail for ``FinalityFlow`` and related sub-flows.

* Notary client flow retry logic was improved to handle validating flows better. Instead of re-sending flow messages the
  entire flow is now restarted after a timeout. The relevant node configuration section was renamed from ``p2pMessagingRetry``,
  to ``flowTimeout`` to reflect the behaviour change.

* The node's configuration is only printed on startup if ``devMode`` is ``true``, avoiding the risk of printing passwords
  in a production setup.

* ``NodeStartup`` will now only print node's configuration if ``devMode`` is ``true``, avoiding the risk of printing passwords
  in a production setup.

* SLF4J's MDC will now only be printed to the console if not empty. No more log lines ending with "{}".

* ``WireTransaction.Companion.createComponentGroups`` has been marked as ``@CordaInternal``. It was never intended to be
  public and was already internal for Kotlin code.

* RPC server will now mask internal errors to RPC clients if not in devMode. ``Throwable``s implementing ``ClientRelevantError``
  will continue to be propagated to clients.

* RPC Framework moved from Kryo to the Corda AMQP implementation [Corda-847]. This completes the removal
  of ``Kryo`` from general use within Corda, remaining only for use in flow checkpointing.

* Set co.paralleluniverse.fibers.verifyInstrumentation=true in devMode.

* Node will now gracefully fail to start if one of the required ports is already in use.

* Node will now gracefully fail to start if ``devMode`` is true and ``compatibilityZoneURL`` is specified.

* Added smart detection logic for the development mode setting and an option to override it from the command line.

* Changes to the JSON/YAML serialisation format from ``JacksonSupport``, which also applies to the node shell:

  * ``WireTransaction`` now nicely outputs into its components: ``id``, ``notary``, ``inputs``, ``attachments``, ``outputs``,
    ``commands``, ``timeWindow`` and ``privacySalt``. This can be deserialized back.
  * ``SignedTransaction`` is serialised into ``wire`` (i.e. currently only ``WireTransaction`` tested) and ``signatures``,
    and can be deserialized back.

* The Vault Criteria API has been extended to take a more precise specification of which class contains a field. This
  primarily impacts Java users; Kotlin users need take no action. The old methods have been deprecated but still work -
  the new methods avoid bugs that can occur when JPA schemas inherit from each other.

* Due to ongoing work the experimental interfaces for defining custom notary services have been moved to the internal package.
  CorDapps implementing custom notary services will need to be updated, see ``samples/notary-demo`` for an example.
  Further changes may be required in the future.

* Configuration file changes:

  * Added program line argument ``on-unknown-config-keys`` to allow specifying behaviour on unknown node configuration property keys.
    Values are: [FAIL, IGNORE], default to FAIL if unspecified.
  * Introduced a placeholder for custom properties within ``node.conf``; the property key is "custom".
  * The deprecated web server now has its own ``web-server.conf`` file, separate from ``node.conf``.
  * Property keys with double quotes (e.g. "key") in ``node.conf`` are no longer allowed, for rationale refer to :doc:`corda-configuration-file`.
  * The ``issuableCurrencies`` property is no longer valid for ``node.conf``. Instead, it has been moved to the finance workflows CorDapp configuration.

* Added public support for creating ``CordaRPCClient`` using SSL. For this to work the node needs to provide client applications
  a certificate to be added to a truststore. See :doc:`tutorial-clientrpc-api`

* The node RPC broker opens 2 endpoints that are configured with ``address`` and ``adminAddress``. RPC Clients would connect
  to the address, while the node will connect to the adminAddress. Previously if ssl was enabled for RPC the ``adminAddress``
  was equal to ``address``.

* Upgraded H2 to v1.4.197

* Shell (embedded available only in dev mode or via SSH) connects to the node via RPC instead of using the ``CordaRPCOps``
  object directly. To enable RPC connectivity ensure node’s ``rpcSettings.address`` and ``rpcSettings.adminAddress`` settings
  are present.

* Changes to the network bootstrapper:

  * The whitelist.txt file is no longer needed. The existing network parameters file is used to update the current contracts
    whitelist.
  * The CorDapp jars are also copied to each nodes' ``cordapps`` directory.

* Errors thrown by a Corda node will now reported to a calling RPC client with attention to serialization and obfuscation
  of internal data.

* Serializing an inner class (non-static nested class in Java, inner class in Kotlin) will be rejected explicitly by the serialization
  framework. Prior to this change it didn't work, but the error thrown was opaque (complaining about too few arguments
  to a constructor). Whilst this was possible in the older Kryo implementation (Kryo passing null as the synthesised
  reference to the outer class) as per the Java documentation `here <https://docs.oracle.com/javase/tutorial/java/javaOO/nested.html>`_
  we are disallowing this as the paradigm in general makes little sense for contract states.

* Node can be shut down abruptly by ``shutdown`` function in ``CordaRPCOps`` or gracefully (draining flows first) through
  ``gracefulShutdown`` command from shell.

* API change: ``net.corda.core.schemas.PersistentStateRef`` fields (index and txId) are now non-nullable.
  The fields were always effectively non-nullable - values were set from non-nullable fields of other objects.
  The class is used as database Primary Key columns of other entities and databases already impose those columns as non-nullable
  (even if JPA annotation nullable=false was absent).
  In case your Cordapps use this entity class to persist data in own custom tables as non Primary Key columns refer to
  :doc:`app-upgrade-notes` for upgrade instructions.

* Adding a public method to check if a public key satisfies Corda recommended algorithm specs, ``Crypto.validatePublicKey(java.security.PublicKey)``.
  For instance, this method will check if an ECC key lies on a valid curve or if an RSA key is >= 2048bits. This might
  be required for extra key validation checks, e.g., for Doorman to check that a CSR key meets the minimum security requirements.

* Table name with a typo changed from ``NODE_ATTCHMENTS_CONTRACTS`` to ``NODE_ATTACHMENTS_CONTRACTS``.

* Node logs a warning for any ``MappedSchema`` containing a JPA entity referencing another JPA entity from a different ``MappedSchema``.
  The log entry starts with "Cross-reference between MappedSchemas".
  API: Persistence documentation no longer suggests mapping between different schemas.

* Upgraded Artemis to v2.6.2.

* Introduced the concept of "reference input states". A reference input state is a ``ContractState`` which can be referred
  to in a transaction by the contracts of input and output states but whose contract is not executed as part of the
  transaction verification process and is not consumed when the transaction is committed to the ledger but is checked
  for "current-ness". In other words, the contract logic isn't run for the referencing transaction only. It's still a
  normal state when it occurs in an input or output position. *This feature is only available on Corda networks running
  with a minimum platform version of 4.*

* A new wrapper class over ``StateRef`` is introduced, called ``ReferenceStateRef``. Although "reference input states" are stored as
  ``StateRef`` objects in ``WireTransaction``, we needed a way to distinguish between "input states" and "reference input states" when
  required to filter by object type. Thus, when one wants to filter-in all "reference input states" in a ``FilteredTransaction``
  then he/she should check if it is of type ``ReferenceStateRef``.

* Removed type parameter ``U`` from ``tryLockFungibleStatesForSpending`` to allow the function to be used with ``FungibleState``
  as well as ``FungibleAsset``. This _might_ cause a compile failure in some obscure cases due to the removal of the type
  parameter from the method. If your CorDapp does specify types explicitly when using this method then updating the types
  will allow your app to compile successfully. However, those using type inference (e.g. using Kotlin) should not experience
  any changes. Old CorDapp JARs will still work regardless.

* ``issuer_ref`` column in ``FungibleStateSchema`` was updated to be nullable to support the introduction of the
  ``FungibleState`` interface. The ``vault_fungible_states`` table can hold both ``FungibleAssets`` and ``FungibleStates``.

* CorDapps built by ``corda-gradle-plugins`` are now signed and sealed JAR files.
  Signing can be configured or disabled, and it defaults to using the Corda development certificate.

* Finance CorDapps are now built as sealed and signed JAR files.
  Custom classes can no longer be placed in the packages defined in either finance Cordapp or access it's non-public members.

* Finance CorDapp was split into two separate apps: ``corda-finance-contracts`` and ``corda-finance-workflows``. There is
  no longer a single cordapp which provides both. You need to have both JARs installed in the node simultaneously for the
  app to work however.

* All sample CorDapps were split into separate apps: workflows and contracts to reflect new convention. It is recommended to structure your CorDapps
  this way, see :doc:`app-upgrade-notes` on upgrading your CorDapp.

* The format of the shell commands' output can now be customized via the node shell, using the ``output-format`` command.

* The ``node_transaction_mapping`` database table has been folded into the ``node_transactions`` database table as an additional column.

* Logging for P2P and RPC has been separated, to make it easier to enable all P2P or RPC logging without hand-picking loggers for individual classes.

* Vault Query Criteria have been enhanced to allow filtering by state relevancy. Queries can request all states, just relevant ones, or just non relevant ones. The default is to return all states, to maintain backwards compatibility.
  Note that this means apps running on nodes using Observer node functionality should update their queries to request only relevant states if they are only expecting to see states in which they participate.

* Postgres dependency was updated to version 42.2.5

* Test ``CordaService`` s can be installed on mock nodes using ``UnstartedMockNode.installCordaService``.

* The finance-contracts demo CorDapp has been slimmed down to contain only that which is relevant for contract verification. Everything else
  has been moved to the finance-workflows CorDapp:

  * The cash selection logic. ``AbstractCashSelection`` is now in net.corda.finance.contracts.asset so any custom implementations must now be
    defined in ``META-INF/services/net.corda.finance.workflows.asset.selection.AbstractCashSelection``.

  * The jackson annotations on ``Expression`` have been removed. You will need to use ``FinanceJSONSupport.registerFinanceJSONMappers`` if
    you wish to preserve the JSON format for this class.

  * The various utility methods defined in ``Cash`` for creating cash transactions have been moved to ``net.corda.finance.workflows.asset.CashUtils``.
    Similarly with ``CommercialPaperUtils`` and ``ObligationUtils``.

  * Various other utilities such as ``GetBalances`` and the test calendar data.

  The only exception to this is ``Interpolator`` and related classes. These are now in the `IRS demo workflows CorDapp <https://github.com/corda/corda/tree/master/samples/irs-demo/cordapp/workflows-irs>`_.

* Vault states are migrated when moving from V3 to V4: the relevancy column is correctly filled, and the state party table is populated.
  Note: This means Corda can be slow to start up for the first time after upgrading from V3 to V4.
