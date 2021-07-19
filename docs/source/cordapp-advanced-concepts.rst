.. Intended reader of this document is a CorDapp developer who wants to understand how to write production-ready CorDapp kernels.
 - Introduce the basic building blocks of transaction verification and how they fit together.
 - Gradually introduce more advanced requirements like CorDapp dependencies, evolution rules.
 - Present the limitations of Corda 3 and Corda 4.
 - Proposed solutions and troubleshooting.


Advanced CorDapp Concepts
=========================

.. Preamble.

At the heart of the Corda design and security model is the idea that a transaction is valid if and only if all the `verify()` functions in
the contract code associated with each state in the transaction succeed. The contract constraints features in Corda provide a rich set
of tools for specifying and constraining which verify functions out of the universe of possibilities can legitimately be used in (attached to) a transaction.

In simple scenarios, this works as you would expect and Corda's built-in security controls ensure that your applications work as you expect them too.
However, if you move to more advanced scenarios, especially ones where your verify function depends on code from other non-Corda libraries,
code that other people's verify functions may also depend on, you need to start thinking about what happens if and when states
governed by these different pieces of code are brought together. If they both depend on a library, which common version should be used?
How do you avoid your verify function's behaviour changing unexpectedly if the wrong version of the library is used? Are you at risk of subtle attacks?
The good news is that Corda is designed to deal with these situations but the flip side is that you need to understand how this is done,
and the implications for how you package, distribute and attach your contract code to transactions.

This document provides the information you need in order to understand what happens behind the scenes and how it affects the CorDapp you are working on.


How transactions are verified in Corda
--------------------------------------

.. Recap: basic transaction structure.

Corda transactions evolve input states into output states. A state is a data structure containing: the actual data fact (that is expressed as a
strongly typed serialized java object) and a reference to the logic (contract) that needs to verify a transition to and from this state.
Corda does not embed the actual verification bytecode in transactions. The logic is expressed as a Java class name and a contract constraint
(read more in: :doc:`api-contract-constraints`), and the actual code lives in a JAR file that is referenced by the transaction.


The basic threat model and security requirement.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Being a decentralized system, anyone who can build transactions can create `.java` files, compile and bundle them in a JAR, and then reference
this code in the transaction he created. If it were possible to do this without any restrictions, an attacker seeking to steal your money,
for example, might create a transaction that transitions a `Cash` contract owned by you to one owned by the attacker.
The only thing that is protecting your `Cash` is the contract verification code, so all the attacker has to do is attach a version of the
`net.corda.finance.contracts.asset.Cash` contract class that permits this transition to occur.
So we clearly need a way to ensure that the actual code attached to a transaction purporting to implement any given contract is constrained in some way.
For example, perhaps we wish to ensure that only the specific implementation of `net.corda.finance.contracts.asset.Cash` that was specified by the initial issuer of the cash is used.
Or perhaps we wish to constrain it in some other way.

To prevent the types of attacks that can arise if there were no restrictions on which
implementations of Contract classes were attached to transactions, we provide the contract constraints mechanism to complement the class name.
This mechanism allows the state to specify exactly what code can be attached.
In Corda 4, for example, the state can say: "I'm ok to be spent if the transaction is verified by a class: `com.megacorp.megacontract.MegaContract` as
long as the JAR containing this contract is signed by `Mega Corp`".

.. Introduce the `LedgerTransaction` abstraction and how it relates to the transaction chain. Introduce the state serialization/deserialization and Classloaders.


The LedgerTranscation
^^^^^^^^^^^^^^^^^^^^^

Another relevant aspect to remember is that because states are serialised binary objects, to perform any useful operation on them they need to
be deserialized into instances of Java objects. All these instances are made available to the contract code as the `LedgerTransaction` parameter
passed to the `verify` method. The `LedgerTransaction` class abstracts away a lot of complexity and offers contracts a usable data structure where
all objects are loaded in the same classloader and can be freely used and filtered by class. This way, the contract developer can focus on the business logic.

Behind the scenes, the matter is more complex. As can be seen in this illustration:

.. image:: resources/tx-chain.png
   :scale: 20%
   :align: center

.. How The UTxO model is applied.

.. note:: Corda's design is based on the UTXO model. In a serialized transaction the input and reference states are `StateRefs` - only references
          to output states from previous transactions (see :doc:`api-transactions`).
          When building the `LedgerTransaction`, the `inputs` and `references` are resolved to Java objects created by deserialising blobs of data
          fetched from previous transactions that were in turn serialized in that context (within the classloader of that transaction - introduced here: :ref:`attachments_classloader`).
          This model has consequences when it comes to how states can be evolved. Removing a field from a newer version of a state would mean
          that when deserialising that state in the context of a transaction using the more recent code, that field could just disappear.
          In Corda 4 we implemented the no-data loss rule, which prevents this to happen. See :doc:`serialization-default-evolution`


Simple example of transaction verification.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Let's consider a very simple case, a transaction swapping `Apples` for `Oranges`. Each of the states that need to be swapped is the output of a previous transaction.
Similar to the above image the `Apples` state is the output of some previous transaction, through which it came to be possessed by the party now paying it away in return for some oranges.
The `Apples` and `Oranges` states that will be consumed in this new transaction exist as serialised `TransactionState`s.
It is these `TransactionState`s that specify the fully qualified names of the contract code that should be run to verify their consumption as well as,
importantly, the governing `constraint`s on which specific implementations of that class name can be used.
The swap transaction would contain the two input states, the two output states with the new owners of the fruit and the code to be used to deserialize and
verify the transaction as two attachment IDs - which are SHA-256 hashes of the apples and oranges CorDapps (more specifically, the contracts JAR).

.. TODO - update this note once the DJVM is integrated

.. note:: The attachment ID is a cryptographic hash of a file. Any node calculates this hash when it downloads the file from a peer (during transaction resolution) or from
          another source, and thus knows that it is the exact file that any other party verifying this transaction will use. In the current version of
          Corda - |corda_version| -, nodes won't load JARs downloaded from a peer into a classloader. This is a temporary security measure until we integrate the
          Deterministic JVM Sandbox, which will be able to isolate network loaded code from sensitive data.

This combination of fully qualified contract class name and constraint ensures that, when a state is spent, the contract code attached to the transaction
(that will ultimately determine whether the transaction is considered valid or not) meets the criteria laid down in the transaction that created that state.
For example, if a state is created with a constraint that says its consumption can only be verified by code signed by MegaCorp,
then the Corda consensus rules mean that any transaction attaching an implementation of the class that is _not_ signed by MegaCorp will not be considered valid.


Verify attachment constraints. Introduce constraints propagation.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The previous discussion explained the construction of a transaction that consumes one or more states. Now let's consider this from the perspective
of somebody verifying a transaction they are presented with.
The first thing the node has to do is to ensure that the transaction was formed correctly and then execute the contract verification logic.
Given that the input states are already agreed to be valid facts, the attached code has to be compliant with their constraints.

.. note:: The output states created by this transaction must also specify constraints and, to prevent a malicious transaction creator specifying
          constraints that enable their malicious code to take control of a state in a future transaction, these constraints must be consistent
          with those of any input states of the same type. This is explained more fully as part of the platform's 'constraints propagation' rules documentation :ref:`constraints_propagation` .

The rule for contract code attachment validity checking is that for each state there must be one and only one attachment that contains the fully qualified contract class name.
This attachment will be identified as the CorDapp JAR corresponding to that state and thus it must satisfy the constraint of that state.
For example, if one state is signature constrained, the corresponding attachment must be signed by the key specified in the state.
If this rule is breached the transaction is considered invalid even if it is signed by all the required parties, and any compliant node will refuse to execute
the verification code.

This rule, together with the no-overlap rule - which we'll introduce below - ensure that the code used to deserialize and verify the transaction is
legitimate and that there is no ambiguity when it comes to what code to execute.

.. _attachments_classloader:

Contract execution in the AttachmentsClassloader and the no-overlap rule.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

After ensuring that the contract code is correct the node needs to execute it to verify the business rules of the transaction.
This is done by creating an `AttachmentsClassloader` from all the attachments listed by the transaction, then deserialising the binary
representation of the transaction inside this classloader, creating the `LedgerTransaction` and then running the contract verification code
in this classloader.

Corda transactions can combine any states, which makes it possible that 2 different transaction attachments contain the same class name (they overlap).
This can happen legitimately or it can be a malicious party attempting to break the contract rules. Due to how Java classloaders work,
this would cause ambiguity as to what code will be executed, so an attacker could attempt to exploit this and trick other nodes that a transaction that
should be invalid is actually valid. To address this vulnerability, Corda introduces the `no-overlap` rule:

.. note:: The `no-overlap rule` is applied to the `AttachmentsClassloader` that is build for each transaction. If a file with the same path but different content exists
          in multiple attachments, the transaction is considered invalid. The reason for this is that these files provide different implementations
          of the same class and which one is loaded might depend on the implementation of the underlying JVM. This would break determinism, and
          would also open security problems. Even in the legitimate case, if a contract expects and was tested against a certain implementation,
          then running it against a different, but still legitimate implementation could cause unexpected results.

.. Why does this need to be so complicated? Cross contract references, Class identity crisis.
   Here we explain why all the attachments need to be combined.

The process described above may appear surprising and complex. Nodes have CorDapps installed anyway, so why does the code need to also be attached to the transaction?
Corda is designed to ensure that the validity of any transaction does not depend on any node specific setup and should always return the same result,
even if the transaction is verified in 20 years when the current version of the CorDapps it uses will not be installed on any node.
This attachments mechanism ensures that given the same input - the binary representation of a transaction and its back-chain, any node is and will
be able to load the same code and calculate the exact same result.

Another surprise might be the fact that if every state has its own governing code then why can't we just verify individual transitions independently?
This would simplify a lot of things.
The answer is that for a trivial case like swapping `Apples` for `Oranges` where the two contracts might not care about the other states in the
transaction, this could be a valid solution. But Corda is designed to support complex business scenarios. For example the `Apples` contract logic
can have a requirement to check that Pink Lady apples can only be traded against Valencia oranges. For this to be possible, the `Apples` contract needs to be able to find
`Orange` states in the `LedgerTransaction`, understand their properties and run logic against them. If apples and oranges were loaded in
separate classloaders then the `Apples` classloader would need to load code for `Oranges` anyway in order to perform those operations.


CorDapp dependencies
--------------------

.. Now we introduce a simple dependency. And the problems that come with this. We already established that all attachments are combined.

Exchanging Apples for Oranges is a contrived example, of course, but this pattern is not uncommon. And a common scenario is one where code
that is common to a collection of state types is abstracted into a common library.
For example, imagine Apples and Oranges both depended on a `Fruit` library developed by a third party as part of their verification logic.

This library must obviously be available to execute, since the verification logic depends on it, which in turn means it must be loaded by the Attachments Classloader.
Since the classloader is constructed solely from code attached to the transaction, it means the library must be attached to the transaction.

The question to consider as a developer of a CorDapp is: where and how should my dependencies be attached to transactions?

There are 2 options to achieve this (given the hypothetical `Apples` for `Oranges` transaction):

 1. Bundle the `Fruit` library with the CorDapp. This means creating a Fat-JAR containing all the required code.
 2. Add the dependency as another attachment to the transaction manually.

These options have pros and cons, which are now discussed:

The first approach is fairly straightforward and does not require any additional setup. Just declaring a `compile` dependency
will by default bundle the dependency with the CorDapp. One obvious drawback is that CorDapp JARs can grow quite large in case they depend on
large libraries. Other more subtle drawbacks will be discussed below.

.. _manually_attach_dependency:

The second approach is more flexible in cases where multiple applications depend on the same library but it currently requires an additional
security check to be included in the contract code. The reason is that given that anyone can create a JAR containing a class your CorDapp depends on, a malicious actor
could just create his own version of the library and attach that to the transaction instead of the legitimate one your code expects. This would allow
the attacker to change the intended behavior of your contract to his advantage.
See :ref:`contract_security` for an example.
Basically, what this manual check does is extend the security umbrella provided by the attachment constraint of the state to its dependencies.

.. note:: As soon as support is added at the platform level this code can be removed from future versions of the CorDapp.

.. warning:: In Corda 4, it is the responsibility of the CorDapp developer to ensure that all dependencies are added in a secure way.
             Bundling the dependency together with the contract code is secure, so if there are no other factors it is the preferred approach.
             If the dependency is not bundled, just adding the attachment to the transaction is not enough. The contract code, that is guaranteed
             to be correct by the constraints mechanism, must verify that all dependencies are available in the `attachments` and are not malicious.


CorDapps depending on the same library.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It should be evident now that each CorDapp must add its own dependencies to the transaction, but what happens when two CorDapps depend on different versions of the same library?
The node that is building the transaction must ensure that the attached JARs contain all code needed for all CorDapps and also do not break the `no-overlap` rule.

In the above example, if the `Apples` code depends on `Fruit v3.2` and the `Oranges` code depends on `Fruit v3.4` that would be impossible to achieve,
because of the overlap over some of the fruit classes.

A simple way to fix this problem is for CorDapps to shade this common dependency under their own namespace. This would avoid breaking the `no-overlap rule`.
The primary downside is that multiple apps using (and shading) this dependency may lose the ability in other contexts to carry out operations like casting to a common superclass.
If this is the approach taken then `Apples` and `Oranges` could not be treated as just `com.fruitcompany.Fruit` but would actually be `com.applecompany.com.fruitcompany.Fruit` or
`com.orangecompany.com.fruitcompany.Fruit`, which would not be ideal.

Also, currently, the Corda gradle plugin does not provide any tooling for shading.

.. important:: A very important point to remember as a CorDapp developer when you prepare for release is that states created with your CorDapp can in theory
          be used in transactions with any other states that are governed by CorDapps that might not exist for the next 10 years. In order to
          maximise the usefulness of your CorDapp you have to ensure that the overlap footprint is as low as possible.

The ideal solution is for CorDapps to declare their dependencies, and for the platform to be able to automatically select valid dependencies
when a transaction is built, and also to ensure that transactions are formed with the right dependencies at verification time.
This type of functionality is what we plan to implement in a future version of Corda.

Until then, because the network is not that developed and the chance of overlap is not very high, CorDapps can just choose one of the above approaches,
and in case such a clash becomes a real problem, handle it in a case by case basis.
For example the authors of the two clashing CorDapps could decide to use a certain version of the dependency and thus not trigger the no-overlap rule

.. note:: Currently the `cordapp` gradle plugin that ships with Corda only supports bundling a dependency fully unshaded, by declaring it as a `compile` dependency.
          It also supports `cordaCompile`, which assumes the dependency is available so it does not bundle it. There is no current support for shading or partial bundling.


.. Introduce the most complex case.

CorDapp depending on other CorDapp(s)
-------------------------------------

.. Present some reasonable examples. Why is FatJar not an option?

We presented the "complex" business requirement earlier where the `Apples` contract has to check that it can't allow swapping Pink Lady apples for anything
but Valencia Oranges. This requirement translates into the fact that the library that the `Apples` CorDapp depends on is itself a CorDapp (the `Oranges` CorDapp).

Let's assume the `Apples` CorDapp bundles the `Oranges` CorDapp as a fat-jar.
If someone attempts to build a swap transaction they would find it impossible:

 - If the two attachments are added to the transaction, then the `com.orangecompany.Orange` class would be found in both, and that would break the rule that states
   "There can be only one and precisely one attachment that is identified as the contract code that controls each state".
 - In case only the `Apples` CorDapp is attached then the constraint of the `Oranges` states would not pass, as the JAR would not be signed by the actual `OrangeCo`.


Another example that shows that bundling is not an option when depending on another CorDapp is if the `Fruit` library contains a ready to use `Banana` contract.
Also let's assume that the `Apples` and `Oranges` CorDapps bundle the `Fruit` library inside their distribution fat-jar.
In this case `Apples` for `Oranges` swaps would work fine if the two CorDapps use the same version of `Fruit`, but what if someone attempts to swap `Apples` for `Bananas`?
They would face the same problem as described above and would not be able to build such a transaction.


.. warning:: If, as a CorDapp developer you bundle a third party CorDapp that you depend upon, it will become impossible for anyone to build
             valid transactions that contain both your states and states from the third party CorDapp. This would severely limit the usefulness of your CorDapp.

.. The suggested solution.

The highly recommended solution for CorDapp to CorDapp dependency is to always manually attach the dependent CorDapp to the transaction.
(see :ref:`manually_attach_dependency` and :ref:`contract_security`)

.. package ownership

Another way to look at bundling third party CorDapps is from the point of view of identity. With the introduction of the `SignatureConstraint`, CorDapps will be signed
by their creator, so the signature will become part of their identity: `com.fruitcompany.Banana` @SignedBy_TheFruitCo.
But if another CorDapp developer, `OrangeCo` bundles the `Fruit` library, they must strip the signatures from `TheFruitCo` and sign the JAR themselves.
This will create a `com.fruitcompany.Banana` @SignedBy_TheOrangeCo, so there could be two types of Banana states on the network,
but "owned" by two different parties. This means that while they might have started using the same code, nothing stops these `Banana` contracts from diverging.
Parties on the network receiving a `com.fruitcompany.Banana` will need to explicitly check the constraint to understand what they received.
In Corda 4, to help avoid this type of confusion, we introduced the concept of Package Namespace Ownership. Briefly, it allows companies to claim namespaces
and anyone who encounters a class in that package that is not signed by the registered key knows is invalid.

This new feature can be used to solve the above scenario. If `TheFruitCo` claims package ownership of `com.fruitcompany`, it will prevent anyone
from bundling its code because they will not be able to sign it with the right key.

.. Other options.

.. note:: Same as for normal dependencies, CorDapp developers can use alternative strategies like shading or partial bundling if they really want to bundle the code.
          All the described drawbacks will apply.


.. _contract_security:

Code samples for dependent libraries and CorDapps
-------------------------------------------------

Add this to the flow:

.. container:: codeset

    .. sourcecode:: kotlin

        builder.addAttachment(hash_of_the_fruit_jar)

    .. sourcecode:: java

        builder.addAttachment(hash_of_the_fruit_jar);


And in the contract code verify that there is one attachment that contains the dependency.

In case the contract depends on a specific version:

.. container:: codeset

    .. sourcecode:: kotlin

        requireThat {
            "the correct fruit jar was attached to the transaction" using (tx.attachments.find {it.id == hash_of_fruit_jar} !=null)
        }

    .. sourcecode:: java

        requireThat(require -> {
            require.using("the correct fruit jar was attached to the transaction", tx.getAttachments().contains(hash_of_fruit_jar));
        ...

.. _contract_security_signed:

In case the dependency has to be signed by a known public key the contract must check that there is a JAR attached that contains that class name and is signed by the right key:

.. container:: codeset

    .. sourcecode:: kotlin

        requireThat {
            "the correct my_reusable_cordapp jar was attached to the transaction" using (tx.attachments.find {attch -> attch.containsClass(dependentClass) && SignatureAttachmentConstraint(my_public_key).isSatisfiedBy(attch)} !=null)
        }

    .. sourcecode:: java

        requireThat(require -> {
            require.using("the correct my_reusable_cordapp jar was attached to the transaction", tx.getAttachments().stream().anyMatch(attch -> containsClass(attch, dependentClass)  new SignatureAttachmentConstraint(my_public_key).isSatisfiedBy(attch))));


.. note:: Dependencies that are not Corda specific need to be imported using the `uploadAttachment` RPC command. The reason for this is that in Corda 4
          only JARs containing contracts are automatically imported in the `AttachmentStorage`. It needs to be in the `AttachmentStorage` because
          that's the only way to attach JARs to a transaction.


Changes between version 3 and version 4 of Corda
------------------------------------------------

In Corda v3 transactions were verified inside the System Classloader that contained all the installed CorDapps.
This was a temporary simplification and we explained above why it could only be short-lived.

If we consider the example from above with the `Apples` contract that depends on `Fruit`, the `Apples` CorDapp developer could have just released
the `Apples` specific code (without bundling in the dependency on `Fruit` or attaching it to the transaction ) and rely on the fact that
`Fruit` would be on the classpath during verification.

This means that in Corda 3 nodes could have formed `valid` transactions that were not entirely self-contained. In Corda 4, because we
moved transaction verification inside the `AttachmentsClassloader` these transactions would fail with `ClassNotFound` exceptions.

These incomplete transactions need to be considered valid in Corda 4 and beyond though, so the fix we added for this was to look for a `trusted` attachment
in the current node storage that contains the missing code and use that for validation.
This fix is in the spirit of the original transaction and is secure because the chosen code must have been vetted and whitelisted first by the node operator.

.. note:: The transition to the `AttachmentsClassloader` is one more step towards the intended design of Corda. Next step is to integrate the DJVM and
         nodes will be able to execute any code downloaded from peers without any manual whitelisting step. Also it will ensure that the validation
         will return the exact same result no matter on what node or when it is run.

This change also affects testing as the test classloader no longer contains the CorDapps.

.. note:: Corda 4 maintains backwards compatibility for existing data even for CorDapps that depend on other CorDapps. If your CorDapp didn't add
          all its dependencies to the transaction, the platform will find one installed on the node. There should be no special steps that node operators need to make.
          Going forward, when building new transactions there will be a warning and the node will attempt to add the right attachment.
          The contract code of the new version of the CorDapp should add the security check:  :ref:`contract_security`



The demo `finance` CorDapp
--------------------------

Corda ships with a `finance` CorDapp demo that brings some handy utilities that can be used by code in other CorDapps, some abstract base types like `OnLedgerAsset`,
but also comes with its own ready-to-use contracts like: `Cash`, `Obligation` and `Commercial Paper`.

As it is just a sample, it is signed by R3's development key, which the node is explicitly configured - but overridable - to blacklist
by default in production. This was done in order to avoid you inadvertently going live without having first determined the right approach for your solution.

Some CorDapps might depend on `finance` since Corda v3 when finance was not signed. Most likely `finance` was not bundled or attached to the transactions, but
the transactions created just worked as described above.

The path forward in this case is first of all to reconsider if depending on a sample is a good idea. If the decision is to go forward, then the CorDapp
needs to be updated with the code described here: :ref:`contract_security`.

.. warning:: The `finance` CorDapp is a sample and should not normally be used in production or depended upon in a production CorDapp. In case
             the app developer requires some code, they can just copy it under their own namespace.
