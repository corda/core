.. _node-naming:

Node identity
=============
A node's name must be a valid X.500 distinguished name. In order to be compatible with other implementations
(particularly TLS implementations), we constrain the allowed X.500 name attribute types to a subset of the minimum
supported set for X.509 certificates (specified in RFC 3280), plus the locality attribute:

* Organization (O)
* State (ST)
* Locality (L)
* Country (C)
* Organizational-unit (OU)
* Common name (CN)

Note that the serial number is intentionally excluded from Corda certificates in order to minimise scope for uncertainty in
the distinguished name format. The distinguished name qualifier has been removed due to technical issues; consideration was
given to "Corda" as qualifier, however the qualifier needs to reflect the Corda compatibility zone, not the technology involved.
There may be many Corda namespaces, but only one R3 namespace on Corda. The ordering of attributes is important.

``State`` should be avoided unless required to differentiate from other ``localities`` with the same or similar names at the
country level. For example, London (GB) would not need a ``state``, but St Ives would (there are two, one in Cornwall, one
in Cambridgeshire). As legal entities in Corda are likely to be located in major cities, this attribute is not expected to be
present in the majority of names, but is an option for the cases which require it.

The name must also obey the following constraints:

* The ``organisation``, ``locality`` and ``country`` attributes are present

    * The ``state``, ``organisational-unit`` and ``common name`` attributes are optional

* For now, the maximum number of characters in the whole x500 name (across all 6 fields) is 128 characters.

* The fields of the name have the following maximum character lengths:

    * Common name: 64
    * Organisation: 128
    * Organisation unit: 64
    * Locality: 64
    * State: 64
    * Country 2

* The ``country`` attribute is a valid `ISO 3166-1<https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2>` two letter code in upper-case

* The ``organisation`` field of the name obeys the following constraints:
    * Has at least two letters
    * Does not include the following characters: ``,`` , ``"``, ``\``
    * Is in NFKC normalization form
    * Does not contain the null character
    * Only the latin, common and inherited unicode scripts are supported
    * No double-spacing

This is to avoid right-to-left issues, debugging issues when we can't pronounce names over the phone, and
character confusability attacks.

.. note:: The network operator of a Corda Network may put additional constraints on node naming in place.

External identifiers
^^^^^^^^^^^^^^^^^^^^
Mappings to external identifiers such as Companies House nos., LEI, BIC, etc. should be stored in custom X.509
certificate extensions. These values may change for operational reasons, without the identity they're associated with
necessarily changing, and their inclusion in the distinguished name would cause significant logistical complications.
The OID and format for these extensions will be described in a further specification.
