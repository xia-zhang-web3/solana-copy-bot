# Capture ledger fixture

These two files preserve the accepted September 14 VirtualLedger admission,
observation, and persistence implementation for hermetic capture adapter tests.
`provenance.json` binds the original source and fixture hashes. Store is unchanged;
engine replaces only the WSOL import with its identical constant so importing it
does not load the historical decoder or its private IDL.

The tests exercise publication, observation, restart and storage. Deferred methods
for quote/proof application and economic reports are outside this fixture; they
retain their original imports and are not invoked here. No network job is run.
