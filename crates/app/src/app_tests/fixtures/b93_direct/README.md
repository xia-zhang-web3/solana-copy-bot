# Synthetic direct-chain operands

Source: repository evidence `audit/2026-09-10/batch102-review-evidence/inputs/fixtures/b91/direct`.
These are the existing explicitly synthetic B91 direct-chain inputs, not live captures.
`chain.json` records the public repeated-byte identities, quantities, slots, frame
order and synthetic timestamps. The six protobuf operands pass through the actual
replay decoder and observation-only inbox preparation. No private key is included.
DB, results and the deliberately absent signer path live in a fresh test root.
Retained capture harnesses and their explicit input contract remain separate.

SHA256 of the original bytes:

- `chain.json`: `7c88094e307aaaa34edfa978c2f6b7b21944d492f10d727cb8296ba0d1f2717e`
- `source.pb`: `1e27a249fec8b3157e81ce3cfe5d5a4768ae20b0abf6b30e4cd14155f1b5558c`
- `our.pb`: `ae267bd36f01d806395524923ce64a7ea824f97bffd3475cda6230176e618164`
- `sell.pb`: `4b12572364f4ba440166d3a11c73cea42a0baecb3d228c332d424e50eafaafe3`
- `block-100.pb`: `12203267356dc935b2785ae64965be2e81de27258c9aa053afd25529a59e6ca1`
- `block-120.pb`: `b7edd2f55e8ab2098cc72325d35452a20f0500016dfe825b7cf11c6c67feeaf4`
- `block-150.pb`: `065ceda03c066d723f93190202d264e33a11a1abf78b88e40e12b5465812c54a`
