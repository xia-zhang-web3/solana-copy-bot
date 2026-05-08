use super::*;

#[test]
fn parse_token_holders_from_program_accounts_response_counts_unique_nonzero_owners() -> Result<()> {
    let response = json!({
        "jsonrpc": "2.0",
        "result": [
            {
                "account": {
                    "data": {
                        "parsed": {
                            "info": {
                                "owner": "OwnerA",
                                "tokenAmount": { "amount": "10" }
                            }
                        }
                    }
                }
            },
            {
                "account": {
                    "data": {
                        "parsed": {
                            "info": {
                                "owner": "OwnerA",
                                "tokenAmount": { "amount": "5" }
                            }
                        }
                    }
                }
            },
            {
                "account": {
                    "data": {
                        "parsed": {
                            "info": {
                                "owner": "OwnerB",
                                "tokenAmount": { "amount": "0" }
                            }
                        }
                    }
                }
            },
            {
                "account": {
                    "data": {
                        "parsed": {
                            "info": {
                                "owner": "OwnerC",
                                "tokenAmount": { "amount": "42" }
                            }
                        }
                    }
                }
            }
        ]
    });
    let holders = parse_token_holders_from_program_accounts_response(&response)?;
    assert_eq!(holders, 2);
    Ok(())
}

#[test]
fn parse_token_holders_from_program_accounts_response_accepts_wrapped_value_array() -> Result<()> {
    let response = json!({
        "jsonrpc": "2.0",
        "result": {
            "value": [
                {
                    "account": {
                        "data": {
                            "parsed": {
                                "info": {
                                    "owner": "OwnerX",
                                    "tokenAmount": { "amount": "1" }
                                }
                            }
                        }
                    }
                }
            ]
        }
    });
    let holders = parse_token_holders_from_program_accounts_response(&response)?;
    assert_eq!(holders, 1);
    Ok(())
}

#[test]
fn parse_token_holders_from_program_accounts_response_rejects_invalid_shape() {
    let response = json!({
        "jsonrpc": "2.0",
        "result": {
            "value": {
                "owner": "not-an-array"
            }
        }
    });
    let error = parse_token_holders_from_program_accounts_response(&response)
        .expect_err("invalid response shape must fail");
    assert!(
        error.to_string().contains("missing token accounts array"),
        "unexpected error: {error}"
    );
}
