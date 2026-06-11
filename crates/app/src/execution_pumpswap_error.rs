pub(crate) fn annotate_pumpswap_custom_errors(message: &str) -> String {
    if message.contains("pamm_custom_errors=") {
        return message.to_string();
    }
    let Some(summary) = pumpswap_custom_errors_summary_field(message) else {
        return message.to_string();
    };
    format!("{message}{summary}")
}

pub(crate) fn pumpswap_custom_errors_summary_field(message: &str) -> Option<String> {
    if let Some(existing) = existing_pumpswap_custom_errors_field(message) {
        return Some(format!(" {existing}"));
    }
    let matches = PUMPSWAP_ERRORS
        .iter()
        .filter(|(code, _)| message_contains_custom_code(message, *code))
        .map(|(code, name)| format!("{code}:{name}"))
        .collect::<Vec<_>>();
    (!matches.is_empty()).then(|| format!(" pamm_custom_errors={}", matches.join(",")))
}

pub(crate) fn is_pump_fun_bonding_curve_not_found(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("bonding curve") && lower.contains("not found")
}

fn message_contains_custom_code(message: &str, code: u64) -> bool {
    message.contains(&format!("\"Custom\":{code}"))
        || message.contains(&format!("\"Custom\": {code}"))
        || message.contains(&format!("Custom({code})"))
        || message
            .to_ascii_lowercase()
            .contains(&format!("custom program error: 0x{code:x}"))
}

fn existing_pumpswap_custom_errors_field(message: &str) -> Option<&str> {
    let start = message.find("pamm_custom_errors=")?;
    let field = message[start..]
        .split_whitespace()
        .next()
        .unwrap_or(&message[start..]);
    (!field.trim().is_empty()).then_some(field)
}

const PUMPSWAP_ERRORS: &[(u64, &str)] = &[
    (6000, "FeeBasisPointsExceedsMaximum"),
    (6001, "ZeroBaseAmount"),
    (6002, "ZeroQuoteAmount"),
    (6003, "TooLittlePoolTokenLiquidity"),
    (6004, "ExceededSlippage"),
    (6005, "InvalidAdmin"),
    (6006, "UnsupportedBaseMint"),
    (6007, "UnsupportedQuoteMint"),
    (6008, "InvalidBaseMint"),
    (6009, "InvalidQuoteMint"),
    (6010, "InvalidLpMint"),
    (6011, "AllProtocolFeeRecipientsShouldBeNonZero"),
    (6012, "UnsortedNotUniqueProtocolFeeRecipients"),
    (6013, "InvalidProtocolFeeRecipient"),
    (6014, "InvalidPoolBaseTokenAccount"),
    (6015, "InvalidPoolQuoteTokenAccount"),
    (6016, "BuyMoreBaseAmountThanPoolReserves"),
    (6017, "DisabledCreatePool"),
    (6018, "DisabledDeposit"),
    (6019, "DisabledWithdraw"),
    (6020, "DisabledBuy"),
    (6021, "DisabledSell"),
    (6022, "SameMint"),
    (6023, "Overflow"),
    (6024, "Truncation"),
    (6025, "DivisionByZero"),
    (6026, "NewSizeLessThanCurrentSize"),
    (6027, "AccountTypeNotSupported"),
    (6028, "OnlyCanonicalPumpPoolsCanHaveCoinCreator"),
    (6029, "InvalidAdminSetCoinCreatorAuthority"),
    (6030, "StartTimeInThePast"),
    (6031, "EndTimeInThePast"),
    (6032, "EndTimeBeforeStartTime"),
    (6033, "TimeRangeTooLarge"),
    (6034, "EndTimeBeforeCurrentDay"),
    (6035, "SupplyUpdateForFinishedRange"),
    (6036, "DayIndexAfterEndIndex"),
    (6037, "DayInActiveRange"),
    (6038, "InvalidIncentiveMint"),
    (6039, "BuyNotEnoughQuoteTokensToCoverFees"),
    (6040, "BuySlippageBelowMinBaseAmountOut"),
    (6041, "MayhemModeDisabled"),
    (6042, "OnlyPumpPoolsMayhemMode"),
    (6043, "MayhemModeInDesiredState"),
    (6044, "NotEnoughRemainingAccounts"),
    (6045, "InvalidSharingConfigBaseMint"),
    (6046, "InvalidSharingConfigCoinCreator"),
    (6047, "CoinCreatorMigratedToSharingConfig"),
    (6048, "CreatorVaultMigratedToSharingConfig"),
    (6049, "CashbackNotEnabled"),
    (6050, "OnlyPumpPoolsCashback"),
    (6051, "CashbackNotInDesiredState"),
    (6052, "TokensInVaultLessThanCashbackEarned"),
    (6053, "BuybackFeeRecipientNotAuthorized"),
    (6054, "AllBuybackFeeRecipientsShouldBeNonZero"),
    (6055, "NotUniqueBuybackFeeRecipients"),
    (6056, "BuybackBasisPointsOutOfRange"),
    (6057, "WrongBuybackFeeRecipientsCount"),
    (6058, "BuybackFeeRecipientMissing"),
];
