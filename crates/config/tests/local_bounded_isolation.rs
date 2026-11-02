use copybot_config::*;
#[test]
fn feature_alone_and_empty_opt_in_grant_no_native_authority() {
 let c=ExecutionConfig::default();assert!(validate_local_bounded_session(&c).is_ok());
 assert!(c.local_bounded_session.is_none());assert!(!native_first_buy_activation(&c));
}
#[cfg(not(feature="local-bounded-session"))]
#[test]
fn default_build_refuses_explicit_local_capability_before_configuration_use() {
 let mut c=ExecutionConfig::default();c.local_bounded_session=Some(LocalBoundedSession{
 policy:"local_bounded_session_v1".into(),session_id:"fixture".into(),max_cycles:2,
 gross_buy_cap:20000000,fee_cap:400000,cycles:vec![]});
 assert_eq!(validate_local_bounded_session(&c).unwrap_err().to_string(),"local_session_feature_required");
}
