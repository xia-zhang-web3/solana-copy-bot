//! Valid normal config first; each mutation must reach the intended boundary.
use copybot_config::*;
fn valid()->ExecutionConfig{toml::from_str(include_str!("fixtures/real-session.toml")).unwrap()}
#[test]
fn real_policy_positive_and_exact_cap_identity_refusals(){
    let c=valid();validate_real_session(&c).unwrap();validate_technical_cohort(&c).unwrap();
    let mut lower=c.clone();lower.real_session.as_mut().unwrap().total_fee_cap=50000;
    assert_eq!(validate_real_session(&lower).unwrap_err().to_string(),"real_session_explicit_bounds");
    let mut wrong=c.clone();wrong.real_session.as_mut().unwrap().genesis=LOCAL_SESSION_GENESIS.into();
    assert_eq!(validate_real_session(&wrong).unwrap_err().to_string(),"real_session_profile");
    let mut once=c.clone();once.real_session=None;
    assert_eq!(validate_technical_cohort(&once).unwrap_err().to_string(),"technical_cohort_window_or_count");
    let raw=toml::to_string(&c.real_session.unwrap()).unwrap()+"cycles = []\n";
    assert!(toml::from_str::<RealSession>(&raw).is_err());
}

#[test]
#[ignore="explicit offline harness runtime config preflight; never reads signer keys"]
fn verify_saved_normal_session_runtime_config(){
    let path=std::env::var("REAL_SESSION_RUNTIME_CONFIG").unwrap();
    let text=std::fs::read_to_string(path).unwrap();
    let c:AppConfig=toml::from_str(&text).unwrap();
    validate_real_session(&c.execution).unwrap();
    validate_technical_cohort(&c.execution).unwrap();
    assert!(c.execution.real_session.is_some() && c.execution.local_bounded_session.is_none() && !c.execution.enabled);
}
