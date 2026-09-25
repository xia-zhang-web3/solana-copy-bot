use super::yellowstone_association::{
    limits::{Budget, Limits},
    Admission, Context, Input, Programs, Rejection, Session, YellowstoneAssociation,
};
use std::{collections::HashSet, time::Duration};
use yellowstone_grpc_proto::prelude::SubscribeUpdateBlock;

#[test]
fn old_32_block_window_rejects_33rd_before_ttl() {
    let programs = HashSet::new();
    let session = Session {
        id: [9; 16],
        generation: 1,
    };
    let limits = Limits {
        pending: Budget {
            count: 1,
            encoded_bytes: 1024,
        },
        blocks: Budget {
            count: 32,
            encoded_bytes: 64 << 20,
        },
        history: Budget {
            count: 1,
            encoded_bytes: 1024,
        },
        outputs: Budget {
            count: 1,
            encoded_bytes: 1024,
        },
        input_bytes: 8 << 20,
        metadata_bytes: 32 << 20,
        pending_ttl: Duration::from_secs(60),
        block_ttl: Duration::from_secs(60),
        history_ttl: Duration::from_secs(60),
    };
    let mut association = YellowstoneAssociation::new(
        session,
        limits,
        Programs {
            interested: &programs,
            raydium: &programs,
            pumpswap: &programs,
        },
    )
    .unwrap();
    for index in 0..33 {
        let block = SubscribeUpdateBlock {
            slot: index + 1,
            ..Default::default()
        };
        let outcome = association.push(
            Context {
                session,
                offset: Duration::from_millis(index * 400),
            },
            Input::Block(&block),
        );
        if index == 32 {
            assert_eq!(outcome, Err(Rejection::BlockCapacity));
        } else {
            assert_eq!(outcome, Ok(Admission::Block));
            assert!(association.drain().complete);
        }
    }
}
