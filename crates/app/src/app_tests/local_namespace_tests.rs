use crate::execution_controlled_receipt_lot::bounded::namespace::*;
#[test]
fn local_namespace_down_tunnel_placeholders_are_offline() {
    assert!(offline_interface("lo", 9));
    for name in ["tunl0", "gre0", "gretap0", "erspan0", "ip_vti0", "ip6_vti0", "sit0", "ip6tnl0", "ip6gre0"] {
        assert!(offline_interface(name, 0x80));
        assert!(!offline_interface(name, 0x81));
    }
    assert!(!offline_interface("eth0", 0x1003));
    assert!(loopback_routes("Iface Destination Gateway Flags\n", true));
    assert!(loopback_routes("00000000 00 00000000 lo\n", false));
    assert!(!loopback_routes("Iface Destination Gateway Flags\neth0 00000000 0100007f 3\n", true));
    assert!(!loopback_routes("00000000 00 00000000 eth0\n", false));
}
