package client

import "testing"

func TestReplicaOrderLocality(t *testing.T) {
	local, remote1, remote2 := "127.0.0.1:7001", "192.0.2.1:7001", "192.0.2.2:7001"
	addrs := []string{remote1, local, remote2}
	for _, avoid := range []bool{false, true} {
		c := &Client{opt: Options{AvoidLocal: avoid}, local: localIPs()}
		for i := 0; i < 20; i++ {
			chain, reads := c.chainOrder(addrs), c.orderReplicas(addrs)
			if len(chain) != 3 || len(reads) != 3 {
				t.Fatalf("lost replicas: chain %v reads %v", chain, reads)
			}
			first, last := 0, 2
			want := first
			if avoid {
				want = last
			}
			if chain[want] != local || reads[want] != local {
				t.Fatalf("avoid=%v: chain %v reads %v; local replica should be at %d", avoid, chain, reads, want)
			}
		}
	}
}
