package chord

import (
	"crypto/sha1"
	"fmt"
	"math/big"
	"net"
	"net/rpc"
)

func Get_SHA1(addr string) *big.Int {
	ans := sha1.Sum([]byte(addr))
	return new(big.Int).SetBytes(ans[:])
}

func Range_Check(val, beg, end *big.Int, RightClosure bool) bool {
	if end.Cmp(beg)==1 {
		return (val.Cmp(beg)==1 && val.Cmp(end)==-1) || (RightClosure && val.Cmp(end)==0)
	} else {
		return val.Cmp(beg)==1 || val.Cmp(end)==-1 || (RightClosure && val.Cmp(end)==0)
	}
}

func Power_Two(id *big.Int, exponent uint) *big.Int {
	ans := big.NewInt(1)
	ans.Lsh(ans, exponent)
	ans.Add(ans, id)
	if ans.BitLen() > RING_SIZE {
		tmp := big.NewInt(1)
		ans.Mod(ans, tmp.Lsh(tmp, uint(RING_SIZE)))
	}
	return ans
}

func GetLocalAddress() (localaddress string) {
	ifaces, err := net.Interfaces()
	if err != nil {
		panic("init: failed to find network interfaces")
	}

	// find the first non-loopback interface with an IP address
	for _, elt := range ifaces {
		if elt.Flags&net.FlagLoopback == 0 && elt.Flags&net.FlagUp != 0 {
			addrs, err := elt.Addrs()
			if err != nil {
				panic("init: failed to get addresses for network interface")
			}

			for _, addr := range addrs {
				ipnet, ok := addr.(*net.IPNet)
				if ok {
					if ip4 := ipnet.IP.To4(); len(ip4) == net.IPv4len {
						localaddress = ip4.String()
						break
					}
				}
			}
		}
	}
	if localaddress == "" {
		panic("init: failed to find non-loopback interface with valid address on this node")
	}

	return localaddress
}

func Close(client *rpc.Client) error {
	err := client.Close()
	if err != nil {
		fmt.Println("Client Closure Failed: ", err)
	}
	return err
}

func Dial(addr string) (*rpc.Client, error) {
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		fmt.Println("Dialing Failed: ", err)
	}
	return client, err
}
