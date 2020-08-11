package chord

import (
	"fmt"
	"log"
	"math/big"
	"net"
	"net/rpc"
	"strconv"
)

type RPCNode struct {
	Node 		*LinkNode
	Listener net.Listener
	Server 	  *rpc.Server
	Port 			  int
}

func (node *RPCNode) Initialize(port int) {
	node.Node = new(LinkNode)
	node.Node.Initialize(port)

	node.Server = rpc.NewServer()
	node.Port = port

	err := node.Server.Register(node)
	if err != nil {
		log.Fatalln("Register Failed: ", err)
	}
	lis, er := net.Listen("tcp", ":"+strconv.Itoa(node.Port))
	if er != nil {
		fmt.Println("Listen Failed: ", er)
		return
	}
	node.Listener = lis
}

func (node *RPCNode) Find_Successor(id *big.Int, ans *address_type) error {
	return node.Node.Find_Successor(id, ans)
}

func (node *RPCNode) Get_Predecessor(args int, ans *address_type) error {
	return node.Node.Get_Predecessor(args, ans)
}

func (node *RPCNode) Get_Successor_List(args int, ans *[SUCCESSOR_LIST_SIZE]address_type) error {
	return node.Node.Get_Successor_List(args, ans)
}

func (node *RPCNode) Put_Value(p pair_type, ret *int) error {
	return node.Node.Put_Value(p, ret)
}

func (node *RPCNode) Put_Value_Backup(p pair_type, ret *int) error {
	return node.Node.Put_Value_Backup(p, ret)
}

func (node *RPCNode) Put_Value_Successor(p pair_type, ret *int) error {
	return node.Node.Put_Value_Successor(p, ret)
}

func (node *RPCNode) Get_Value(key string, ans *string) error {
	return node.Node.Get_Value(key, ans)
}

func (node *RPCNode) Delete_Key(key string, successful *bool) error {
	return node.Node.Delete_Key(key, successful)
}

func (node *RPCNode) Delete_Key_Backup(key string, successful *bool) error {
	return node.Node.Delete_Key_Backup(key, successful)
}

func (node *RPCNode) Delete_Key_Successor(key string, successful *bool) error {
	return node.Node.Delete_Key_Successor(key, successful)
}

func (node *RPCNode) Update_Successor(nw_scsr address_type, ret *int) error {
	return node.Node.Update_Successor(nw_scsr, ret)
}

func (node *RPCNode) Update_Predecessor(nw_prdcsr address_type, ret *int) error {
	return node.Node.Update_Predecessor(nw_prdcsr, ret)
}

func (node *RPCNode) Deliver(node_joined *big.Int, ans *map[string]string) error {
	return node.Node.Deliver(node_joined, ans)
}

func (node *RPCNode) Deliver_Backup(args int, ans *map[string]string) error {
	return node.Node.Deliver_Backup(args, ans)
}

func (node *RPCNode) Deliver_Quit(delivery *data_type, ret *int) error {
	return node.Node.Deliver_Quit(delivery, ret)
}

func (node *RPCNode) Deliver_Quit_Backup(delivery *data_type, ret *int) error {
	return node.Node.Deliver_Quit_Backup(delivery, ret)
}

func (node *RPCNode) Notify(pred *address_type, ret *int) error {
	return node.Node.Notify(pred, ret)
}