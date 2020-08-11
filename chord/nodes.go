package chord

import (
	"errors"
	"fmt"
	"log"
	"math/big"
	"net/rpc"
	"strconv"
	"sync"
	"time"
)

const (
	RING_SIZE           int = 160
	FINGER_TABLE_SIZE       = RING_SIZE
	SUCCESSOR_LIST_SIZE int = 10
	//SEARCH_TIMES_LIMIT  int = 32
)

type LinkNode struct {
	Successor   [SUCCESSOR_LIST_SIZE]address_type
	Finger      [FINGER_TABLE_SIZE]address_type
	Predecessor *address_type

	ID			 *big.Int
	Addr 		   string

	Data        data_type
	Data_Backup data_type

	node_lock sync.Mutex

	Enabled bool

	finger_cnt int
}

type address_type struct {
	Addr string
	ID   *big.Int
}

type data_type struct {
	_M_data map[string]string
	data_lock sync.Mutex
}

type pair_type struct {
	Key, Data string
}

func (node *LinkNode) get_address() address_type {
	return create_address(node.Addr, node.ID)
}

func create_address(addr string, id *big.Int) address_type {
	return address_type{addr, new(big.Int).Set(id)}
}

func (node *LinkNode) Find_Successor(id *big.Int, ans *address_type) error {
	node.validate_successors()
	if id.Cmp(node.ID)==0 || node.Addr == node.Successor[0].Addr {
		*ans = node.get_address()
	} else if Range_Check(id, node.ID, node.Successor[0].ID, true) {
		*ans = node.Successor[0]
	} else {
		cpn := node.closest_preceding_node(id)
		client, err := rpc.Dial("tcp", cpn.Addr)
		if err != nil {
			fmt.Println("Dialing Failed: ", err)
			return err
		}
		err = client.Call("RPCNode.Find_Successor", id, ans)
		if err != nil {
			fmt.Println("RPC Call Failed: Find_Successor: ", err)
			_ = Close(client)
			return err
		}
		err = client.Close()
		if err != nil {
			fmt.Println("Client Closure Failed: ", err)
			return err
		}
	}
	return nil
}

func (node *LinkNode) closest_preceding_node(id *big.Int) address_type {
	for i := RING_SIZE - 1; i >= 0; i -= 1 {
		fg := node.Finger[i]
		if Range_Check(fg.ID, node.ID, id, true) && node.Ping(fg.Addr) {
			return fg
		}
	}
	return node.get_address()
}

func (node *LinkNode) Get_Predecessor(args int, ans *address_type) error {
	if node.Predecessor==nil {
		return errors.New("predecessor not found")
	}
	*ans = create_address(node.Predecessor.Addr, node.Predecessor.ID)
	return nil
}

func (node *LinkNode) Get_Successor_List(args int, ans *[SUCCESSOR_LIST_SIZE]address_type) error {
	node.node_lock.Lock()
	for i := 0; i < SUCCESSOR_LIST_SIZE; i++ {
		(*ans)[i] = create_address(node.Successor[i].Addr, node.Successor[i].ID)
	}
	node.node_lock.Unlock()
	return nil
}

func (node *LinkNode) Put_Value(p pair_type, ret *int) error {
	node.Data.data_lock.Lock()
	node.Data._M_data[p.Key] = p.Data
	node.Data.data_lock.Unlock()
	return nil
}

func (node *LinkNode) Put_Value_Backup(p pair_type, ret *int) error {
	node.Data_Backup.data_lock.Lock()
	node.Data_Backup._M_data[p.Key] = p.Data
	node.Data_Backup.data_lock.Unlock()
	return nil
}

func (node *LinkNode) Put_Value_Successor(p pair_type, ret *int) error {
	node.validate_successors()
	if node.Ping(node.Successor[0].Addr) == false {
		fmt.Println("Chord Ring Unconnected: ", node.Successor[0].Addr)
		return errors.New("chord ring unconnected")
	}
	client, err := rpc.Dial("tcp", node.Successor[0].Addr)
	if err != nil {
		fmt.Println("Dialing Failed: ", err)
		return err
	}
	err = client.Call("RPCNode.Put_Value_Backup", p, nil)
	if err != nil {
		fmt.Println("RPC Call Failed: Put_Value_Backup: ", err)
		_ = Close(client)
		return err
	}
	err = client.Close()
	if err != nil {
		fmt.Println("Client Closure Failed: ", err)
		return err
	}
	return nil
}

func (node *LinkNode) Get_Value(key string, ans *string) error {
	node.Data.data_lock.Lock()
	value, successful := node.Data._M_data[key]
	node.Data.data_lock.Unlock()

	if !successful {
		return errors.New("key not found")
	}
	*ans = value
	return nil
}

func (node *LinkNode) Delete_Key(key string, successful *bool) error {
	node.Data.data_lock.Lock()
	defer node.Data.data_lock.Unlock()
	_, found := node.Data._M_data[key]
	if found {
		delete(node.Data._M_data, key)
		*successful = true
	} else {
		*successful = false
	}
	return nil
}

func (node *LinkNode) Delete_Key_Backup(key string, successful *bool) error {
	node.Data_Backup.data_lock.Lock()
	defer node.Data_Backup.data_lock.Unlock()
	_, found := node.Data_Backup._M_data[key]
	if found {
		delete(node.Data_Backup._M_data, key)
		*successful = true
	} else {
		*successful = false
	}
	return nil
}

func (node *LinkNode) Delete_Key_Successor(key string, successful *bool) error {
	node.validate_successors()
	if node.Ping(node.Successor[0].Addr) == false {
		fmt.Println("Chord Ring Unconnected: ", node.Successor[0].Addr)
		return errors.New("chord ring unconnected")
	}
	client, err := rpc.Dial("tcp", node.Successor[0].Addr)
	if err != nil {
		fmt.Println("Dialing Failed: ", err)
		return err
	}
	err = client.Call("RPCNode.Delete_Key_Backup", key, successful)
	if err != nil {
		_ = client.Close()
		fmt.Println("RPC Call Failed: Delete_Key_Backup: ", err)
		return err
	}
	err = client.Close()
	if err != nil {
		fmt.Println("Client Closure Failed: ", err)
		return err
	}
	return nil
}

func (node *LinkNode) Update_Successor(nw_scsr address_type, ret *int) error {
	node.Successor[0] = nw_scsr
	if node.Ping(node.Successor[0].Addr) == false {
		fmt.Println("Chord Ring Unconnected: ", node.Successor[0].Addr)
		return errors.New("chord ring unconnected")
	}
	client, err := rpc.Dial("tcp", node.Successor[0].Addr)
	if err != nil {
		fmt.Println("Dialing Failed: ", err)
		return err
	}
	var tmp_list [SUCCESSOR_LIST_SIZE]address_type
	err = client.Call("RPCNode.Get_Successor_List", 0, &tmp_list)
	if err != nil {
		fmt.Println("RPC Call Failed: Get_Successor_List: ", err)
		_ = Close(client)
		return err
	}
	err = client.Close()
	if err != nil {
		fmt.Println("Client Closure Failed: ", err)
		return err
	}
	sccrlist := []address_type{node.Successor[0]}
	sccrlist = append(sccrlist, tmp_list[:SUCCESSOR_LIST_SIZE-1]...)
	node.node_lock.Lock()
	copy(node.Successor[:], sccrlist)
	node.node_lock.Unlock()
	return nil
}

func (node *LinkNode) Update_Predecessor(nw_prdcsr address_type, ret *int) error {
	*node.Predecessor = nw_prdcsr
	return nil
}

func (node *LinkNode) Deliver(node_joined *big.Int, ans *map[string]string) error {
	node.Data.data_lock.Lock()
	defer node.Data.data_lock.Unlock()
	node.Data_Backup.data_lock.Lock()
	defer node.Data_Backup.data_lock.Unlock()
	for k, v := range node.Data._M_data {
		ID := Get_SHA1(k)
		if Range_Check(ID, node.Predecessor.ID, node_joined, true) {
			(*ans)[k] = v
			node.Data_Backup._M_data[k] = v
		}
	}
	for k := range *ans {
		delete(node.Data._M_data, k)
	}
	return nil
}

func (node *LinkNode) Deliver_Backup(args int, ans *map[string]string) error {
	node.Data_Backup.data_lock.Lock()
	for k, v := range node.Data_Backup._M_data {
		(*ans)[k] = v
	}
	node.Data_Backup.data_lock.Unlock()
	return nil
}

func (node *LinkNode) Deliver_Quit(delivery *data_type, ret *int) error {
	node.validate_successors()
	if node.Ping(node.Successor[0].Addr) == false {
		fmt.Println("Chord Ring Unconnected: ", node.Successor[0].Addr)
		return errors.New("chord ring unconnected")
	}
	client, err := rpc.Dial("tcp", node.Successor[0].Addr)
	if err != nil {
		fmt.Println("Dialing Failed: ", err)
		return err
	}
	delivery.data_lock.Lock()
	node.Data.data_lock.Lock()
	for k, v := range delivery._M_data {
		node.Data._M_data[k] = v
		err = client.Call("RPCNode.Put_Value_Backup", pair_type{k,v}, new(bool))
		if err != nil {
			delivery.data_lock.Unlock()
			node.Data.data_lock.Unlock()
			_ = Close(client)
			return err
		}
	}
	delivery.data_lock.Unlock()
	node.Data.data_lock.Unlock()
	err = client.Close()
	if err != nil {
		fmt.Println("Client Closure Failed: ", err)
		return err
	}
	return nil
}

func (node *LinkNode) Deliver_Quit_Backup(delivery *data_type, ret *int) error {
	node.Data_Backup.data_lock.Lock()
	node.Data_Backup._M_data = delivery._M_data
	node.Data_Backup.data_lock.Unlock()
	return nil
}

func (node *LinkNode) transmit_to_successor() {
	if node.Ping(node.Successor[0].Addr) == false {
		fmt.Println("Chord Ring Unconnected: ", node.Successor[0].Addr)
		return
	}
	client, failed := Dial(node.Successor[0].Addr)
	if failed {
		return
	}
	err := client.Call("RPCNode.Delivery_Quit", &node.Data, nil)
	if err != nil {
		fmt.Println("RPC Call Failed: Delivery_Quit: ", err)
		_ = Close(client)
		return
	}
	err = client.Call("RPCNode.Delivery_Quit_Backup", &node.Data_Backup, nil)
	if err != nil {
		fmt.Println("RPC Call Failed: Delivery_Quit_Backup: ", err)
		_ = Close(client)
		return
	}
	_ = Close(client)
}

func (node *LinkNode) Stabilize() {
	node.validate_successors()
	x := node.Successor[0]
	client, failed := Dial(x.Addr)
	if failed {
		return
	}
	var predecessor_of_successor address_type
	err := client.Call("RPCNode.Get_Predecessor", 0, &predecessor_of_successor)
	if err != nil {
		fmt.Println("RPC Call Failed: Get_Predecessor: ", err)
		_ = Close(client)
		return
	}
	if Range_Check(predecessor_of_successor.ID, node.ID, x.ID, false) {
		node.node_lock.Lock()
		node.Successor[0] = predecessor_of_successor
		node.node_lock.Unlock()
	}

	//notify
	if x.ID != predecessor_of_successor.ID {
		_ = Close(client)
		x = predecessor_of_successor
		client, failed = Dial(x.Addr)
		if failed {
			return
		}
	}
	err = client.Call("RPCNode.Notify", node.get_address(), nil)
	if err != nil {
		fmt.Println("RPC Call Failed: Notify: ", err)
		return
	}
	_ = Close(client)
}

func (node *LinkNode) Notify(pred *address_type, ret *int) error {
	if node.Predecessor == nil || Range_Check(pred.ID, node.Predecessor.ID, node.ID,false) {
		node.Predecessor = pred
	}
	return nil
}

func (node *LinkNode) validate_successors() {
	if node.Successor[0].Addr == node.Addr{
		return
	}
	node.node_lock.Lock()
	var pt = 0
	for p, scsr := range node.Successor {
		if node.Ping(scsr.Addr) {
			pt = p
			break
		}
	}
	if pt == 0 {
		node.node_lock.Unlock()
		return
	}
	node.Successor[0] = node.Successor[pt]
	node.node_lock.Unlock()

	if node.Ping(node.Successor[0].Addr) == false {
		fmt.Println("Chord Ring Unconnected: ", node.Successor[0].Addr)
		return
	}
	client, failed := Dial(node.Successor[0].Addr)
	if failed {
		return
	}
	var tmp_list [SUCCESSOR_LIST_SIZE]address_type
	err := client.Call("RPCNode.Get_Successor_List", 0, &tmp_list)
	if err != nil {
		fmt.Println("RPC Call Failed: Get_Successor_List: ", err)
		_ = Close(client)
		return
	}
	if Close(client) {
		return
	}
	sccrlist := []address_type{node.Successor[0]}
	sccrlist = append(sccrlist, tmp_list[:SUCCESSOR_LIST_SIZE-1]...)
	node.node_lock.Lock()
	copy(node.Successor[:], sccrlist)
	node.node_lock.Unlock()
}

func (node *LinkNode) Fix_Fingers() {
	node.node_lock.Lock()
	defer node.node_lock.Unlock()
	tmp := new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(node.finger_cnt)), nil)
	var fixed_finger address_type
	err := node.Find_Successor(new(big.Int).Add(tmp, node.ID), &fixed_finger)
	if err != nil {
		fmt.Println("RPC Call in Fix_Fingers Failed: Find_Successor: ", err)
		return
	}
	node.Finger[node.finger_cnt] = fixed_finger
	node.finger_cnt++
	if node.finger_cnt == RING_SIZE {
		node.finger_cnt = 0
	}
}

func (node *LinkNode) Check_Predecessor() {
	node.node_lock.Lock()
	if !(node.Ping(node.Predecessor.Addr)) {
		node.Predecessor = nil
	}
	node.node_lock.Unlock()
}

/*----------------------------------------------------------------------*/

func (node *LinkNode) Initialize(port int) {
	node.Addr = Get_Local_Address() + ":" + strconv.Itoa(port)
	node.ID = Get_SHA1(node.Addr)
	node.Data._M_data = make(map[string]string)
	node.Data_Backup._M_data = make(map[string]string)
}

func (node *LinkNode) Create() {
	node.Predecessor = nil
	node.node_lock.Lock()
	defer node.node_lock.Unlock()
	for i := 0; i < SUCCESSOR_LIST_SIZE; i++ {
		node.Successor[i] = node.get_address()
	}
}

func (node *LinkNode) Join(addr string) {
	if node.Ping(addr) == false {
		fmt.Println("Chord Ring Unconnected: ", addr)
		return
	}

	// set predecessor and first successor
	node.Predecessor = nil

	client, failed := Dial(addr)
	if failed {
		return
	}
	err := client.Call("RPCNode.Find_Successor", node.ID, &node.Successor[0])
	if err != nil {
		fmt.Println("RPC Call Failed: Find_Successor: ", err)
		_ = Close(client)
		return
	}
	_ = Close(client)

	// set successor list
	if node.Ping(node.Successor[0].Addr) == false {
		fmt.Println("Chord Ring Unconnected: ", node.Successor[0].Addr)
		return
	}
	client, failed = Dial(node.Successor[0].Addr)
	if failed {
		return
	}
	var tmp_list [SUCCESSOR_LIST_SIZE]address_type
	err = client.Call("RPCNode.Get_Successor_List", 0, &tmp_list)
	if err != nil {
		fmt.Println("RPC Call Failed: Get_Successor_List: ", err)
		_ = Close(client)
		return
	}
	sccrlist := []address_type{node.Successor[0]}
	sccrlist = append(sccrlist, tmp_list[:SUCCESSOR_LIST_SIZE-1]...)
	node.node_lock.Lock()
	copy(node.Successor[:], sccrlist)
	node.node_lock.Unlock()

	// data handling
	node.Data.data_lock.Lock()
	err = client.Call("RPCNode.Deliver", new(big.Int).Set(node.ID), &node.Data._M_data)
	node.Data.data_lock.Unlock()
	if err != nil {
		fmt.Println("RPC Call Failed: Deliver: ", err)
		return
	}
	node.Data_Backup.data_lock.Lock()
	err = client.Call("RPCNode.Deliver_Backup", 0, &node.Data_Backup._M_data)
	node.Data_Backup.data_lock.Unlock()
	if err != nil {
		fmt.Println("RPC Call Failed: Deliver_Backup: ", err)
		return
	}

	// notify
	err = client.Call("RPCNode.Notify", node.get_address(), nil)
	if err != nil {
		fmt.Println("RPC Call Failed: Notify: ", err)
		return
	}

	_ = Close(client)
	time.Sleep(time.Millisecond*300)
}

func (node *LinkNode) Quit() {
	node.validate_successors()
	if node.Successor[0].Addr == node.Addr {
		node.Enabled = false
		return
	}

	node.transmit_to_successor()

	// update successor of predecessor
	if node.Ping(node.Predecessor.Addr) {
		fmt.Println("Chord Ring Unconnected: Predecessor")
		return
	}
	client, failed := Dial(node.Predecessor.Addr)
	if failed {
		return
	}
	err := client.Call("RPCNode.Update_Successor", node.Successor[0], nil)
	if err != nil {
		fmt.Println("RPC Call Failed: Update_Successor: ", err)
		_ = Close(client)
		return
	}
	_ = Close(client)

	// update predecessor of successor
	if node.Ping(node.Successor[0].Addr) {
		fmt.Println("Chord Ring Unconnected: ", node.Successor[0].Addr)
		return
	}
	client, failed = Dial(node.Successor[0].Addr)
	if failed {
		return
	}
	err = client.Call("RPCNode.Update_Predecessor", *node.Predecessor, nil)
	if err != nil {
		fmt.Println("RPC Call Failed: Update_Predecessor: ", err)
		_ = Close(client)
		return
	}
	_ = Close(client)

	node.Enabled = false
}

func (node *LinkNode) Ping(addr string) bool {
	if addr == "" {
		return false
	}
	for i := 0; i < 3; i++ {
		ch := make(chan bool)
		go func() {
			client, err := rpc.Dial("tcp", addr)
			if err == nil {
				err = client.Close()
				ch <- true
				if err != nil {
					fmt.Println("Client Closure Failed: ", err)
				}
			} else {
				ch <- false
			}
		} ()
		select {
		case right := <-ch:
			if right {
				return true
			} else {
				continue
			}
		case <-time.After(time.Millisecond*300):
			break
		}
	}
	log.Fatalln("Ping Failed: ", node.Addr, " ping ", addr)
	return false
}

func (node *LinkNode) Put(key, value string) bool {
	ID := Get_SHA1(key)
	var dst address_type
	err := node.Find_Successor(ID, &dst)
	if err != nil {
		fmt.Println("RPC Call Failed: Find_Successor: ", err)
		return false
	}
	if node.Ping(dst.Addr) == false {
		fmt.Println("Chord Ring Unconnected: ", dst.Addr)
		return false
	}
	client, failed := Dial(dst.Addr)
	if failed {
		return false
	}
	err = client.Call("RPCNode.Put_Value", pair_type{key, value}, nil)
	if err != nil {
		fmt.Println("RPC Call Failed: Put_Value: ", err)
		_ = Close(client)
		return false
	}
	err = client.Call("RPCNode.Put_Value_Successor", pair_type{key, value}, nil)
	if err != nil {
		fmt.Println("RPC Call Failed: Put_Value_Succcessor: ", err)
		_ = Close(client)
		return false
	}
	if Close(client) {
		return false
	}
	return true
}

func (node *LinkNode) Get(key string) (bool, string) {
	ID := Get_SHA1(key)
	var dst address_type
	err := node.Find_Successor(ID, &dst)
	if err != nil {
		fmt.Println("RPC Call Failed: Find_Successor: ", err)
		return false, ""
	}
	if node.Ping(dst.Addr) == false {
		fmt.Println("Chord Ring Unconnected: ", dst.Addr)
		return false, ""
	}
	client, failed := Dial(dst.Addr)
	if failed {
		return false, ""
	}
	var value string
	err = client.Call("RPCNode.Get_Value", key, &value)
	if err != nil {
		fmt.Println("RPC Call Failed: Get_Value: ", err)
		_ = Close(client)
		return false, ""
	}
	if Close(client) {
		return false, ""
	}
	return true, value
}

func (node *LinkNode) Delete(key string) bool {
	ID := Get_SHA1(key)
	var dst address_type
	err := node.Find_Successor(ID, &dst)
	if err != nil {
		fmt.Println("RPC Call Failed: Find_Successor: ", err)
		return false
	}
	if node.Ping(dst.Addr) == false {
		fmt.Println("Chord Ring Unconnected: ", dst.Addr)
		return false
	}
	client, er := rpc.Dial("tcp", dst.Addr)
	if er != nil {
		fmt.Println("Dialing Failed: ", err)
		return false
	}
	var successful bool
	err = client.Call("RPCNode.Delete_Key", key, &successful)
	if err != nil {
		fmt.Println("RPC Call Failed: Delete_Key: ", err)
		_ = Close(client)
		return false
	}
	err = client.Call("RPCNode.Delete_Key_Successor", key, new(bool))
	if err != nil {
		fmt.Println("RPC Call Failed: Delete_Key_Succcessor: ", err)
		_ = Close(client)
		return false
	}
	if Close(client) {
		return false
	}
	return successful
}