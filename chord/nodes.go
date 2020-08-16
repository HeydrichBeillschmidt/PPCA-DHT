package chord

import (
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"sync"
	"time"
)

const (
	RING_SIZE           int = 160
	FINGER_TABLE_SIZE       = RING_SIZE
	SUCCESSOR_LIST_SIZE int = 10

	BACKGROUND_INTERVAL int = 200
)

type LinkNode struct {
	Successor   [SUCCESSOR_LIST_SIZE]Address_Type
	Finger      [FINGER_TABLE_SIZE]Address_Type
	Predecessor *Address_Type

	ID			 *big.Int
	Addr 		   string

	Data        Data_Type
	Data_Backup Data_Type

	node_lock sync.Mutex

	Enabled bool

	finger_cnt int
}

type Address_Type struct {
	Addr string
	ID   *big.Int
}
type Data_Type struct {
	_M_data map[string]string
	data_lock sync.Mutex
}
type Pair_Type struct {
	Key, Data string
}

func (node *LinkNode) get_address() Address_Type {
	return Address_Type{node.Addr, new(big.Int).Set(node.ID)}
}
func copy_address(addr Address_Type) Address_Type {
	return Address_Type{addr.Addr, new(big.Int).Set(addr.ID)}
}

// Chord Routing and Maintenance
/*------------------------------------------------------------------------------------------*/
func (node *LinkNode) Find_Successor(id *big.Int, ans *Address_Type) error {
	node.validate_successors()
	if id.Cmp(node.ID)==0 || node.Addr == node.Successor[0].Addr {
		*ans = node.get_address()
	} else if Range_Check(id, node.ID, node.Successor[0].ID, true) {
		*ans = node.Successor[0]
	} else {
		cpn := node.closest_preceding_node(id)
		if cpn==node.Addr {
			*ans = node.get_address()
			return nil
		}
		client, err := Dial(cpn)
		if err != nil {
			return err
		}
		err = client.Call("RPCNode.Find_Successor", id, ans)
		if err != nil {
			fmt.Println("RPC Call Failed: Find_Successor: ", err)
			_ = Close(client)
			return err
		}
		err = Close(client)
		if err != nil {
			return err
		}
	}
	return nil
}
func (node *LinkNode) closest_preceding_node(id *big.Int) string {
	node.node_lock.Lock()
	defer node.node_lock.Unlock()
	for i := RING_SIZE - 1; i >= 0; i -= 1 {
		fg := node.Finger[i]
		if fg.Addr!="" && Range_Check(fg.ID, node.ID, id, false) && node.Ping(fg.Addr) {
			return fg.Addr
		}
	}
	return node.Addr
}

func (node *LinkNode) validate_successors() {
	if node.Successor[0].Addr == node.Addr {
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

	_ = node.Update_Successor(node.Successor[0], nil)
}

func (node *LinkNode) Get_Predecessor(args int, ans *Address_Type) error {
	if node.Predecessor==nil {
		return errors.New("predecessor not found")
	}
	*ans = copy_address(*node.Predecessor)
	return nil
}
func (node *LinkNode) Get_Successor_List(args int, ans *[SUCCESSOR_LIST_SIZE]Address_Type) error {
	node.node_lock.Lock()
	for i, addr := range node.Successor {
		(*ans)[i] = copy_address(addr)
	}
	node.node_lock.Unlock()
	return nil
}

func (node *LinkNode) Update_Successor(nw_scsr Address_Type, ret *int) error {
	node.node_lock.Lock()
	defer node.node_lock.Unlock()
	node.Successor[0] = copy_address(nw_scsr)
	if node.Addr == nw_scsr.Addr {
		return nil
	}
	if node.Ping(node.Successor[0].Addr) == false {
		fmt.Println("Chord Ring Unconnected: ", node.Successor[0].Addr)
		return errors.New("chord ring unconnected")
	}
	client, err := Dial(node.Successor[0].Addr)
	if err != nil {
		return err
	}
	var tmp_list [SUCCESSOR_LIST_SIZE]Address_Type
	err = client.Call("RPCNode.Get_Successor_List", 0, &tmp_list)
	if err != nil {
		fmt.Println("RPC Call Failed in Update_Successor: Get_Successor_List: ", err)
		_ = Close(client)
		return err
	}
	err = Close(client)
	if err != nil {
		return err
	}
	sccrlist := []Address_Type{node.Successor[0]}
	sccrlist = append(sccrlist, tmp_list[:SUCCESSOR_LIST_SIZE-1]...)
	copy(node.Successor[:], sccrlist)
	return nil
}
func (node *LinkNode) Update_Predecessor(nw_prdcsr Address_Type, ret *int) error {
	*node.Predecessor = nw_prdcsr
	return nil
}

// For Join
func (node *LinkNode) link_with_successor(addr string) bool {
	client, err := Dial(addr)
	if err != nil {
		return false
	}
	if err = client.Call("RPCNode.Find_Successor", node.ID, &node.Successor[0]); err != nil {
		fmt.Println("RPC Call Failed in link_with_successor: Find_Successor: ", err)
		_ = Close(client)
		return false
	}
	if Close(client) != nil {
		return false
	}

	if err = node.Update_Successor(node.Successor[0], nil); err != nil {
		fmt.Println("Successor List Update Failed in link_with_successor: ", err)
		return false
	}
	return true
}

// For Quit
func (node *LinkNode) delink_with_predecessor() {
	if node.Predecessor==nil || !node.Ping(node.Predecessor.Addr) {
		fmt.Println("Chord Ring Unconnected: Predecessor")
		return
	}
	client, err := Dial(node.Predecessor.Addr)
	if err != nil {
		return
	}
	if err = client.Call("RPCNode.Update_Successor", node.Successor[0], nil); err != nil {
		fmt.Println("RPC Call Failed in delink_with_predecessor: Update_Successor: ", err)
		_ = Close(client)
		return
	}
	_ = Close(client)
}
func (node *LinkNode) delink_with_successor() {
	if !node.Ping(node.Successor[0].Addr) {
		fmt.Println("Chord Ring Unconnected: ", node.Successor[0].Addr)
		return
	}
	client, err := Dial(node.Successor[0].Addr)
	if err != nil {
		return
	}
	if err = client.Call("RPCNode.Update_Predecessor", *node.Predecessor, nil); err != nil {
		fmt.Println("RPC Call Failed in delink_with_successor: Update_Predecessor: ", err)
		_ = Close(client)
		return
	}
	_ = Close(client)
}

// Background Functions
func (node *LinkNode) Notify(pred Address_Type, ret *int) error {
	if node.Predecessor == nil {
		node.Predecessor = new(Address_Type)
		*node.Predecessor = pred
	} else if Range_Check(pred.ID, node.Predecessor.ID, node.ID,false) {
		*node.Predecessor = pred
	} else {
		return nil
	}
	if node.Predecessor.Addr != node.Addr {
		if !node.Ping(node.Predecessor.Addr) {
			return errors.New("chord ring unconnected")
		}
		client, err := Dial(node.Predecessor.Addr)
		if err != nil {
			return err
		}
		if err = client.Call("RPCNode.Update_Successor", node.get_address(), nil); err != nil {
			fmt.Println("RPC Call Failed in Notify: Update_Successor: ", err)
			_ = Close(client)
			return err
		}
		node.Data_Backup.data_lock.Lock()
		err = client.Call("RPCNode.Deliver_Data", 0, &node.Data_Backup._M_data)
		node.Data_Backup.data_lock.Unlock()
		if err != nil {
			fmt.Println("RPC Call Failed in Notify: Deliver_Data: ", err)
			_ = Close(client)
			return err
		}
		if err = Close(client); err != nil {
			return err
		}
	}
	node.transfer_backup()
	return nil
}
func (node *LinkNode) Stabilize() error {
	node.validate_successors()
	client, err := Dial(node.Successor[0].Addr)
	if err != nil {
		return err
	}
	defer Close(client)
	var predecessor_of_successor Address_Type
	err = client.Call("RPCNode.Get_Predecessor", 0, &predecessor_of_successor)
	if err != nil {
		fmt.Println("RPC Call Failed in Stabilize: Get_Predecessor: ", err)
		return nil
	}
	if predecessor_of_successor.Addr!="" && Range_Check(predecessor_of_successor.ID, node.ID, node.Successor[0].ID, false) {
		pclient, er := Dial(predecessor_of_successor.Addr)
		if er == nil {
			defer Close(pclient)
			err = node.Update_Successor(predecessor_of_successor, nil)
			if err != nil {
				fmt.Println("Successor List Update Failed: ", err)
			}
			// notify
			err = pclient.Call("RPCNode.Notify", node.get_address(), nil)
			if err != nil {
				fmt.Println("RPC Call Failed in Stabilize: Notify: ", err)
			}
			return er
		}
	}
	// notify
	err = client.Call("RPCNode.Notify", node.get_address(), nil)
	if err != nil {
		fmt.Println("RPC Call Failed in Stabilize: Notify: ", err)
	}
	return nil
}
func (node *LinkNode) Fix_Fingers() {
	defer func() {
		node.finger_cnt++
		if node.finger_cnt == RING_SIZE {
			node.finger_cnt = 0
		}
	}()
	_ = node.Find_Successor(Power_Two(node.ID, uint(node.finger_cnt)), &node.Finger[node.finger_cnt])
	return
}
func (node *LinkNode) Check_Predecessor() {
	if node.Predecessor == nil {
		return
	}
	if !(node.Ping(node.Predecessor.Addr)) {
		node.node_lock.Lock()
		defer node.node_lock.Unlock()
		node.Predecessor = nil
		node.validate_successors()
		if node.Addr==node.Successor[0].Addr {
			node.transfer_self()
		} else {
			node.transfer_deliver()
		}
		return
	}
}
func (node *LinkNode) Backgrounds() {
	go func() {
		for node.Enabled {
			time.Sleep(time.Millisecond*time.Duration(BACKGROUND_INTERVAL))
			node.Check_Predecessor()
			_ = node.Stabilize()
			node.Fix_Fingers()
		}
	}()
}

// Data Operations
/*------------------------------------------------------------------------------------------*/
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

func (node *LinkNode) Put_Value(p Pair_Type, ret *int) error {
	node.Data.data_lock.Lock()
	node.Data._M_data[p.Key] = p.Data
	node.Data.data_lock.Unlock()
	return nil
}
func (node *LinkNode) Put_Value_Backup(p Pair_Type, ret *int) error {
	node.Data_Backup.data_lock.Lock()
	node.Data_Backup._M_data[p.Key] = p.Data
	node.Data_Backup.data_lock.Unlock()
	return nil
}
func (node *LinkNode) Put_Value_Successor_Backup(p Pair_Type, ret *int) error {
	node.validate_successors()
	if node.Ping(node.Successor[0].Addr) == false {
		fmt.Println("Chord Ring Unconnected: ", node.Successor[0].Addr)
		return errors.New("chord ring unconnected")
	}
	client, err := Dial(node.Successor[0].Addr)
	if err != nil {
		return err
	}
	err = client.Call("RPCNode.Put_Value_Backup", p, nil)
	if err != nil {
		fmt.Println("RPC Call Failed in Put_Value_Successor_Backup: Put_Value_Backup: ", err)
		_ = Close(client)
		return err
	}
	if err = Close(client); err != nil {
		return err
	}
	return nil
}

func (node *LinkNode) Delete_Key(key string, successful *bool) error {
	node.Data.data_lock.Lock()
	defer node.Data.data_lock.Unlock()
	if _, found := node.Data._M_data[key]; found {
		delete(node.Data._M_data, key)
		*successful = true
		return nil
	}
	*successful = false
	return nil
}
func (node *LinkNode) Delete_Key_Backup(key string, successful *bool) error {
	node.Data_Backup.data_lock.Lock()
	defer node.Data_Backup.data_lock.Unlock()
	if _, found := node.Data_Backup._M_data[key]; found {
		delete(node.Data_Backup._M_data, key)
		*successful = true
		return nil
	}
	*successful = false
	return nil
}
func (node *LinkNode) Delete_Key_Successor_Backup(key string, successful *bool) error {
	node.validate_successors()
	if node.Ping(node.Successor[0].Addr) == false {
		fmt.Println("Chord Ring Unconnected: ", node.Successor[0].Addr)
		return errors.New("chord ring unconnected")
	}
	client, err := Dial(node.Successor[0].Addr)
	if err != nil {
		return err
	}
	if err = client.Call("RPCNode.Delete_Key_Backup", key, successful); err != nil {
		fmt.Println("RPC Call Failed in Delete_Key_Successor_Backup: Delete_Key_Backup: ", err)
		_ = Close(client)
		return err
	}
	if err = Close(client); err != nil {
		return err
	}
	return nil
}

// For Join
func (node *LinkNode) Deliver_Part(node_joined *big.Int, ans *map[string]string) error {
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
	node.Data_Backup._M_data = make(map[string]string)
	node.Data_Backup.data_lock.Unlock()
	return nil
}

func (node *LinkNode) Deliver_Data(args int, ans *map[string]string) error {
	node.Data.data_lock.Lock()
	for k, v := range node.Data._M_data {
		(*ans)[k] = v
	}
	node.Data.data_lock.Unlock()
	return nil
}

func (node *LinkNode) transfer_backup() {
	node.Data.data_lock.Lock()
	defer node.Data.data_lock.Unlock()
	node.Data_Backup.data_lock.Lock()
	defer node.Data_Backup.data_lock.Unlock()

	for k, v := range node.Data._M_data {
		node.Data_Backup._M_data[k] = v
	}
	return
}

// For ForceQuit
func (node *LinkNode) transfer_self() {
	node.Data.data_lock.Lock()
	defer node.Data.data_lock.Unlock()
	node.Data_Backup.data_lock.Lock()
	defer node.Data_Backup.data_lock.Unlock()

	for k, v := range node.Data_Backup._M_data {
		node.Data._M_data[k] = v
	}
	node.Data_Backup._M_data = make(map[string]string)
	return
}
func (node *LinkNode) transfer_deliver() {
	client, err := Dial(node.Successor[0].Addr)
	if err != nil {
		return
	}
	if err = client.Call("RPCNode.Receive_Quit_Backup", &node.Data_Backup._M_data, nil); err != nil {
		fmt.Println("RPC Call Failed in transfer_deliver: Receive_Quit_Backup: ", err)
		_ = Close(client)
		return
	}
	_ = Close(client)
	node.transfer_self()
}

// For Quit
func (node *LinkNode) Receive_Quit(delivery *map[string]string, ret *int) error {
	node.validate_successors()
	if node.Ping(node.Successor[0].Addr) == false {
		fmt.Println("Chord Ring Unconnected: ", node.Successor[0].Addr)
		return errors.New("chord ring unconnected")
	}
	client, err := Dial(node.Successor[0].Addr)
	if err != nil {
		return err
	}
	if err = client.Call("RPCNode.Receive_Quit_Backup", delivery, nil); err != nil {
		fmt.Println("RPC Call Failed in Receive_Quit: Receive_Quit_Backup: ", err)
		_ = Close(client)
		return err
	}
	if err = Close(client); err != nil {
		return err
	}

	node.Data.data_lock.Lock()
	defer node.Data.data_lock.Unlock()

	for k, v := range (*delivery) {
		node.Data._M_data[k] = v

		_, found := node.Data_Backup._M_data[k]
		if found {
			delete(node.Data_Backup._M_data, k)
		}
	}
	return nil
}
func (node *LinkNode) Receive_Quit_Backup(delivery *map[string]string, ret *int) error {
	node.Data_Backup.data_lock.Lock()
	defer node.Data_Backup.data_lock.Unlock()

	for k, v := range (*delivery) {
		node.Data_Backup._M_data[k] = v
	}
	return nil
}
func (node *LinkNode) transmit_to_successor() {
	if node.Ping(node.Successor[0].Addr) == false {
		fmt.Println("Chord Ring Unconnected: ", node.Successor[0].Addr)
		return
	}
	client, err := Dial(node.Successor[0].Addr)
	if err != nil {
		return
	}
	node.Data.data_lock.Lock()
	err = client.Call("RPCNode.Receive_Quit", &node.Data._M_data, nil)
	node.Data.data_lock.Unlock()
	if err != nil {
		fmt.Println("RPC Call Failed in transmit_to_successor: Receive_Quit: ", err)
		_ = Close(client)
		return
	}
	node.Data_Backup.data_lock.Lock()
	err = client.Call("RPCNode.Receive_Quit_Backup", &node.Data_Backup._M_data, nil)
	node.Data_Backup.data_lock.Unlock()
	if err != nil {
		fmt.Println("RPC Call Failed in transmit_to_successor: Receive_Quit_Backup: ", err)
		_ = Close(client)
		return
	}
	_ = Close(client)
}

// Interfaces
/*------------------------------------------------------------------------------------------*/
func (node *LinkNode) Initialize(port int) {
	node.Addr = GetLocalAddress() + ":" + strconv.Itoa(port)
	node.ID = Get_SHA1(node.Addr)
	node.Data._M_data = make(map[string]string)
	node.Data_Backup._M_data = make(map[string]string)
}

func (node *LinkNode) Create() {
	node.Predecessor = nil
	node.node_lock.Lock()
	defer node.node_lock.Unlock()
	for i, _ := range node.Successor {
		node.Successor[i] = node.get_address()
	}
}

func (node *LinkNode) Join(addr string) bool {
	if node.Ping(addr) == false {
		fmt.Println("Chord Ring Unconnected: ", addr)
		return false
	}

	// set predecessor and successor list
	node.Predecessor = nil
	if !node.link_with_successor(addr) {
		return false
	}

	// notify
	client, err := Dial(node.Successor[0].Addr)
	if err != nil {
		return false
	}
	if err = client.Call("RPCNode.Notify", node.get_address(), nil); err != nil {
		fmt.Println("RPC Call Failed in Join: Notify: ", err)
		return false
	}

	// data handling
	node.Data_Backup.data_lock.Lock()
	err = client.Call("RPCNode.Deliver_Backup", 0, &node.Data_Backup._M_data)
	node.Data_Backup.data_lock.Unlock()
	if err != nil {
		fmt.Println("RPC Call Failed in Join: Deliver_Backup: ", err)
		return false
	}
	node.Data.data_lock.Lock()
	err = client.Call("RPCNode.Deliver_Part", new(big.Int).Set(node.ID), &node.Data._M_data)
	node.Data.data_lock.Unlock()
	if err != nil {
		fmt.Println("RPC Call Failed in Join: Deliver_Part: ", err)
		return false
	}

	if Close(client) != nil {
		return false
	}
	fmt.Println("Join succeeded.")
	return true
}

func (node *LinkNode) Quit() {
	node.validate_successors()
	if node.Successor[0].Addr == node.Addr {
		node.Enabled = false
		return
	}
	node.transmit_to_successor()
	node.delink_with_predecessor()
	node.delink_with_successor()

	node.Enabled = false
}

func (node *LinkNode) Ping(addr string) bool {
	if addr == "" {
		return false
	}
	for i := 0; i < 3; i++ {
		ch := make(chan bool)
		go func() {
			client, err := Dial(addr)
			ch <- err == nil
			if err == nil {
				_ = Close(client)
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
	fmt.Println("Ping Failed: ", node.Addr, " ping ", addr)
	return false
}

func (node *LinkNode) Put(key, value string) bool {
	ID := Get_SHA1(key)
	var dst Address_Type
	err := node.Find_Successor(ID, &dst)
	if err != nil {
		fmt.Println("Func Call Failed in Put: Find_Successor: ", err)
		return false
	}
	if node.Ping(dst.Addr) == false {
		fmt.Println("Chord Ring Unconnected: ", dst.Addr)
		return false
	}
	client, er := Dial(dst.Addr)
	if er != nil {
		return false
	}
	if err = client.Call("RPCNode.Put_Value", Pair_Type{key, value}, nil); err != nil {
		fmt.Println("RPC Call Failed in Put: Put_Value: ", err)
		_ = Close(client)
		return false
	}
	if err = client.Call("RPCNode.Put_Value_Successor_Backup", Pair_Type{key, value}, nil); err != nil {
		fmt.Println("RPC Call Failed in Put: Put_Value_Succcessor_Backup: ", err)
		_ = Close(client)
		return false
	}
	if Close(client) != nil {
		return false
	}
	return true
}

func (node *LinkNode) Get(key string) (bool, string) {
	ID := Get_SHA1(key)
	var dst Address_Type
	err := node.Find_Successor(ID, &dst)
	if err != nil {
		fmt.Println("Func Call Failed in Get: Find_Successor: ", err)
		return false, ""
	}
	if node.Ping(dst.Addr) == false {
		fmt.Println("Chord Ring Unconnected: ", dst.Addr)
		return false, ""
	}
	client, er := Dial(dst.Addr)
	if er != nil {
		return false, ""
	}
	var value string
	if err = client.Call("RPCNode.Get_Value", key, &value); err != nil {
		fmt.Println("RPC Call Failed in Get: Get_Value: ", err)
		_ = Close(client)
		return false, ""
	}
	if Close(client) != nil {
		return false, ""
	}
	return true, value
}

func (node *LinkNode) Delete(key string) bool {
	ID := Get_SHA1(key)
	var dst Address_Type
	err := node.Find_Successor(ID, &dst)
	if err != nil {
		fmt.Println("RPC Call Failed in Delete: Find_Successor: ", err)
		return false
	}
	if node.Ping(dst.Addr) == false {
		fmt.Println("Chord Ring Unconnected: ", dst.Addr)
		return false
	}
	client, er := Dial(dst.Addr)
	if er != nil {
		return false
	}
	var successful bool
	if err = client.Call("RPCNode.Delete_Key", key, &successful); err != nil {
		fmt.Println("RPC Call Failed in Delete: Delete_Key: ", err)
		_ = Close(client)
		return false
	}
	if err = client.Call("RPCNode.Delete_Key_Successor_Backup", key, new(bool)); err != nil {
		fmt.Println("RPC Call Failed in Delete: Delete_Key_Succcessor_Backup: ", err)
		_ = Close(client)
		return false
	}
	if Close(client) != nil {
		return false
	}
	return successful
}
