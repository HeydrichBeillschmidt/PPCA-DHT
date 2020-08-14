package main

import (
	"chord"
	"fmt"
)

/* In this file, you should implement function "NewNode" and
 * a struct which implements the interface "dhtNode".
 */


func NewNode(port int) dhtNode {
	// Todo: create a Node and then return it.
	tmp := new(NetNode)
	tmp.core = new(chord.RPCNode)
	chord.Initialize(tmp.core, port)

	var result dhtNode
	result = tmp
	return result
}

// Todo: implement a struct which implements the interface "dhtNode".

type NetNode struct {
	core *chord.RPCNode
}

func (node *NetNode) Run() {
	node.core.Node.Enabled = true
	go node.core.Server.Accept(node.core.Listener)
}

func (node *NetNode) Create() {
	node.core.Node.Create()
	node.core.Node.Backgrounds()
}

func (node *NetNode) Join(addr string) (successful bool) {
	successful = node.core.Node.Join(addr)
	node.core.Node.Backgrounds()
	return
}

func (node *NetNode) Quit() {
	if node.core.Node.Enabled == false {
		return
	}
	node.core.Node.Quit()
	node.ForceQuit()
}

func (node *NetNode) ForceQuit() {
	node.core.Node.Enabled = false
	err := node.core.Listener.Close()
	if err != nil {
		fmt.Println("Listener Closure Failed: ", err)
	}
}

func (node *NetNode) Ping(addr string) bool {
	return node.core.Node.Ping(addr)
}

func (node *NetNode) Put(key, value string) bool {
	return node.core.Node.Put(key, value)
}

func (node *NetNode) Get(key string) (bool, string) {
	return node.core.Node.Get(key)
}

func (node *NetNode) Delete(key string) bool {
	return node.core.Node.Delete(key)
}
