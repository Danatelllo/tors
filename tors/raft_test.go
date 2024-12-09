package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/stretchr/testify/require"

	"raft/application"
	"raft/raft"

	"github.com/shirou/gopsutil/v3/process"
)

const (
	basePort = 8080
	numNodes = 3
)

func createRaftNode(t *testing.T, nodeID int, numNodes int) *exec.Cmd {
	cmd := exec.Command("go", "run", "main.go", fmt.Sprintf("/home/chebryakov/tors_new/tmp/%v", nodeID), strconv.Itoa(nodeID), strconv.Itoa(numNodes))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start Raft node %d: %v", nodeID, err)
	}

	fmt.Printf("%v", cmd.Process)

	go func() {
		if err := cmd.Wait(); err != nil {
			if _, ok := err.(*exec.ExitError); !ok {
				t.Fatalf("Failed to wait for Raft node to stop: %v", err)
			}
		}
	}()

	return cmd
}

func CreateRaftNodes(t *testing.T) []*exec.Cmd {
	var nodes []*exec.Cmd
	for i := 2; i < numNodes+2; i++ {
		nodes = append(nodes, createRaftNode(t, i, numNodes))
	}
	return nodes
}

func KillRaftNodes(t *testing.T, nodes []*exec.Cmd) error {
	for _, node := range nodes {
		SendSignal(t, node, syscall.SIGINT)
	}
	for j, _ := range nodes {
		if err := os.Remove(fmt.Sprintf("/home/chebryakov/tors_new/tmp/%v", j+2)); err != nil {
			fmt.Printf("Error: %v", err)
		}
	}
	return nil
}

func SendSignal(t *testing.T, node *exec.Cmd, signal syscall.Signal) error {
	p, err := process.NewProcess(int32(node.Process.Pid))
	if err != nil {
		return err
	}

	children, err := p.Children()
	if err != nil {
		fmt.Printf("Failed to get children processes: %v\n", err)
		return err
	}

	fmt.Printf("Children %v", children)

	if err := syscall.Kill(int(children[0].Pid), signal); err != nil {
		return err
	}
	return nil
}

func UnpauseRaftNode(t *testing.T, node *exec.Cmd) error {
	return SendSignal(t, node, syscall.SIGCONT)
}

func StopRaftNode(t *testing.T, node *exec.Cmd) error {
	return SendSignal(t, node, syscall.SIGSTOP)
}

func getRaftNodesAddresses(t *testing.T) []string {
	var addresses []string
	for i := 2; i < numNodes+2; i++ {
		addresses = append(addresses, fmt.Sprintf("http://127.0.0.%v:8080", i))
	}
	return addresses
}

func IsLeader(t *testing.T, addr string) (bool, error) {
	client := resty.New()
	client.SetTimeout(1 * time.Second)
	resp, err := client.R().
		SetHeader("Accept", "application/json").
		SetHeader("Content", "application/json").
		Get(fmt.Sprintf("%v/is_master", addr))
	fmt.Println(err)
	if resp.IsSuccess() {
		return true, err
	}
	return false, err
}

func LookUpLeader(t *testing.T, addresses []string) (int, error) {
	client := resty.New()
	client.SetTimeout(1 * time.Second)

	for i, addr := range addresses {
		isLeader, _ := IsLeader(t, addr)
		if isLeader {
			return i, nil
		}
	}

	return 0, errors.New("not found leader")
}

func WaitLeader(t *testing.T, addresses []string) int {
	for {
		leaderId, err := LookUpLeader(t, addresses)
		if err == nil {
			return leaderId
		}
		time.Sleep(1 * time.Second)
	}
}

func CreateCall(t *testing.T, addr string, req application.CreateReq) (error, int, raft.Value) {
	client := resty.New()
	resp, err := client.R().
		SetHeader("Accept", "application/json").
		SetHeader("Content", "application/json").
		SetBody(req).
		Post(fmt.Sprintf("%v/create", addr))

	if err != nil {
		return err, 0, raft.Value{}
	}

	var rsp raft.Value
	if err := json.Unmarshal(resp.Body(), &rsp); err != nil {
		return err, 0, raft.Value{}
	}

	return nil, resp.StatusCode(), rsp
}

func UpdateCall(t *testing.T, addr string, req application.UpdateReq) (error, int, raft.Value) {
	client := resty.New()
	resp, err := client.R().
		SetHeader("Accept", "application/json").
		SetHeader("Content", "application/json").
		SetBody(req).
		Put(fmt.Sprintf("%v/update", addr))

	if err != nil {
		return err, 0, raft.Value{}
	}

	var rsp raft.Value
	if err := json.Unmarshal(resp.Body(), &rsp); err != nil {
		return err, 0, raft.Value{}
	}

	return nil, resp.StatusCode(), rsp
}

func PatchCall(t *testing.T, addr string, req application.PatchReq) (error, int, raft.Value) {
	client := resty.New()
	resp, err := client.R().
		SetHeader("Accept", "application/json").
		SetHeader("Content", "application/json").
		SetBody(req).
		Patch(fmt.Sprintf("%v/patch", addr))

	if err != nil {
		return err, 0, raft.Value{}
	}

	var rsp raft.Value
	if err := json.Unmarshal(resp.Body(), &rsp); err != nil {
		return err, 0, raft.Value{}
	}

	return nil, resp.StatusCode(), rsp
}

func DeleteCall(t *testing.T, addr string, req application.DeleteReq) (error, int, raft.Value) {
	client := resty.New()
	resp, err := client.R().
		SetHeader("Accept", "application/json").
		SetHeader("Content", "application/json").
		SetBody(req).
		Delete(fmt.Sprintf("%v/delete", addr))

	if err != nil {
		return err, 0, raft.Value{}
	}

	var rsp raft.Value
	if err := json.Unmarshal(resp.Body(), &rsp); err != nil {
		return err, 0, raft.Value{}
	}

	return nil, resp.StatusCode(), rsp
}

func DropTraffic(t *testing.T, addr string, req application.DropTrafficReq) error {
	client := resty.New()
	_, err := client.R().
		SetHeader("Accept", "application/json").
		SetHeader("Content", "application/json").
		SetBody(req).
		Post(fmt.Sprintf("%v/drop_traffic", addr))

	if err != nil {
		return err
	}

	return nil
}

func UnblockTraffic(t *testing.T, addr string) error {
	client := resty.New()
	_, err := client.R().
		SetHeader("Accept", "application/json").
		SetHeader("Content", "application/json").
		Post(fmt.Sprintf("%v/unblock_drop_traffic", addr))

	if err != nil {
		return err
	}

	return nil
}

func GetCall(t *testing.T, addr string, req application.GetReq) (error, int, raft.Value) {
	client := resty.New()
	resp, err := client.R().
		SetHeader("Accept", "application/json").
		SetHeader("Content", "application/json").
		EnableTrace().
		SetBody(req).
		Get(fmt.Sprintf("%v/read?key=%v", addr, req.Key))

	if err != nil {
		return err, 0, raft.Value{}
	}

	var rsp raft.Value

	fmt.Printf("Resp %v", resp.Request.TraceInfo())

	if err := json.Unmarshal(resp.Body(), &rsp); err != nil {
		return err, 0, raft.Value{}
	}

	return nil, resp.StatusCode(), rsp
}

// func BlockTraffic(t *testing.T, id1 int, id2 int) {
// 	cmd := exec.Command("iptables", "-A", "INPUT", "-s", fmt.Sprintf("127.0.0.%d", id1), "-d", fmt.Sprintf("127.0.0.%d", id2), "-p", "tcp", "--dport", fmt.Sprintf("%d", 8080), "-j", "DROP", "--ipv4")
// 	// cmd := exec.Command("iptables", "-A", "OUTPUT", "-p", "tcp", "--dport", "8080", "-j", "REJECT")
// 	if err := cmd.Run(); err != nil {
// 		t.Fatalf("Failed to block traffic: %v, %v, %v", err, id1, id2)
// 	}
// }

// func UnblockTraffic(t *testing.T, id1 int, id2 int) {
// 	cmd := exec.Command("iptables", "-D", "INPUT", "-s", fmt.Sprintf("127.0.0.%d", id1), "-d", fmt.Sprintf("127.0.0.%d", id2), "-p", "tcp", "--dport", "8080", "-j", "DROP")
// 	if err := cmd.Run(); err != nil {
// 		t.Fatalf("Failed to unblock traffic: %v", err)
// 	}
// }

func TestRaftNodes(t *testing.T) {
	var nodes []*exec.Cmd = CreateRaftNodes(t)
	var addresses []string = getRaftNodesAddresses(t)
	require.Equal(t, len(addresses), 3)
	time.Sleep(7 * time.Second)
	defer KillRaftNodes(t, nodes)

	var err error
	var masterId int
	var statusCode int

	masterId = WaitLeader(t, addresses)

	require.Nil(t, err)

	var req application.CreateReq

	req.Key = "1"
	req.Value.Cnt = 2

	// do create call to master node
	var value raft.Value
	err, statusCode, value = CreateCall(t, addresses[masterId], req)

	require.Nil(t, err)
	require.Equal(t, http.StatusOK, statusCode)
	require.Equal(t, value, raft.Value{Cnt: 2})

	var getReq application.GetReq
	getReq.Key = "1"

	// do call to master with Found and Location
	err, statusCode, value = GetCall(t, addresses[masterId], getReq)

	fmt.Printf("GetCall , err %v, statusCode %v, value %v", err, statusCode, value)

	require.Nil(t, err)
	require.Equal(t, http.StatusOK, statusCode)
	require.Equal(t, value, raft.Value{Cnt: 2})

	// do call for another
	err, statusCode, value = GetCall(t, addresses[masterId], getReq)

	require.Nil(t, err)
	require.Equal(t, http.StatusOK, statusCode)
	require.Equal(t, value, raft.Value{Cnt: 2})

	StopRaftNode(t, nodes[masterId])

	newMasterId := WaitLeader(t, addresses)

	require.NotEqual(t, newMasterId, masterId)

	// new key for new master
	req.Key = "2"
	req.Value.Cnt = 10
	req.Value.Data = "data"

	err, statusCode, value = CreateCall(t, addresses[newMasterId], req)

	require.Nil(t, err)
	require.Equal(t, http.StatusOK, statusCode)
	require.Equal(t, value, raft.Value{Cnt: 10, Data: "data"})

	// do call for another here i go to the same node
	getReq.Key = "2"
	err, statusCode, value = GetCall(t, addresses[newMasterId], getReq)

	require.Nil(t, err)
	require.Equal(t, http.StatusOK, statusCode)
	require.Equal(t, value, raft.Value{Cnt: 10, Data: "data"})

	err, statusCode, value = GetCall(t, addresses[newMasterId], getReq)

	require.Nil(t, err)
	require.Equal(t, http.StatusOK, statusCode)
	require.Equal(t, value, raft.Value{Cnt: 10, Data: "data"})

	UnpauseRaftNode(t, nodes[masterId])

	time.Sleep(1 * time.Second)

	err, statusCode, value = GetCall(t, addresses[newMasterId], getReq)

	require.Nil(t, err)
	require.Equal(t, http.StatusOK, statusCode)
	require.Equal(t, value, raft.Value{Cnt: 10, Data: "data"})

}

func TestRaftDropPartial(t *testing.T) {
	var nodes []*exec.Cmd = CreateRaftNodes(t)
	var addresses []string = getRaftNodesAddresses(t)
	require.Equal(t, len(addresses), 3)
	time.Sleep(7 * time.Second)
	defer KillRaftNodes(t, nodes)

	//reject leader with bad log

	masterId := WaitLeader(t, addresses)
	var newMasterId int = (masterId + 1) % 3

	fmt.Printf("Block traffic from %v, to %v", newMasterId+2, masterId+2)

	// block traffic for one node
	//       *
	//      /
	//     *
	//      \
	//       *

	var req application.DropTrafficReq
	req.NodeId = masterId + 2
	DropTraffic(t, addresses[newMasterId], req)
	req.NodeId = newMasterId + 2
	DropTraffic(t, addresses[masterId], req)

	for {
		if afterNetworkPartitionMasterId := WaitLeader(t, addresses); afterNetworkPartitionMasterId != masterId {
			break
		}
		time.Sleep(1 * time.Second)
	}

	var createReq application.CreateReq
	createReq.Key = "2"
	createReq.Value.Cnt = 10
	createReq.Value.Data = "data"

	err, statusCode, value := CreateCall(t, addresses[newMasterId], createReq)

	require.Nil(t, err)
	require.Equal(t, http.StatusOK, statusCode)
	require.Equal(t, value, raft.Value{Cnt: 10, Data: "data"})

	time.Sleep(10 * time.Second)
}

func TestRaftDropMaster(t *testing.T) {
	var nodes []*exec.Cmd = CreateRaftNodes(t)
	var addresses []string = getRaftNodesAddresses(t)
	require.Equal(t, len(addresses), 3)
	time.Sleep(7 * time.Second)
	defer KillRaftNodes(t, nodes)

	masterId := WaitLeader(t, addresses)

	// block traffic for one node
	//       *
	//      /
	//     *
	//
	//       *

	var req application.DropTrafficReq
	for j := range addresses {
		if masterId != j {
			req.NodeId = masterId + 2
			DropTraffic(t, addresses[j], req)
			req.NodeId = j + 2
			DropTraffic(t, addresses[masterId], req)
		}
	}

	var newMasterId int
	for {
		client := resty.New()
		client.SetTimeout(1 * time.Second)

		var cnt int

		for i, addr := range addresses {
			isLeader, _ := IsLeader(t, addr)
			if isLeader {
				if i != masterId {
					newMasterId = i
				}
				cnt++
			}
		}

		if cnt == 2 {
			break
		}
		time.Sleep(1 * time.Second)
	}

	var createReq application.CreateReq
	createReq.Key = "1"
	createReq.Value.Cnt = 10
	createReq.Value.Data = "data"

	err, statusCode, value := CreateCall(t, addresses[newMasterId], createReq)

	require.Nil(t, err)
	require.Equal(t, http.StatusOK, statusCode)
	require.Equal(t, value, raft.Value{Cnt: 10, Data: "data"})

	time.Sleep(5 * time.Second)

	for _, addr := range addresses {
		UnblockTraffic(t, addr)
	}

	time.Sleep(5 * time.Second)

	var getReq application.GetReq
	getReq.Key = "1"

	err, statusCode, value = GetCall(t, addresses[masterId], getReq)

	require.Nil(t, err)
	require.Equal(t, http.StatusOK, statusCode)
	require.Equal(t, value, raft.Value{Cnt: 10, Data: "data"})

	err, statusCode, value = GetCall(t, addresses[newMasterId], getReq)

	require.Nil(t, err)
	require.Equal(t, http.StatusOK, statusCode)
	require.Equal(t, value, raft.Value{Cnt: 10, Data: "data"})

	err, statusCode, value = GetCall(t, addresses[newMasterId], getReq)

	require.Nil(t, err)
	require.Equal(t, http.StatusOK, statusCode)
	require.Equal(t, value, raft.Value{Cnt: 10, Data: "data"})
}

func TestCRUD(t *testing.T) {
	var nodes []*exec.Cmd = CreateRaftNodes(t)
	var addresses []string = getRaftNodesAddresses(t)
	require.Equal(t, len(addresses), 3)
	time.Sleep(7 * time.Second)
	defer KillRaftNodes(t, nodes)

	masterId := WaitLeader(t, addresses)

	/////////////////////////////////////////////////
	{
		var req application.CreateReq
		req.Key = "1"
		req.Value.Cnt = 2
		req.Value.Data = "42"

		// do create call to master node
		var value raft.Value
		err, statusCode, value := CreateCall(t, addresses[masterId], req)

		require.Nil(t, err)
		require.Equal(t, http.StatusOK, statusCode)
		require.Equal(t, value, raft.Value{Cnt: 2, Data: "42"})

		var getReq application.GetReq
		getReq.Key = "1"

		time.Sleep(2 * time.Second)

		// do call to master with Found and Location
		err, statusCode, value = GetCall(t, addresses[masterId], getReq)

		fmt.Printf("GetCall , err %v, statusCode %v, value %v", err, statusCode, value)

		require.Nil(t, err)
		require.Equal(t, http.StatusOK, statusCode)
		require.Equal(t, value, raft.Value{Cnt: 2, Data: "42"})

		// do call for another
		err, statusCode, value = GetCall(t, addresses[masterId], getReq)

		require.Nil(t, err)
		require.Equal(t, http.StatusOK, statusCode)
		require.Equal(t, value, raft.Value{Cnt: 2, Data: "42"})
	}

	/////////////////////////////////////////////////

	{
		var req application.UpdateReq
		req.Key = "1"
		req.Value.Cnt = 0
		req.Value.Data = "ok"

		// do create call to master node
		var value raft.Value
		err, statusCode, value := UpdateCall(t, addresses[masterId], req)

		require.Nil(t, err)
		require.Equal(t, http.StatusOK, statusCode)
		require.Equal(t, value, raft.Value{Cnt: 0, Data: "ok"})

		var getReq application.GetReq
		getReq.Key = "1"

		time.Sleep(2 * time.Second)

		// do call to master with Found and Location
		err, statusCode, value = GetCall(t, addresses[masterId], getReq)

		fmt.Printf("GetCall , err %v, statusCode %v, value %v", err, statusCode, value)

		require.Nil(t, err)
		require.Equal(t, http.StatusOK, statusCode)
		require.Equal(t, value, raft.Value{Cnt: 0, Data: "ok"})

		// do call for another
		err, statusCode, value = GetCall(t, addresses[masterId], getReq)

		require.Nil(t, err)
		require.Equal(t, http.StatusOK, statusCode)
		require.Equal(t, value, raft.Value{Cnt: 0, Data: "ok"})
	}

	/////////////////////////////////////////////////

	{
		var req application.PatchReq
		req.Key = "1"
		var cnt int32 = 42
		req.Cnt = &cnt

		// do create call to master node
		var value raft.Value
		err, statusCode, value := PatchCall(t, addresses[masterId], req)

		require.Nil(t, err)
		require.Equal(t, http.StatusOK, statusCode)
		require.Equal(t, value, raft.Value{Cnt: 42, Data: "ok"})

		var getReq application.GetReq
		getReq.Key = "1"

		time.Sleep(2 * time.Second)

		// do call to master with Found and Location
		err, statusCode, value = GetCall(t, addresses[masterId], getReq)

		fmt.Printf("GetCall , err %v, statusCode %v, value %v", err, statusCode, value)

		require.Nil(t, err)
		require.Equal(t, http.StatusOK, statusCode)
		require.Equal(t, value, raft.Value{Cnt: 42, Data: "ok"})

		// do call for another
		err, statusCode, value = GetCall(t, addresses[masterId], getReq)

		require.Nil(t, err)
		require.Equal(t, http.StatusOK, statusCode)
		require.Equal(t, value, raft.Value{Cnt: 42, Data: "ok"})
	}

	/////////////////////////////////////////////////

	{
		var req application.DeleteReq
		req.Key = "1"

		// do create call to master node
		var value raft.Value
		err, statusCode, value := DeleteCall(t, addresses[masterId], req)

		require.Nil(t, err)
		require.Equal(t, http.StatusOK, statusCode)

		var getReq application.GetReq
		getReq.Key = "1"

		time.Sleep(2 * time.Second)

		// do call to master with Found and Location
		err, statusCode, value = GetCall(t, addresses[masterId], getReq)

		fmt.Printf("GetCall , err %v, statusCode %v, value %v", err, statusCode, value)

		require.Nil(t, err)
		require.Equal(t, http.StatusNotFound, statusCode)

		// do call for another
		err, statusCode, value = GetCall(t, addresses[masterId], getReq)

		require.Nil(t, err)
		require.Equal(t, http.StatusNotFound, statusCode)
	}

}
