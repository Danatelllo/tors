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

	"github.com/shirou/gopsutil/v3/process"
	"raft/application"
	"raft/raft"
)

const (
	basePort = 8080
	numNodes = 3
)

func createRaftNode(t *testing.T, nodeID int, numNodes int) *exec.Cmd {
	cmd := exec.Command("go", "run", "main.go", "~/tors/tmp/1", strconv.Itoa(nodeID), strconv.Itoa(numNodes))
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

	nodes := make([]*exec.Cmd, numNodes)
	for i := 1; i < numNodes+1; i++ {
		nodes[i-1] = createRaftNode(t, i, numNodes)
	}
	return nodes
}

func KillRaftNodes(t *testing.T, nodes []*exec.Cmd) error {
	for _, node := range nodes {
		pgid, err := syscall.Getpgid(node.Process.Pid)
		if err != nil {
			t.Fatalf("Failed to get process group ID: %v", err)
		}
		syscall.Kill(-pgid, syscall.SIGTERM)
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
	for i := 1; i < numNodes+1; i++ {
		addresses = append(addresses, fmt.Sprintf("http://127.0.0.%v:8080", i))
	}
	return addresses
}

func lookUpLeader(t *testing.T, addresses []string) (int, error) {
	client := resty.New()

	for i, addr := range addresses {
		resp, _ := client.R().
			SetHeader("Accept", "application/json").
			SetHeader("Content", "application/json").
			Get(fmt.Sprintf("%v/is_master", addr))
		if resp.IsSuccess() {
			return i, nil
		}
	}

	return 0, errors.New("not found leader")
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

func TestRaftNodes(t *testing.T) {
	var nodes []*exec.Cmd = CreateRaftNodes(t)
	var addresses []string = getRaftNodesAddresses(t)
	require.Equal(t, len(addresses), 3)
	time.Sleep(7 * time.Second)
	defer KillRaftNodes(t, nodes)

	var err error
	var masterId int
	var statusCode int

	masterId, err = lookUpLeader(t, addresses)

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

	time.Sleep(1 * time.Second)

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
	for {

	}

	// requestBody := Request{
	// 	Key: "1",
	// 	Cnt: 2,
	// }
	// requestBodyBytes, err := json.Marshal(requestBody)
	// if err != nil {
	// 	t.Fatalf("Failed to marshal request body: %v", err)
	// }

	// resp, err := http.Post(
	// 	fmt.Sprintf("http://127.0.0.1:%d/create", basePort),
	// 	"application/json",
	// 	bytes.NewBuffer(requestBodyBytes),
	// )
	// if err != nil {
	// 	t.Fatalf("Failed to send POST request: %v", err)
	// }
	// defer resp.Body.Close()

	// if resp.StatusCode != http.StatusOK {
	// 	t.Fatalf("Expected status code %d, got %d", http.StatusOK, resp.StatusCode)
	// }

	// responseBody, err := ioutil.ReadAll(resp.Body)
	// if err != nil {
	// 	t.Fatalf("Failed to read response body: %v", err)
	// }

	// var response Response
	// if err := json.Unmarshal(responseBody, &response); err != nil {
	// 	t.Fatalf("Failed to unmarshal response body: %v", err)
	// }

	// if !response.Success {
	// 	t.Fatalf("Expected success to be true, got false")
	// }

	// // Stop Raft nodes
	// nodes []*exec.Cmd
}
