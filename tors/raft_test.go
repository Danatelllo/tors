package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/stretchr/testify/require"

	"raft/application"
	"raft/raft"
)

const (
	basePort = 8080
	numNodes = 3
)

func startRaftNode(t *testing.T, nodeID int, numNodes int) *exec.Cmd {
	cmd := exec.Command("go", "run", "main.go", "~/tors/tmp/1", strconv.Itoa(nodeID), strconv.Itoa(numNodes))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start Raft node %d: %v", nodeID, err)
	}

	return cmd
}

func startRaftNodes(t *testing.T) []*exec.Cmd {

	nodes := make([]*exec.Cmd, numNodes)
	for i := 1; i < numNodes+1; i++ {
		nodes[i-1] = startRaftNode(t, i, numNodes)
	}
	return nodes
}

func stopRaftNodes(t *testing.T, nodes []*exec.Cmd) {
	for _, node := range nodes {
		if err := node.Process.Signal(os.Kill); err != nil {
			t.Fatalf("Failed to stop Raft node: %v", err)
		}

		// Wait for the process to exit
		if err := node.Wait(); err != nil {
			if _, ok := err.(*exec.ExitError); !ok {
				t.Fatalf("Failed to wait for Raft node to stop: %v", err)
			}
		}
	}
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
		client.SetTimeout(1 * time.Second)
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
	client.SetTimeout(1 * time.Second)
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

func TestRaftNodes(t *testing.T) {
	var nodes []*exec.Cmd = startRaftNodes(t)
	var addresses []string = getRaftNodesAddresses(t)
	require.Equal(t, len(addresses), 3)
	time.Sleep(10 * time.Second)
	defer stopRaftNodes(t, nodes)

	var err error
	var masterId int
	var statusCode int

	masterId, err = lookUpLeader(t, addresses)

	require.Nil(t, err)

	var req application.CreateReq

	req.Key = "1"
	req.Value.Cnt = 2

	var value raft.Value
	err, statusCode, value = CreateCall(t, addresses[masterId], req)

	require.Nil(t, err)
	require.Equal(t, http.StatusOK, statusCode)
	require.Equal(t, value, raft.Value{Cnt: 2})

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
