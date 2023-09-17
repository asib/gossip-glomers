package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func wrapHandler(n *maelstrom.Node, messages *[]interface{}, f func(*maelstrom.Node, map[string]any, *[]interface{}) (map[string]any, error)) func(maelstrom.Message) error {
	return func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body, err := f(n, body, messages)
		if err != nil {
			return err
		}

		return n.Reply(msg, body)
	}
}

func handleTopology(n *maelstrom.Node, body map[string]any, messages *[]interface{}) (map[string]any, error) {
	body["type"] = "topology_ok"
	delete(body, "topology")
	return body, nil
}

func handleRead(n *maelstrom.Node, body map[string]any, messages *[]interface{}) (map[string]any, error) {
	body["type"] = "read_ok"
	body["messages"] = *messages
	return body, nil
}

func handleBroadcast(n *maelstrom.Node, body map[string]any, messages *[]interface{}) (map[string]any, error) {
	for _, nodeId := range n.NodeIDs() {
		n.Send(nodeId, body)
	}

	*messages = append(*messages, body["message"])

	body["type"] = "broadcast_ok"
	delete(body, "message")
	return body, nil
}

func main() {
	n := maelstrom.NewNode()
	messages := make([]interface{}, 0)

	n.Handle("topology", wrapHandler(n, &messages, handleTopology))
	n.Handle("read", wrapHandler(n, &messages, handleRead))
	n.Handle("broadcast", wrapHandler(n, &messages, handleBroadcast))

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
