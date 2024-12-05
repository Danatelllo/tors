package storage

import (
	"log"
	"os"

	proto "raft/raft/storage/proto"

	goobleProto "google.golang.org/protobuf/proto"
)

func ReadProtoFromFile(filePath string) (*proto.Storage, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return &proto.Storage{}, nil
	}

	message := &proto.Storage{}
	if err := goobleProto.Unmarshal(data, message); err != nil {
		log.Fatalf("Failed to unmarshal protobuf message: %v", err)
		return nil, err
	}

	return message, nil
}

func WriteProtoToFile(filePath string, message *proto.Storage) error {
	data, err := goobleProto.Marshal(message)
	if err != nil {
		log.Fatalf("Failed to marshal protobuf message: %v", err)
		return err
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		log.Fatalf("Failed to write file: %v", err)
		return err
	}
	return nil
}
